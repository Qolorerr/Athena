import functools
import logging.config
from collections import defaultdict
from typing import Coroutine, List, Callable

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, ConversationHandler, MessageHandler, filters

from src.condition_processor import ConditionProcessor
from src.config import telegram_key, LOGGER_CONFIG
from src.dialog_options import DialogLines
from src.enums import AggregatorName
from src.tickers_naming import TickerNaming

logging.config.dictConfig(LOGGER_CONFIG)
logger = logging.getLogger("bot")

MAIN_MENU, ADD_TICKER, ADD_CONDITION, CREATED, SHOW_GRAPH = range(5)


async def send_default_message(update: Update, line: DialogLines) -> None:
    reply_keyboard = [line.value.buttons[i:i + 2] for i in range(0, len(line.value.buttons), 2)]
    markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=False)
    await update.message.reply_text(line.value.text, reply_markup=markup)


def default_conversation_message(line: DialogLines):
    def wrapper(func: Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine]):
        @functools.wraps(func)
        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Coroutine:
            await send_default_message(update, line)
            return await func(update, context)

        return wrapped

    return wrapper


@default_conversation_message(DialogLines.start)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['new_tickers']: List[TickerNaming] = []
    return MAIN_MENU


@default_conversation_message(DialogLines.new_rule)
async def new_rule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return MAIN_MENU


@default_conversation_message(DialogLines.add_ticker)
async def add_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return ADD_TICKER


async def new_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    args = update.message.text.split()
    if len(args) == 1:
        ticker, aggregator = args[0], AggregatorName.moex.value
    elif len(args) == 2 and args[1] in [agg.value for agg in AggregatorName]:
        ticker, aggregator = args
    else:
        await send_default_message(update, DialogLines.add_ticker)
        return ADD_TICKER
    # TODO: Check args by aggregator directly
    context.user_data['new_tickers'].append(TickerNaming(ticker, AggregatorName[aggregator]))
    await send_default_message(update, DialogLines.ticker_added)
    return MAIN_MENU


async def print_available_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = "Available tickers:\n\n"

    for naming in context.user_data['new_tickers']:
        columns = ["mean", "vol", "high", "low"]
        if naming.aggregator == AggregatorName.polygon:
            columns += ["number"]
        elif naming.aggregator == AggregatorName.moex_analytic:
            columns += ["long", "short", "long_numb", "short_numb"]
        text += f"{naming.name} - {', '.join(columns)}\n\n"

    await update.message.reply_text(text)


async def add_condition(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.text == DialogLines.new_rule.value.buttons[1]:
        if context.user_data['new_tickers']:
            await send_default_message(update, DialogLines.add_condition)
            await print_available_tickers(update, context)
            return ADD_CONDITION
        else:
            await update.message.reply_text(DialogLines.no_tickers.value.text)
            return MAIN_MENU
    try:
        await cond_processor.create_condition(update.message.chat_id, context.user_data['new_tickers'],
                                              update.message.text)
    except NameError | SyntaxError as e:
        logger.debug(f"Wrong syntax", exc_info=e)
        await update.message.reply_text("Wrong syntax")
        return ADD_CONDITION
    except Exception as e:
        logger.debug(f"Something wrong with notification", exc_info=e)
        return ADD_CONDITION
    await send_default_message(update, DialogLines.created_rule)
    return ConversationHandler.END


@default_conversation_message(DialogLines.show_graph)
async def show_graph(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return MAIN_MENU


@default_conversation_message(DialogLines.end)
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return ConversationHandler.END


async def notification(context: ContextTypes.DEFAULT_TYPE) -> None:
    active_notifications = await cond_processor.get_active_notifications()
    if not active_notifications:
        return

    texts_by_chats = defaultdict(list)
    for notification in active_notifications:
        texts_by_chats[notification.chat_id].append(notification.condition)

    logger.debug(f"Sending notification to the following chats: {', '.join(texts_by_chats.keys())}")
    for chat_id, conditions in texts_by_chats.items():
        text = "Following conditions activated:\n\n"
        text += '\n\n'.join(conditions)
        await context.bot.send_message(chat_id, text)


def button_filter(line: DialogLines, id: int) -> filters.Text:
    return filters.Text(line.value.buttons[id])


if __name__ == '__main__':
    application = ApplicationBuilder().token(telegram_key).build()

    cond_processor = ConditionProcessor(application.job_queue, notification)

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(button_filter(DialogLines.start, 0), new_rule),
                        MessageHandler(button_filter(DialogLines.start, 1), show_graph),
                        MessageHandler(button_filter(DialogLines.new_rule, 0), add_ticker),
                        MessageHandler(button_filter(DialogLines.new_rule, 1), add_condition),
                        MessageHandler(button_filter(DialogLines.new_rule, 2), start)],
            ADD_TICKER: [MessageHandler(button_filter(DialogLines.add_ticker, 0), new_rule),
                         MessageHandler(filters.TEXT, new_ticker)],
            ADD_CONDITION: [MessageHandler(button_filter(DialogLines.add_condition, 0), new_rule),
                            MessageHandler(filters.TEXT, add_condition)],
            SHOW_GRAPH: [],
        },
        fallbacks=[CommandHandler('stop', stop)],
        per_chat=False
    )
    application.add_handler(conv_handler)

    application.run_polling(allowed_updates=Update.ALL_TYPES)
