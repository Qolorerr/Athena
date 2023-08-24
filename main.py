import argparse
import functools
import logging
from typing import Coroutine, List, Callable

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, ConversationHandler, MessageHandler, filters, \
    PollAnswerHandler

from src.config import telegram_key
from src.dialog_options import dialog_texts as dialog
from src.enums import AggregatorName, Column
from src.tickers_naming import TickerColumnNaming, TickerNaming

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

MAIN_MENU, NEW_RULE_TICKERS, ADD_TICKER, CHOOSE_COLUMNS, NEW_RULE_CONDITIONS, SHOW_GRAPH = range(6)


def default_conversation_message(name: str):
    def wrapper(func: Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine]):
        @functools.wraps(func)
        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Coroutine:
            reply_keyboard = [dialog[name]['buttons'][i:i + 2] for i in range(0, len(dialog[name]['buttons']), 2)]
            markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=False)
            await update.message.reply_text(dialog[name]['text'], reply_markup=markup)
            return await func(update, context)
        return wrapped
    return wrapper


@default_conversation_message("Greeting")
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['new_tickers']: List[TickerColumnNaming] = []
    return MAIN_MENU


@default_conversation_message("Add new rule tickers")
async def new_rule_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return NEW_RULE_TICKERS


@default_conversation_message("Add ticker")
async def add_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return ADD_TICKER


async def new_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    args = update.message.text.split()
    if len(args) == 1:
        ticker, aggregator = args[0], AggregatorName.moex.value
    elif len(args) == 2 and args[1] in [agg.value for agg in AggregatorName]:
        ticker, aggregator = args
    else:
        await update.message.reply_text(dialog["Add ticker"]['text'])
        return ADD_TICKER
    # TODO: Check args by aggregator directly
    context.user_data['new_ticker_meta'] = TickerNaming(ticker, AggregatorName[aggregator])
    await choose_columns(update, context)
    return CHOOSE_COLUMNS


@default_conversation_message("Choose columns")
async def choose_columns(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    question = dialog["Choose columns"]["poll_question"]
    options = ["Time", "Mean price", "Volume", "High", "Low"]
    columns = ["index", "mean", "vol", "high", "low"]
    if context.user_data['new_ticker_meta'].aggregator == AggregatorName.polygon:
        options += ["Transactions"]
        columns += ["number"]
    elif context.user_data['new_ticker_meta'].aggregator == AggregatorName.moex_analytic:
        options += ["Long", "Short", "Number of longs", "Number of shorts"]
        columns += ["long", "short", "long_numb", "short_numb"]
    message = await update.message.reply_poll(question, options, is_anonymous=False,
                                              allows_multiple_answers=True, open_period=60)
    payload = {
        message.poll.id: {
            "columns": columns,
            "message_id": message.message_id,
            "chat_id": update.effective_chat.id,
            "answers": 0,
        }
    }
    context.user_data.update(payload)
    return CHOOSE_COLUMNS


async def receive_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = update.poll_answer
    answered_poll = context.user_data[answer.poll_id]
    try:
        columns = answered_poll["columns"]
    except KeyError:
        return CHOOSE_COLUMNS
    selected_options = answer.option_ids
    if not selected_options:
        return CHOOSE_COLUMNS
    await update.get_bot().stop_poll(answered_poll["chat_id"], answered_poll["message_id"])
    column_names = [Column[columns[option]].value for option in selected_options]
    try:
        context.user_data['new_tickers'].append(TickerColumnNaming(context.user_data['new_ticker_meta'], column_names))
    except KeyError as e:
        logging.error(e.args)
        return MAIN_MENU
    return NEW_RULE_TICKERS


async def check_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not context.user_data['new_tickers']:
        await update.message.reply_text(dialog["No tickers"]['text'])
        return NEW_RULE_TICKERS
    return NEW_RULE_CONDITIONS


@default_conversation_message("Add new rule conditions")
async def new_rule_conditions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return NEW_RULE_TICKERS


@default_conversation_message("Show graph")
async def show_graph(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return MAIN_MENU


def button_filter(name: str, id: int) -> filters.Text:
    return filters.Text(dialog[name]['buttons'][id])


if __name__ == '__main__':
    application = ApplicationBuilder().token(telegram_key).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(button_filter('Greeting', 0), new_rule_tickers),
                        MessageHandler(button_filter('Greeting', 1), show_graph),
                        MessageHandler(filters.TEXT, start)],
            NEW_RULE_TICKERS: [MessageHandler(button_filter('Add new rule tickers', 0), add_ticker),
                               MessageHandler(button_filter('Add new rule tickers', 1), check_tickers),
                               MessageHandler(button_filter('Add new rule tickers', 2), start),
                               MessageHandler(filters.TEXT, new_rule_tickers)],
            ADD_TICKER: [MessageHandler(button_filter('Add ticker', 0), new_rule_tickers),
                         MessageHandler(filters.TEXT, new_ticker)],
            CHOOSE_COLUMNS: [MessageHandler(button_filter('Choose columns', 0), new_rule_tickers),
                             MessageHandler(filters.TEXT, choose_columns),
                             PollAnswerHandler(receive_poll_answer)],
            NEW_RULE_CONDITIONS: [],
            SHOW_GRAPH: [],
        },
        fallbacks=[],
        per_chat=False
    )
    application.add_handler(conv_handler)

    application.run_polling(allowed_updates=Update.ALL_TYPES)
