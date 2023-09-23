import functools
import logging.config
from collections import defaultdict
from typing import Callable, Coroutine

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, filters

from src.condition_processor import ConditionProcessor
from src.config import telegram_key, LOGGER_CONFIG
from src.dialog_options import DialogLines
from src.exceptions import WrongCondition, NonexistentAggregator

logging.config.dictConfig(LOGGER_CONFIG)
logger = logging.getLogger("bot")


async def send_default_message(update: Update, line: DialogLines) -> None:
    await update.message.reply_text(line.value.text)


def default_conversation_message(line: DialogLines):
    def wrapper(func: Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine]):
        @functools.wraps(func)
        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Coroutine:
            await send_default_message(update, line)
            return await func(update, context)

        return wrapped

    return wrapper


@default_conversation_message(DialogLines.start)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    return


@default_conversation_message(DialogLines.help)
async def help_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pass


async def add_condition(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await cond_processor.create_condition(update.message.chat_id, update.message.text.removeprefix("/add "))
    except NameError | SyntaxError | WrongCondition as e:
        logger.debug(f"WC", exc_info=e)
        await send_default_message(update, DialogLines.wrong_condition_syntax)
        return
    except NonexistentAggregator as e:
        logger.debug("NEA", exc_info=e)
        await update.message.reply_text(e.args[0])
        return
    except Exception as e:
        logger.debug(f"Something wrong with notification", exc_info=e)
        return
    await send_default_message(update, DialogLines.created_rule)


async def notification(context: ContextTypes.DEFAULT_TYPE) -> None:
    active_notifications = await cond_processor.get_active_notifications()
    if not active_notifications:
        return

    texts_by_chats = defaultdict(list)
    for notification in active_notifications:
        texts_by_chats[notification.chat_id].append(notification.origin_condition)

    logger.debug(f"Sending notification to the following chats: {', '.join(map(str, texts_by_chats.keys()))}")
    for chat_id, conditions in texts_by_chats.items():
        text = "Following conditions activated:\n\n"
        text += '\n\n'.join(conditions)
        await context.bot.send_message(chat_id, text)


if __name__ == '__main__':
    application = ApplicationBuilder().token(telegram_key).build()

    cond_processor = ConditionProcessor(application.job_queue, notification)
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_message))
    # TODO: Add argument filter
    application.add_handler(CommandHandler('add', add_condition))

    application.run_polling(allowed_updates=Update.ALL_TYPES)
