import functools
import logging.config
from collections import defaultdict
from typing import Callable, Coroutine

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, filters

from src.condition_processor import ConditionProcessor
from src.config import telegram_key, LOGGER_CONFIG
from src.dialog_options import DialogLines
from src.enums import Command, CommandHelpMessage
from src.exceptions import WrongCondition, NonexistentAggregator, NonexistentNotification

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


async def help_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(update.message.text.split()) == 1:
        await send_default_message(update, DialogLines.help)
        return
    command = update.message.text.removeprefix(f"/{Command.help.value} ")
    for item in Command:
        if command in (item.value, '/' + item.value):
            await send_default_message(update, CommandHelpMessage[item.name].value)
            return
    await update.message.reply_text(f"I don't know command {command}")


async def list_conditions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    notifications = cond_processor.list_notifications(update.message.chat_id)
    if not notifications:
        await send_default_message(update, DialogLines.no_notifications)
        return
    text = "Your notifications:\n\n"
    text += '\n\n'.join(f"{notification.id}   {notification.origin_condition}" for notification in notifications)
    await update.message.reply_text(text)


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
        logger.error(f"Something wrong with notification", exc_info=e)
        return
    await send_default_message(update, DialogLines.created_rule)


async def remove_condition(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        cond_processor.remove_notification(int(update.message.text.removeprefix("/remove ")))
    except ValueError | NonexistentNotification as e:
        logger.debug(f"WA", exc_info=e)
        await send_default_message(update, DialogLines.wrong_notification_id)
        return
    await send_default_message(update, DialogLines.removed_rule)


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
    application.add_handler(CommandHandler(Command.help.value, help_message))
    application.add_handler(CommandHandler(Command.list.value, list_conditions))
    application.add_handler(CommandHandler(Command.add.value, add_condition))
    application.add_handler(CommandHandler(Command.remove.value, remove_condition))

    logger.debug("Starting application")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
