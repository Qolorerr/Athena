import logging
from typing import List, Callable, Coroutine, Any

import pandas as pd
from telegram.ext import JobQueue, ContextTypes

from src.config import notification_interval
from src.enums import ConditionInterval, AggregatorName, Column
from src.exceptions import NonexistentTicker, WrongCondition
from src.notifications import Notification
from src.store_keeper import StoreKeeper
from src.tickers_naming import TickerNaming


logger = logging.getLogger("submodule")
NOTIFICATOR = "notificator"


class ConditionProcessor:
    def __init__(self, job_queue: JobQueue,
                 notification: Callable[[ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, None]]):
        self.job_queue = job_queue
        self.store_keeper = StoreKeeper()
        self.notifications: List[Notification] = []
        self.load_notifications()
        self.set_notificator(notification)
        logger.info("Condition processor initiated")

    def load_notifications(self, chat_id: int = None) -> None:
        self.notifications = self.store_keeper.get_notifications(chat_id)

    def remove_notificator(self) -> None:
        jobs = self.job_queue.get_jobs_by_name(NOTIFICATOR)
        for job in jobs:
            job.schedule_removal()

    def set_notificator(self, notification: Callable[[ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, None]]) -> None:
        self.remove_notificator()
        self.job_queue.run_repeating(notification, notification_interval, name=NOTIFICATOR)

    @staticmethod
    def _reformat_condition(tickers: List[TickerNaming], condition: str) -> str:
        new_condition = 'async def __ex():\n return '
        while condition.partition('!')[2]:
            new_part, _, condition = condition.partition('!')
            new_condition += new_part
            ticker, _, condition = condition.partition('.')
            column, _, condition = condition.partition('[')
            column = f"['{Column[column].value}']"
            interval, _, condition = condition.partition(']')
            if condition and condition[0] == '.':
                new_part, _, condition = condition.partition('()')
                column += new_part + '()'
            else:
                column += ".tail(1).item()"

            ticker_naming = list(filter(lambda x: x.name == ticker, tickers))
            if not ticker_naming:
                raise NonexistentTicker(f"Can't find {ticker} in chosen tickers")
            ticker_naming = ticker_naming[0]

            interval_type = ConditionInterval[interval[-1]].value
            interval = 'T' if interval == 'C' else interval
            interval_time = int(interval[:-1]) if interval[:-1] else 1

            new_condition += f"(await gt(TN('{ticker_naming.symbol}', " \
                             f"AN{ticker_naming.aggregator.value}, '{ticker_naming.name}'), " \
                             f"'{interval_type}')).tail({interval_time}){column}"

        return new_condition + condition

    async def _check_condition(self, condition: str) -> bool:
        allowed_names = {"gt": self.store_keeper.async_get_ticker, "TN": TickerNaming, "tail": pd.DataFrame.tail,
                         "mean": pd.Series.mean, "max": pd.Series.max, "min": pd.Series.min,
                         "sum": pd.Series.sum, "item": pd.Series.item}
        for aggregator in AggregatorName:
            allowed_names[f"AN{aggregator.value}"] = aggregator
        # code = compile(condition, "<string>", "eval")
        # for name in code.co_names:
        #     if name not in allowed_names:
        #         logger.debug(f"Wrong condition:\n{condition}")
        #         raise NameError(f"Use of {name} not allowed")
        allowed_names["__builtins__"] = {}
        exec(condition, allowed_names)
        try:
            return await locals()['allowed_names']['__ex']()
        except Exception as e:
            raise WrongCondition(e)

    def save_notification(self, chat_id: int, condition: str) -> None:
        notification = self.store_keeper.add_notification(chat_id, condition)
        try:
            logger.debug(notification.id)
        except Exception as e:
            logger.debug("Notification id??", exc_info=e)
        self.notifications.append(notification)
        logger.debug(f"Notification {notification.id} saved")

    async def create_condition(self, chat_id: int, tickers: List[TickerNaming], condition: str) -> None:
        logger.debug("Processing new condition")
        condition = self._reformat_condition(tickers, condition)
        await self._check_condition(condition)
        logger.debug("Checked!")
        self.save_notification(chat_id, condition)

    async def get_active_notifications(self) -> List[Notification]:
        active_notifications = []
        for notification in self.notifications:
            if await self._check_condition(notification.condition):
                active_notifications.append(notification)
        return active_notifications
