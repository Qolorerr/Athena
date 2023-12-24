import logging
from typing import Callable, Coroutine, Any

import pandas as pd
from telegram.ext import JobQueue, ContextTypes

from src.config import notification_interval
from src.enums import ConditionInterval, AggregatorName, Column, AggregatorNameFromShort
from src.exceptions import WrongCondition, NonexistentAggregator, NonexistentNotification
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
        self.notifications: dict[int, Notification] = dict()
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
    def _reformat_condition(condition: str) -> str:
        new_condition = 'async def __ex():\n return '
        while condition.partition('#')[2]:
            new_part, _, condition = condition.partition('#')
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

            rewind = 0
            if ':' in interval:
                interval, _, rewind = interval.partition(':')
                try:
                    rewind = int(rewind)
                except ValueError:
                    raise WrongCondition(f"Wrong rewind value: {rewind}")
                if rewind >= 0:
                    raise WrongCondition(f"Wrong rewind value: {rewind}")
            timespan = ConditionInterval[interval[-1]].value
            interval_time = int(interval[:-1]) if interval[:-1] else 1
            end = rewind
            start = end - interval_time

            aggregator_name = AggregatorName.moex
            if ':' in ticker:
                aggregator, _, ticker = ticker.partition(':')
                if aggregator.lower() not in [agg.name for agg in AggregatorNameFromShort]:
                    raise NonexistentAggregator(f"There's no such aggregator as {aggregator.lower()}")
                aggregator_name = AggregatorName[AggregatorNameFromShort[aggregator.lower()].value]

            new_condition += f"(await gt(TN('{ticker}', AN{aggregator_name.value}, '{timespan}'), " \
                             f"{start}, {end})).tail({interval_time}){column}"

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

    def save_notification(self, chat_id: int, condition: str, origin_condition: str) -> None:
        notification = self.store_keeper.add_notification(chat_id, condition, origin_condition)
        self.notifications[notification.id] = notification
        logger.debug(f"Notification {notification.id} saved")

    async def create_condition(self, chat_id: int, condition: str) -> None:
        logger.debug("Processing new condition")
        condition, origin_condition = self._reformat_condition(condition), condition
        print(condition, origin_condition)
        await self._check_condition(condition)
        logger.debug("Checked!")
        self.save_notification(chat_id, condition, origin_condition)

    def list_notifications(self, chat_id: int) -> list[Notification]:
        notifications = []
        for notification in self.notifications.values():
            if notification.chat_id == chat_id:
                notifications.append(notification)
        return notifications

    def remove_notification(self, id: int) -> None:
        if id not in self.notifications:
            raise NonexistentNotification
        self.store_keeper.remove_notification(id)
        self.notifications.pop(id)

    async def get_active_notifications(self) -> list[Notification]:
        active_notifications = []
        for notification in self.notifications.values():
            if await self._check_condition(notification.condition):
                active_notifications.append(notification)
        return active_notifications
