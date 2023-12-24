import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Awaitable

import pandas as pd
from sqlalchemy import text, select

from src import db_session
from src.aggregators import MOEX, MOEXAnalytical, Aggregator
from src.enums import AggregatorShortName, AggregatorName, Column, ToMinutes
from src.exceptions import NonexistentNotification
from src.notifications import Notification
from src.tickers import Ticker
from src.tickers_naming import TickerNaming

logger = logging.getLogger("submodule")


class StoreKeeper:
    def __init__(self):
        self.aggregators: dict[str, Aggregator] = {
            # AggregatorName.polygon.value: Polygon(),
            # AggregatorName.yfinance.value: YahooFinance(),
            AggregatorName.moex.value: MOEX(),
            AggregatorName.moex_analytic.value: MOEXAnalytical(),
        }

        db_session.global_init(Path().resolve() / "res/db/athena_data.sqlite")

    # Universal storing name
    # Example: poly_gold, yfin_silver, etc.
    @staticmethod
    def get_storing_name(naming: TickerNaming) -> str:
        return f"{AggregatorShortName[naming.aggregator.name].value}_{naming.name}_{naming.db_interval()}"

    # Save ticker data to db
    def add_ticker_to_db(self, naming: TickerNaming, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        session = db_session.create_session()
        ticker = session.execute(select(Ticker).where((Ticker.name == naming.name) &
                                                      (Ticker.aggregator == naming.aggregator.value) &
                                                      (Ticker.timespan == naming.db_interval()))).scalar()
        if ticker is None:
            ticker = Ticker()
            ticker.name = naming.name
            ticker.aggregator = naming.aggregator.value
            ticker.timespan = naming.db_interval()
            session.add(ticker)
            session.commit()
        session.commit()

        df.to_sql(self.get_storing_name(naming), session.bind, if_exists='append')
        session.close()

    # Download ticker data from db
    def get_ticker_from_db(self, naming: TickerNaming, start: float,
                           end: float) -> pd.DataFrame | None:
        session = db_session.create_session()
        ticker = session.execute(select(Ticker).where((Ticker.name == naming.name) &
                                                      (Ticker.aggregator == naming.aggregator.value) &
                                                      (Ticker.timespan == naming.db_interval()))).scalar()
        if ticker is None:
            return None
        storing_name = self.get_storing_name(naming)
        request = text(f"SELECT * FROM {storing_name} WHERE {Column.index.value} >= {start} AND "
                       f"{Column.index.value} <= {end}")
        df = pd.read_sql(request, db_session.create_connection())
        df = df.set_index(Column.index.value).sort_index()
        df = df[~df.index.duplicated(keep='last')]
        return df

    async def async_get_ticker(self, naming: TickerNaming, start: int, end: int) -> Awaitable[pd.DataFrame]:
        if start >= end:
            raise ValueError("Start time is greater than end time")
        if naming.aggregator.value not in self.aggregators:
            raise ValueError("Unknown aggregator")

        now = datetime.now()
        start_time = now + timedelta(minutes=start * ToMinutes[naming.timespan].value)
        end_time = now + timedelta(minutes=end * ToMinutes[naming.timespan].value)
        start_timestamp = datetime.timestamp(start_time)
        end_timestamp = datetime.timestamp(end_time)

        df = self.get_ticker_from_db(naming, start_timestamp, end_timestamp)

        if df is not None and not df.empty and len(df) >= -(start - end):
            return df

        df = await self.aggregators[naming.aggregator.value].download_data(naming.name, start_time, end_time,
                                                                           naming.timespan,
                                                                           market=naming.moex_market,
                                                                           engine=naming.moex_engine)
        logger.debug(df)
        logger.debug((start_timestamp, end_timestamp))
        df = df.loc[(start_timestamp <= df.index) & (df.index <= end_timestamp)]
        logger.debug(df)
        self.add_ticker_to_db(naming, df)
        return df

    @staticmethod
    def add_notification(chat_id: int, condition: str, origin_condition: str) -> Notification:
        session = db_session.create_session()
        notification = session.execute(select(Notification).where((Notification.chat_id == chat_id) &
                                                                  (Notification.condition == condition))).scalar()
        if not notification:
            notification = Notification()
            notification.chat_id = chat_id
            notification.condition = condition
            notification.origin_condition = origin_condition

            session.add(notification)
            session.commit()
            session.close()
            # session.expunge(notification)
        return notification

    @staticmethod
    def get_notifications(chat_id: int = None) -> dict[int, Notification]:
        session = db_session.create_session()
        selection = select(Notification)
        if chat_id is not None:
            selection = selection.where(Notification.chat_id == chat_id)
        notifications = session.execute(selection).scalars().all()
        session.close()
        notifications = {notification.id: notification for notification in notifications}
        return notifications

    @staticmethod
    def remove_notification(id: int) -> None:
        session = db_session.create_session()
        notification = session.execute(select(Notification).where(Notification.id == id)).scalar()
        if notification:
            session.delete(notification)
            session.commit()
        else:
            raise NonexistentNotification()
