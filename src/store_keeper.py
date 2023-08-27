import asyncio
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import aiohttp
import aiomoex
import numpy as np
import pandas as pd
import yfinance
from polygon import StocksClient
from sqlalchemy import text

from src import db_session
from src.config import start_from_date, polygon_key, user_interval, moex_login_password
from src.enums import PolygonInterval, YfinanceInterval, MOEXInterval, ToMinutes, DBInterval, AggregatorShortName, \
    AggregatorName, Column, ResampleInterval
from src.tickers import Ticker
from src.tickers_meta import TickerMeta
from src.tickers_naming import TickerNaming


class StoreKeeper:
    def __init__(self):
        self.polygon_client = StocksClient(polygon_key)

        db_session.global_init(Path().resolve() / "res/db/athena_data.sqlite")

    # Universal storing name
    # Example: poly_gold, yfin_silver, etc.
    @staticmethod
    def get_storing_name(name: TickerNaming) -> str:
        return f"{AggregatorShortName[name.aggregator.name].value}_{name.name}"

    # Gather new data from yfinance
    @staticmethod
    def download_data_from_yfinance(symbol: str, start_from: datetime,
                                    interval: YfinanceInterval) -> pd.DataFrame | None:
        ticker = yfinance.Ticker(symbol)

        df = ticker.history(interval=interval.value, start=start_from)
        if df.shape[0] == 0:
            return None
        df[Column.mean.value] = df.loc[:, ['Open', 'Close']].mean(axis=1)
        df = df.drop(['Open', 'Close', 'Dividends', 'Stock Splits'], axis=1)
        df = df.reset_index()
        df = df.rename({'Datetime': Column.index.value, 'Volume': Column.vol.value, 'Low': Column.low.value,
                        'High': Column.high.value, 'Date': Column.index.value}, axis=1)
        df[Column.index.value] = df[Column.index.value].dt.tz_convert('UTC')
        df[Column.index.value] = df[Column.index.value].values.astype(np.int64) // 10 ** 9
        df = df.set_index(Column.index.value)
        return df

    # Gather new data from polygon
    def download_data_from_polygon(self, symbol: str, start_from: datetime,
                                   interval: PolygonInterval) -> pd.DataFrame | None:
        curr_date = datetime.now()
        if (curr_date - start_from).total_seconds() // 60 < ToMinutes[interval.name].value:
            return None
        multiplier, timespan = interval.value
        time_chunks = self.polygon_client.split_date_range(start_from, curr_date, timespan=timespan)
        if len(time_chunks) == 0:
            return None

        # Setting default arguments to StocksClient.get_aggregate_bars
        def get_aggregate_bars(from_date, to_date) -> pd.DataFrame:
            return pd.DataFrame(self.polygon_client.get_aggregate_bars(symbol, from_date, to_date, limit=50000,
                                                                       multiplier=multiplier, timespan=timespan,
                                                                       full_range=True, warnings=False))

        df = get_aggregate_bars(time_chunks[0][0], time_chunks[0][1])

        # Polygon free account restriction
        if df.empty:
            time.sleep(60)
            df = get_aggregate_bars(time_chunks[0][0], time_chunks[0][1])

        for time_chunk in time_chunks[1:]:
            new_part = get_aggregate_bars(time_chunk[0], time_chunk[1])

            # Polygon free account restriction
            if new_part.empty:
                time.sleep(60)
                new_part = get_aggregate_bars(time_chunk[0], time_chunk[1])

            df = pd.concat([df, new_part]).drop_duplicates(keep='first')
        if df is None or df.shape[0] == 0:
            return None
        df['t'] //= 10 ** 3
        df[Column.mean.value] = df.loc[:, ['o', 'c']].mean(axis=1)
        df = df.drop(['o', 'c', 'vw'], axis=1)
        df = df.rename({'t': Column.index.value, 'v': Column.vol.value, 'l': Column.low.value,
                        'h': Column.high.value, 'n': Column.number.value}, axis=1)
        df = df.set_index(Column.index.value)
        return df

    # Gather new data from moex
    @staticmethod
    async def download_data_from_moex(symbol: str, start_from: datetime, interval: MOEXInterval, market: str = "shares",
                                      engine: str = "stock") -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            data = await aiomoex.get_market_candles(session, symbol, interval.value, start_from.strftime("%Y-%m-%d"),
                                                    market=market, engine=engine)
            df = pd.DataFrame(data)
            df[Column.mean.value] = df.loc[:, ['open', 'close']].mean(axis=1)
            df = df.drop(['open', 'close', 'end', 'value'], axis=1)
            df = df.rename({'begin': Column.index.value}, axis=1)

            def formatter(date: str) -> int:
                return int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp())

            df[Column.index.value] = df[Column.index.value].apply(formatter).astype(np.int64)
            df = df.set_index(Column.index.value)
        return df

    @staticmethod
    async def fetch_2_day_data(session: aiohttp.ClientSession, symbol: str, start_from: datetime) -> pd.DataFrame:
        url = f"https://iss.moex.com/iss/analyticalproducts/futoi/securities/{symbol}.json?" \
              f"from={start_from.strftime('%Y-%m-%d')}&till={(start_from + timedelta(1)).strftime('%Y-%m-%d')}"
        async with session.get(url) as response:
            data = await response.json()
            return pd.DataFrame(data['futoi']['data'], columns=data['futoi']['columns']).iloc[::-1]

    # Gather analytical data from MOEX
    async def download_analytical_data_from_moex(self, symbol: str, start_from: datetime,
                                                 interval: MOEXInterval) -> pd.DataFrame | None:
        if datetime.now() - start_from < timedelta(minutes=5):
            return None

        async with aiohttp.ClientSession() as session:
            await session.get("https://passport.moex.com/authenticate", auth=aiohttp.BasicAuth(*moex_login_password))

            all_df = []
            while start_from.date() <= datetime.now().date():
                all_df.append(self.fetch_2_day_data(session, symbol, start_from))
                start_from += timedelta(2)

            df = pd.concat(await asyncio.gather(*all_df, return_exceptions=True))
            df = df.query("clgroup == 'YUR'")
            formatter = lambda x: round(datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp() / 300) * 300
            df[Column.index.value] = df.loc[:, ['tradedate', 'tradetime']].agg(' '.join, axis=1).apply(formatter)
            df[Column.index.value] = df[Column.index.value].astype(float)
            df['pos_short'] *= -1

            df = df.drop(['tradedate', 'tradetime', 'sess_id', 'seqnum', 'systime', 'ticker', 'clgroup', 'pos'], axis=1)
            df = df.rename({'pos_long': Column.long.value, 'pos_short': Column.short.value,
                            'pos_long_num': Column.long_numb.value, 'pos_short_num': Column.short_numb.value}, axis=1)
            df[Column.index.value] = pd.to_datetime(df[Column.index.value], unit='s')
            df = df.set_index(Column.index.value)
            df = df.resample(ResampleInterval[interval.name].value).mean().dropna(how='all')
            df = df.reset_index()
            df[Column.index.value] = df[Column.index.value].values.astype(np.int64) // 10 ** 9
            df = df.astype(float)
            df = df.set_index(Column.index.value)
            return df

    # Save ticker data to db
    def add_ticker_to_db(self, name: TickerNaming, timespan: DBInterval, from_date: datetime,
                         df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return None
        session = db_session.create_session()
        ticker = session.query(Ticker).filter((Ticker.symbol == name.symbol) &
                                              (Ticker.aggregator == name.aggregator.value)).first()
        if ticker is None:
            ticker = Ticker()
            ticker.name = name.name
            ticker.symbol = name.symbol
            ticker.aggregator = name.aggregator.value
            session.add(ticker)
            session.commit()

        ticker_meta: List[TickerMeta] = ticker.meta
        ticker_meta: List[TickerMeta] = list(filter(lambda meta: meta.timespan == timespan.value, ticker_meta))
        if len(ticker_meta) == 0:
            ticker_meta = TickerMeta()
            ticker_meta.ticker_id = ticker.id
            ticker_meta.timespan = timespan.value
            ticker_meta.from_date = datetime.timestamp(from_date)
            ticker_meta.to_date = datetime.timestamp(datetime.now())
            session.add(ticker_meta)
        else:
            ticker_meta = ticker_meta[0]
            ticker_meta.from_date = min(ticker_meta.from_date, datetime.timestamp(from_date))
            ticker_meta.to_date = max(ticker_meta.to_date, datetime.timestamp(datetime.now()))
        session.commit()

        df.to_sql(self.get_storing_name(name), session.bind, if_exists='append')

    # List available tickers
    @staticmethod
    def list_tickers_in_db() -> List[Ticker]:
        session = db_session.create_session()
        tickers = session.query(Ticker).all()
        return tickers

    # List available time periods for all tickers
    @staticmethod
    def list_ticker_meta_in_db() -> List[TickerMeta]:
        session = db_session.create_session()
        ticker_meta = session.query(TickerMeta).all()
        return ticker_meta

    # Download ticker data from db
    def get_ticker_from_db(self, name: TickerNaming, timespan: DBInterval, from_date: datetime) -> pd.DataFrame | None:
        session = db_session.create_session()
        ticker = session.query(Ticker).filter((Ticker.symbol == name.symbol) &
                                              (Ticker.aggregator == name.aggregator.value)).first()
        if ticker is None:
            return None

        ticker_meta: List[TickerMeta] = ticker.meta
        ticker_meta: List[TickerMeta] = list(filter(lambda meta: meta.timespan == timespan.value, ticker_meta))
        if len(ticker_meta) == 0 and name.aggregator != AggregatorName.moex_analytic:
            return None
        storing_name = self.get_storing_name(name)
        request = text(f"SELECT * FROM {storing_name} WHERE {Column.index.value} >= {datetime.timestamp(from_date)}")
        df = pd.read_sql(request, db_session.create_connection())
        df = df.set_index(Column.index.value).sort_index()
        return df

    def get_ticker(self, name: TickerNaming, custom_interval: str = None) -> pd.DataFrame:
        interval = custom_interval if custom_interval else user_interval

        df = self.get_ticker_from_db(name, DBInterval[interval], start_from_date)

        loop = asyncio.get_event_loop()
        start_from = start_from_date
        if df is not None:
            start_from = datetime.utcfromtimestamp(df.tail(1).index.values[0])

        if name.aggregator == AggregatorName.polygon:
            new_df = self.download_data_from_polygon(name.symbol, start_from, PolygonInterval[interval])

        elif name.aggregator == AggregatorName.yfinance:
            new_df = self.download_data_from_yfinance(name.symbol, start_from, YfinanceInterval[interval])

        elif name.aggregator == AggregatorName.moex:
            new_df = loop.run_until_complete(self.download_data_from_moex(name.symbol, start_from,
                                                                          MOEXInterval[interval], name.moex_market,
                                                                          name.moex_engine))

        elif name.aggregator == AggregatorName.moex_analytic:
            new_df = loop.run_until_complete(self.download_analytical_data_from_moex(name.symbol, start_from,
                                                                                     MOEXInterval[interval]))

        else:
            raise ValueError("Unknown aggregator")

        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated(keep='last')]

        self.add_ticker_to_db(name, DBInterval[interval], start_from, new_df)

        return df
