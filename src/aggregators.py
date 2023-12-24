import asyncio
import logging
# import time
from datetime import datetime, timedelta
# from enum import Enum

import aiohttp
import aiomoex
import numpy as np
import pandas as pd
# import yfinance
# from polygon import StocksClient

from src.config import moex_login_password, polygon_key
from src.enums import Column, MOEXInterval, DBInterval, ToMinutes, YfinanceInterval, PolygonInterval

logger = logging.getLogger("submodule")


class Aggregator:
    def __init__(self):
        logger.debug(f"{self.__class__.__name__} init")

    def download_data(self, symbol: str, start: datetime, end: datetime, interval: str,
                      *args, **kwargs) -> pd.DataFrame | None:
        logger.debug(f"Downloading {symbol} from {self.__class__.__name__}")
        return None


# class Polygon(Aggregator):
#     def __init__(self):
#         super().__init__()
#         self.polygon_client = StocksClient(polygon_key)
#
#     def download_data(self, symbol: str, start: datetime, end: datetime, interval: PolygonInterval,
#                       *args, **kwargs) -> pd.DataFrame | None:
#         super().download_data(symbol, start, end, interval, *args, **kwargs)
#
#         curr_date = datetime.now()
#         if (curr_date - start).total_seconds() // 60 < ToMinutes[interval.name].value:
#             return None
#         multiplier, timespan = interval.value
#         time_chunks = self.polygon_client.split_date_range(start, curr_date, timespan=timespan)
#         if len(time_chunks) == 0:
#             return None
#
#         # Setting default arguments to StocksClient.get_aggregate_bars
#         def get_aggregate_bars(from_date, to_date) -> pd.DataFrame:
#             return pd.DataFrame(self.polygon_client.get_aggregate_bars(symbol, from_date, to_date, limit=50000,
#                                                                        multiplier=multiplier, timespan=timespan,
#                                                                        full_range=True, warnings=False))
#
#         df = get_aggregate_bars(time_chunks[0][0], time_chunks[0][1])
#
#         # Polygon free account restriction
#         if df.empty:
#             time.sleep(60)
#             df = get_aggregate_bars(time_chunks[0][0], time_chunks[0][1])
#
#         for time_chunk in time_chunks[1:]:
#             new_part = get_aggregate_bars(time_chunk[0], time_chunk[1])
#
#             # Polygon free account restriction
#             if new_part.empty:
#                 time.sleep(60)
#                 new_part = get_aggregate_bars(time_chunk[0], time_chunk[1])
#
#             df = pd.concat([df, new_part]).drop_duplicates(keep='first')
#         if df is None or df.shape[0] == 0:
#             return None
#         df['t'] //= 10 ** 3
#         df[Column.mean.value] = df.loc[:, ['o', 'c']].mean(axis=1)
#         df = df.drop(['o', 'c', 'vw'], axis=1)
#         df = df.rename({'t': Column.index.value, 'v': Column.vol.value, 'l': Column.low.value,
#                         'h': Column.high.value, 'n': Column.number.value}, axis=1)
#         df = df.set_index(Column.index.value)
#         return df
#
#
# class YahooFinance(Aggregator):
#     def __init__(self):
#         super().__init__()
#
#     def download_data(self, symbol: str, start: datetime, end: datetime, interval: YfinanceInterval,
#                       *args, **kwargs) -> pd.DataFrame | None:
#         super().download_data(symbol, start, end, interval, *args, **kwargs)
#
#         ticker = yfinance.Ticker(symbol)
#
#         df = ticker.history(interval=interval.value, start=start)
#         if df.shape[0] == 0:
#             return None
#         df[Column.mean.value] = df.loc[:, ['Open', 'Close']].mean(axis=1)
#         df = df.drop(['Open', 'Close', 'Dividends', 'Stock Splits'], axis=1)
#         df = df.reset_index()
#         df = df.rename({'Datetime': Column.index.value, 'Volume': Column.vol.value, 'Low': Column.low.value,
#                         'High': Column.high.value, 'Date': Column.index.value}, axis=1)
#         df[Column.index.value] = df[Column.index.value].dt.tz_convert('UTC')
#         df[Column.index.value] = df[Column.index.value].values.astype(np.int64) // 10 ** 9
#         df = df.set_index(Column.index.value)
#         return df


class MOEX(Aggregator):
    def __init__(self):
        super().__init__()

    async def download_data(self, symbol: str, start: datetime, end: datetime, interval: str,
                            *args, **kwargs) -> pd.DataFrame | None:
        super().download_data(symbol, start, end, interval, *args, **kwargs)

        interval = MOEXInterval[interval]
        market = kwargs["market"] if "market" in kwargs else "shares"
        engine = kwargs["engine"] if "engine" in kwargs else "stock"

        async with aiohttp.ClientSession() as session:
            data = await aiomoex.get_market_candles(session, symbol, interval.value, start.strftime("%Y-%m-%d"),
                                                    end.strftime("%Y-%m-%d"), market=market, engine=engine)
            df = pd.DataFrame(data)
            if df is None or df.empty:
                return None
            df[Column.mean.value] = df.loc[:, ['open', 'close']].mean(axis=1)
            df = df.drop(['open', 'close', 'end', 'value'], axis=1)
            df = df.rename({'begin': Column.index.value}, axis=1)

            def formatter(date: str) -> int:
                return int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp())

            df[Column.index.value] = df[Column.index.value].apply(formatter).astype(np.int64)
            df = df.set_index(Column.index.value)
        return df


class MOEXAnalytical(Aggregator):
    def __init__(self):
        super().__init__()

    @staticmethod
    async def fetch_2_day_data(session: aiohttp.ClientSession, symbol: str, start_from: datetime) -> pd.DataFrame:
        url = f"https://iss.moex.com/iss/analyticalproducts/futoi/securities/{symbol}.json?" \
              f"from={start_from.strftime('%Y-%m-%d')}&till={(start_from + timedelta(1)).strftime('%Y-%m-%d')}"
        async with session.get(url) as response:
            data = await response.json()
            return pd.DataFrame(data['futoi']['data'], columns=data['futoi']['columns']).iloc[::-1]

    async def download_data(self, symbol: str, start: datetime, end: datetime, interval: str,
                            *args, **kwargs) -> pd.DataFrame | None:
        super().download_data(symbol, start, end, interval, *args, **kwargs)

        interval = MOEXInterval[interval]

        if datetime.now() - start < timedelta(minutes=5):
            return None

        async with aiohttp.ClientSession() as session:
            await session.get("https://passport.moex.com/authenticate", auth=aiohttp.BasicAuth(*moex_login_password))

            all_df = []
            while start.date() <= end.date():
                all_df.append(self.fetch_2_day_data(session, symbol, start))
                start += timedelta(2)

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
            df = df.resample(DBInterval[interval.name].value).mean().dropna(how='all')
            df = df.reset_index()
            df[Column.index.value] = df[Column.index.value].values.astype(np.int64) // 10 ** 9
            df = df.astype(float)
            df = df.set_index(Column.index.value)
            return df
