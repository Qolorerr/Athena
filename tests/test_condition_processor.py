import logging.config
from typing import List

import pytest
from telegram.ext import ApplicationBuilder, ContextTypes

from src.condition_processor import ConditionProcessor
from src.config import telegram_key, LOGGER_CONFIG
from src.enums import AggregatorName
from src.tickers_naming import TickerNaming

logging.config.dictConfig(LOGGER_CONFIG)


TICKERS = [TickerNaming("YNDX", AggregatorName.moex)]
CONDITION = "!YNDX.mean[C]>2000"
REFORMATED_CONDITION = "async def __ex():\n return " \
                       "(await gt(TN('YNDX', ANmoex, 'YNDX'), 'minute')).tail(1)['mean_price'].tail(1).item()>2000"


async def notification(context: ContextTypes.DEFAULT_TYPE) -> None:
    return


@pytest.mark.parametrize(
    "tickers, condition",
    [
        (TICKERS, CONDITION)
    ]
)
def test_reformat_condition(tickers: List[TickerNaming], condition: str) -> None:
    application = ApplicationBuilder().token(telegram_key).build()
    cond_processor = ConditionProcessor(application.job_queue, notification)
    assert cond_processor._reformat_condition(tickers, condition) == REFORMATED_CONDITION


@pytest.mark.parametrize(
    "condition",
    [
        REFORMATED_CONDITION
    ]
)
async def test_check_condition(condition: str) -> None:
    application = ApplicationBuilder().token(telegram_key).build()
    cond_processor = ConditionProcessor(application.job_queue, notification)
    assert await cond_processor._check_condition(condition)
