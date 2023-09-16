import logging.config

import pytest

from src.config import LOGGER_CONFIG
from src.store_keeper import StoreKeeper

logging.config.dictConfig(LOGGER_CONFIG)


CHAT_ID = 0
CONDITION = "async def __ex():\n return False"
ORIGIN_CONDITION = "False"


@pytest.mark.parametrize(
    "chat_id, condition, origin_condition",
    [
        (CHAT_ID, CONDITION, ORIGIN_CONDITION)
    ]
)
def test_add_notification(chat_id: int, condition: str, origin_condition: str) -> None:
    store_keeper = StoreKeeper()
    notification = store_keeper.add_notification(chat_id, condition, origin_condition)
    print(notification.id)
    assert notification.chat_id == chat_id
    assert notification.condition == condition


@pytest.mark.parametrize(
    "chat_id, condition, origin_condition",
    [
        (CHAT_ID, CONDITION, ORIGIN_CONDITION)
    ]
)
def test_get_notifications(chat_id: int, condition: str, origin_condition: str) -> None:
    store_keeper = StoreKeeper()
    notifications = store_keeper.get_notifications(chat_id)
    assert notifications
    notification = notifications[0]
    assert notification.chat_id == chat_id
    assert notification.condition == condition
    assert notification.origin_condition == origin_condition
