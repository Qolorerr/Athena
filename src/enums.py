import enum

from polygon.enums import Timespan

from src.dialog_options import DialogLines


class YfinanceInterval(enum.Enum):
    minute = "1m"
    hour = "1h"
    day = "1d"
    week = "1wk"
    month = "1mo"
    quarter = "3mo"


class PolygonInterval(enum.Enum):
    minute = (1, Timespan.MINUTE.value)
    hour = (1, Timespan.HOUR.value)
    day = (1, Timespan.DAY.value)
    week = (1, Timespan.WEEK.value)
    month = (1, Timespan.MONTH.value)
    quarter = (1, Timespan.QUARTER.value)


class MOEXInterval(enum.Enum):
    minute = 1
    hour = 60
    day = 24
    week = 7
    month = 31
    quarter = 4


class DBInterval(enum.Enum):
    minute = "T"
    hour = "H"
    day = "D"
    week = "W"
    month = "M"
    quarter = "Q"


class ConditionInterval(enum.Enum):
    C = "minute"
    T = "minute"
    H = "hour"
    D = "day"
    W = "week"
    M = "month"
    Q = "quarter"


class ToMinutes(enum.Enum):
    minute = 1
    hour = 60
    day = 60 * 24
    week = 60 * 24 * 7
    month = 60 * 24 * 30
    quarter = 60 * 24 * 30 * 3


class AggregatorName(enum.Enum):
    # polygon = "polygon"
    # yfinance = "yfinance"
    moex = "moex"
    moex_analytic = "moex_analytic"


class AggregatorShortName(enum.Enum):
    # polygon = "poly"
    # yfinance = "yfin"
    moex = "moex"
    moex_analytic = "mxnl"


class AggregatorNameFromShort(enum.Enum):
    # poly = "polygon"
    # yfin = "yfinance"
    moex = "moex"
    mxnl = "moex_analytic"


class Column(enum.Enum):
    index = "datetime"
    mean = "mean_price"
    vol = "volume"
    high = "high"
    low = "low"

    # # For polygon
    # number = "transactions"

    # For MOEX analytics
    long = "long"
    short = "short"
    long_numb = "number_long"
    short_numb = "number_short"


class Command(enum.Enum):
    help = "help"
    add = "add"
    list = "list"
    remove = "remove"


class CommandHelpMessage(enum.Enum):
    help = DialogLines.help
    add = DialogLines.add_condition
    list = DialogLines.list_notifications
    remove = DialogLines.remove_notification
