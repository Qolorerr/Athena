import enum
from dataclasses import dataclass


@dataclass
class DialogLine:
    text: str = ""


class DialogLines(enum.Enum):
    start = DialogLine(
        "Hi! This bot can help to analyse stocks market. You can set some conditions and if the condition is met, "
        "you will be notified")
    help = DialogLine(
        "Possible commands:\n\n/help {command} - get info about some command\n\n/list - list all your "
        "notifications\n\n/add {condition} - create new notification\n\n/remove {notification_id} - remove "
        "notification")
    add_condition = DialogLine(
        "Example of syntax: #YNDX.low[2H].mean()*2<#POLY:AAPL.vol[C]\n\nWhere:\n - Possible functions: sum(), min(), "
        "max(), mean()\n\n - Tickers: #POLY:AAPL, where possible aggregators: MOEX, "
        "MXNL (moex analytics) or you can use just #YNDX which the same as #MOEX:YNDX\n\n - This ticker has "
        "only columns you chose but they are written"
        "in the following abbreviations: mean (Mean price), vol (Volume), high (High), low (Low), "
        "long (Long), short (Short), long_numb (Number of longs), short_numb (Number of "
        "shorts)\n\n - Possible time spans: T (minute), H (hour), D (day), W (week), M (month), Q (quarter), "
        "C (current, exactly the same as 1T) and number before them")
    list_notifications = DialogLine("Use it to show active notifications and their ID's")
    remove_notification = DialogLine("Use it to remove active notification. Notification ID you can get from /list")
    wrong_condition_syntax = DialogLine("Wrong syntax")
    wrong_notification_id = DialogLine("Wrong notification id. Check </help remove> for help")
    no_notifications = DialogLine("You have no any notifications")
    created_rule = DialogLine("Rule saved!")
    removed_rule = DialogLine("Notification removed!")
