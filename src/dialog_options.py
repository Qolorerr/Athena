import enum
from typing import List


class DialogLine:
    def __init__(self, text: str = "", buttons: List[str] = []):
        self.text = text
        self.buttons = buttons


class DialogLines(enum.Enum):
    start = DialogLine(
        "Hi! This bot can help to analyse stocks market. You can set some conditions and if the condition is met, "
        "you will be notified",
        ["Add new rule", "Show graph"])
    new_rule = DialogLine("Firstly, you need to choose which tickers you'll use for your condition",
                          ["Add ticker", "Next", "Cancel"])
    add_ticker = DialogLine(
        "Write the ticker name and the source from which I will gathering information: moex (default), polygon, "
        "yfinance (yahoo finance), moex_analytics (for some futures)\nFor example: <AAPL yfinance> or just <YNDX>",
        ["Cancel"])
    ticker_added = DialogLine("Successfully added new ticker", ["Add ticker", "Next", "Cancel"])
    no_tickers = DialogLine("You need at least 1 ticker for your condition")
    add_condition = DialogLine(
        "Example of syntax: !YNDX.low[2H].mean()*2<!YNDX.vol[C]\n\nWhere:\n - Possible functions: sum(), min(), max(), "
        "mean()\n\n - YNDX or any ticker you added\n\n - This ticker has only columns you chose but they are written "
        "in the following abbreviations: mean (Mean price), vol (Volume), high (High), low (Low), "
        "number (Transactions), long (Long), short (Short), long_numb (Number of longs), short_numb (Number of "
        "shorts)\n\n - Possible time spans: T (minute), H (hour), D (day), W (week), M (month), Q (quarter), "
        "C (current, exactly the same as 1T) and number before them",
        ["Cancel"])
    created_rule = DialogLine("Rule saved!")
    show_graph = DialogLine()
    end = DialogLine("Bye! Don't worry, you'll get your notification anyway")
