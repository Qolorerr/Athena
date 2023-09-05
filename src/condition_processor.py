from typing import List

from src.enums import ConditionInterval
from src.exceptions import NonexistentTicker, WrongCondition
from src.store_keeper import StoreKeeper
from src.tickers_naming import TickerNaming


class ConditionProcessor:
    def __init__(self):
        self.store_keeper = StoreKeeper()

    @staticmethod
    def _reformat_condition(tickers: List[TickerNaming], condition: str) -> str:
        new_condition = ''
        while condition.partition('!')[2]:
            new_part, _, condition = condition.partition('!')
            new_condition += new_part
            ticker, _, condition = condition.partition('.')
            column, _, condition = condition.partition('[')
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

            new_condition += f"self.store_keeper.get_ticker(TickerNaming({ticker_naming.symbol}, " \
                             f"AggregatorName.{ticker_naming.aggregator.value}, {ticker_naming.name}), " \
                             f"'{interval_type}').tail({interval_time}).{column}"

        return new_condition

    def _check_condition(self, condition: str) -> bool:
        allowed_names = {"sum": sum, "len": len}
        code = compile(condition, "<string>", "eval")
        for name in code.co_names:
            if name not in allowed_names:
                raise NameError(f"Use of {name} not allowed")
        try:
            return eval(code, {"__builtins__": {}}, allowed_names)
        except Exception as e:
            raise WrongCondition(e.args)

    def create_condition(self, tickers: List[TickerNaming], condition: str) -> None:
        condition = self._reformat_condition(tickers, condition)
        self._check_condition(condition)
        # TODO: Save condition to DB
