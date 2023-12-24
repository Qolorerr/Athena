from dataclasses import dataclass

from src.enums import AggregatorName, DBInterval


@dataclass
class TickerNaming:
    name: str
    aggregator: AggregatorName
    timespan: str

    moex_market: str | None = "shares"
    moex_engine: str | None = "stock"

    def db_interval(self) -> str:
        return DBInterval[self.timespan].value
