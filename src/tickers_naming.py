from dataclasses import dataclass
from typing import Optional, List

from src.enums import AggregatorName, Column


@dataclass
class TickerNaming:
    symbol: str
    aggregator: AggregatorName
    name: Optional[str] = None

    moex_market: Optional[str] = "shares"
    moex_engine: Optional[str] = "stock"

    def __post_init__(self):
        if self.name is None:
            self.name = self.symbol


@dataclass
class TickerColumnNaming:
    naming: TickerNaming
    columns: List[str]
