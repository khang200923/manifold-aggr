from dataclasses import dataclass, field
from typing import Dict

from src.market import Market

# TODO: Implement PostgreSQL
@dataclass
class Database:
    markets: Dict[str, Market] = field(default_factory=dict)

    def add_market(self, id: str, market: Market):
        if id in self.markets:
            raise ValueError(f"Market with id '{id}' already exists.")
        self.markets[id] = market

    def get_market(self, id: str) -> Market:
        if id not in self.markets:
            raise KeyError(f"Market with id '{id}' does not exist.")
        return self.markets[id]
