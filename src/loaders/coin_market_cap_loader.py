from typing import List, Dict, Any
from adapters.clickhouse.coin_market_cap_clickhouse_adapter import CoinMarketCapClickHouseAdapter


class CoinMarketCapLoader:
    def __init__(self):
        self.store = CoinMarketCapClickHouseAdapter()

    def ensure_ready(self) -> bool:
        if not self.store.ch.ping():
            return False
        return self.store.ensure_schema()

    def load(self, items: List[Dict[str, Any]]) -> bool:
        return self.store.upsert_prices(items)


