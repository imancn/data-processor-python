from typing import List, Dict, Any
from adapters.http.coin_market_cap_http_adapter import CoinMarketCapHttpAdapter
from configurations.config import config


async def extract_cmc_quotes(symbols: List[str] | None = None) -> List[Dict[str, Any]]:
    async with CoinMarketCapHttpAdapter() as http:
        return await http.fetch(symbols or config.symbols)


