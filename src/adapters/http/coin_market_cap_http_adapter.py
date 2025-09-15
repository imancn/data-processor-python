from typing import List, Dict, Any
from datetime import datetime
import pytz
from configurations.config import config
from adapters._bases.http_adapter import HttpAdapter


class CoinMarketCapHttpAdapter:
    def __init__(self):
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.http = None

    async def __aenter__(self):
        self.http = await HttpAdapter(self.base_url, default_headers=config.get_cmc_headers()).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.http:
            await self.http.__aexit__(exc_type, exc, tb)

    def _clean_last_updated(self, last_updated: str):
        if not last_updated:
            return None
        try:
            cleaned = last_updated.replace('[UTC]', '').replace('Z', '').strip()
            dt = datetime.fromisoformat(cleaned.replace('T', ' '))
            if dt.tzinfo is None:
                tehran_tz = pytz.timezone('Asia/Tehran')
                dt = tehran_tz.localize(dt)
            return dt
        except Exception as e:
            print(f"Error parsing last_updated '{last_updated}': {e}")
            return None

    async def fetch(self, symbols: List[str]) -> List[Dict[str, Any]]:
        if not config.cmc_api_key:
            raise ValueError("CMC_API_KEY not configured")
        params = {"symbol": ",".join(symbols), "convert": "USD"}
        data = await self.http.get_json("/cryptocurrency/quotes/latest", params=params)
        processed = []
        if 'data' not in data:
            return processed
        for symbol, crypto_data in data['data'].items():
            try:
                quote = crypto_data.get('quote', {}).get('USD', {})
                processed.append({
                    'symbol': symbol,
                    'name': crypto_data.get('name', ''),
                    'slug': crypto_data.get('slug', ''),
                    'price': quote.get('price', 0.0),
                    'market_cap': quote.get('market_cap', 0.0),
                    'volume_24h': quote.get('volume_24h', 0.0),
                    'percent_change_1h': quote.get('percent_change_1h', 0.0),
                    'percent_change_24h': quote.get('percent_change_24h', 0.0),
                    'percent_change_7d': quote.get('percent_change_7d', 0.0),
                    'last_updated': self._clean_last_updated(quote.get('last_updated', '')),
                    'cmc_rank': crypto_data.get('cmc_rank', 0),
                    'circulating_supply': crypto_data.get('circulating_supply', 0.0),
                    'total_supply': crypto_data.get('total_supply', 0.0),
                    'max_supply': crypto_data.get('max_supply', 0.0)
                })
            except Exception as e:
                print(f"Error processing data for {symbol}: {e}")
                continue
        return processed


