import asyncio
import aiohttp
from typing import List, Dict, Any
from datetime import datetime
import pytz
from .config import config

class CoinMarketCapFetcher:
    def __init__(self):
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.session = None

    def _clean_last_updated(self, last_updated: str) -> datetime:
        if not last_updated:
            return None
        cleaned = last_updated.replace('[UTC]', '').replace('Z', '').strip()
        try:
            dt = datetime.fromisoformat(cleaned.replace('T', ' '))
            if dt.tzinfo is None:
                tehran_tz = pytz.timezone('Asia/Tehran')
                dt = tehran_tz.localize(dt)
            return dt
        except:
            return None

    async def __aenter__(self):
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_crypto_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        if not config.cmc_api_key:
            raise ValueError("CMC_API_KEY not configured")
        symbols_str = ','.join(symbols)
        url = f"{self.base_url}/cryptocurrency/quotes/latest"
        params = {
            'symbol': symbols_str,
            'convert': 'USD'
        }
        headers = config.get_cmc_headers()
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._process_cmc_data(data)
                else:
                    error_text = await response.text()
                    raise Exception(f"CMC API error {response.status}: {error_text}")
        except Exception as e:
            print(f"Error fetching CMC data: {e}")
            return []

    def _process_cmc_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_data = []
        if 'data' not in data:
            return processed_data
        for symbol, crypto_data in data['data'].items():
            try:
                quote = crypto_data.get('quote', {}).get('USD', {})
                processed_item = {
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
                }
                processed_data.append(processed_item)
            except Exception as e:
                print(f"Error processing data for {symbol}: {e}")
                continue
        return processed_data

async def fetch_crypto_prices(symbols: List[str]) -> List[Dict[str, Any]]:
    async with CoinMarketCapFetcher() as fetcher:
        return await fetcher.fetch_crypto_data(symbols)