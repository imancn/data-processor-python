import pytz
from datetime import datetime
from typing import Callable, Dict

from extractors.coin_market_cap_extractor import extract_cmc_quotes
from loaders.coin_market_cap_loader import CoinMarketCapLoader


def _log(message: str):
    tehran_tz = pytz.timezone('Asia/Tehran')
    timestamp = datetime.now(tehran_tz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp} Tehran] {message}")


async def job_cmc_hourly_prices() -> int:
    try:
        _log("Starting CMC hourly prices job")
        loader = CoinMarketCapLoader()
        if not loader.ensure_ready():
            _log("ClickHouse connection failed")
            return 1
        items = await extract_cmc_quotes()
        if not items:
            _log("No data fetched from CMC API")
            return 1
        if not loader.load(items):
            _log("Failed to save data to ClickHouse")
            return 1
        _log("Data saved to ClickHouse successfully")
        return 0
    except Exception as e:
        _log(f"Error: {e}")
        return 1


JOBS: Dict[str, Callable[[], int]] = {
    "cmc_hourly_prices": job_cmc_hourly_prices,
}

async def backfill(job_name: str, hours: int) -> int:
    if job_name != "cmc_hourly_prices":
        _log(f"Unsupported backfill for job: {job_name}")
        return 1
    try:
        from datetime import timedelta
        loader = CoinMarketCapLoader()
        if not loader.ensure_ready():
            _log("ClickHouse connection failed")
            return 1
        tehran_tz = pytz.timezone('Asia/Tehran')
        now = datetime.now(tehran_tz)
        for i in range(hours, -1, -1):
            target = (now - timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
            _log(f"Backfilling hour {target}")
            items = await extract_cmc_quotes()
            if not items:
                _log("No data fetched; skipping hour")
                continue
            # use underlying adapter to pass target hour
            ok = loader.store.upsert_prices(items, target_hour=target)
            if not ok:
                _log(f"Failed to save for hour {target}")
        _log("Backfill completed")
        return 0
    except Exception as e:
        _log(f"Backfill error: {e}")
        return 1


