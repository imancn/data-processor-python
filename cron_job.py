import asyncio
import sys
import os
from datetime import datetime
import pytz
from crypto_price_fetcher.config import config
from crypto_price_fetcher.storage_native import storage
from crypto_price_fetcher.fetcher import fetch_crypto_prices

def log_message(message):
    tehran_tz = pytz.timezone('Asia/Tehran')
    timestamp = datetime.now(tehran_tz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp} Tehran] {message}")

async def run_cron_job():
    try:
        log_message("🚀 Starting crypto price fetch job")
        if not await storage.test_connection():
            log_message("❌ ClickHouse connection failed")
            return 1
        log_message(f"📊 Fetching data for {len(config.symbols)} symbols")
        crypto_data = await fetch_crypto_prices(config.symbols)
        if not crypto_data:
            log_message("❌ No data fetched from CMC API")
            return 1
        log_message(f"✅ Fetched {len(crypto_data)} records")
        success = await storage.save_crypto_data(crypto_data)
        if success:
            log_message("✅ Data saved to ClickHouse successfully")
            sample_count = min(5, len(crypto_data))
            log_message(f"📈 Sample data ({sample_count} records):")
            for item in crypto_data[:sample_count]:
                price = item.get('price', 0) or 0
                change_24h = item.get('percent_change_24h', 0) or 0
                log_message(f"  {item['symbol']}: ${price:,.2f} (24h: {change_24h:+.2f}%)")
            if len(crypto_data) > sample_count:
                log_message(f"  ... and {len(crypto_data) - sample_count} more records")
            return 0
        else:
            log_message("❌ Failed to save data to ClickHouse")
            return 1
    except Exception as e:
        log_message(f"❌ Error during execution: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(run_cron_job())
    sys.exit(exit_code)
