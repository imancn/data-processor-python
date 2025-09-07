import asyncio
import sys
from datetime import datetime
from .config import config
from .fetcher import fetch_crypto_prices
from .storage_native import storage

async def main():
    print(f"[{datetime.now()}] Starting crypto data fetch...")
    print("Testing ClickHouse connection...")
    if not await storage.test_connection():
        print("❌ ClickHouse connection failed!")
        return 1
    print("✅ ClickHouse connection successful")
    print("Setting up database and table...")
    await storage.create_database()
    await storage.create_table()
    print("✅ Database setup complete")
    print(f"Fetching data for {len(config.symbols)} symbols")
    print(f"Symbols: {', '.join(config.symbols)}")
    try:
        crypto_data = await fetch_crypto_prices(config.symbols)
        if crypto_data:
            print(f"✅ Fetched data for {len(crypto_data)} cryptocurrencies")
            success = await storage.save_crypto_data(crypto_data)
            if success:
                print("✅ Data saved to ClickHouse successfully")
                print("\nSample data:")
                for item in crypto_data[:5]:
                    price = item.get('price', 0) or 0
                    change_24h = item.get('percent_change_24h', 0) or 0
                    print(f"  {item['symbol']}: ${price:,.2f} (24h: {change_24h:+.2f}%)")
                if len(crypto_data) > 5:
                    print(f"  ... and {len(crypto_data) - 5} more")
            else:
                print("❌ Failed to save data to ClickHouse")
                return 1
        else:
            print("❌ No data fetched")
            return 1
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        return 1
    print(f"[{datetime.now()}] Crypto data fetch completed successfully!")
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)