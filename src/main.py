import asyncio
import sys
from datetime import datetime
from extractors.coin_market_cap_extractor import extract_cmc_quotes
from loaders.coin_market_cap_loader import CoinMarketCapLoader


async def main():
    print(f"[{datetime.now()}] Starting cryptocurrency data fetch...")
    try:
        loader = CoinMarketCapLoader()
        if not loader.ensure_ready():
            print("❌ ClickHouse connection failed!")
            return 1
        print("✅ ClickHouse ready")
        
        items = await extract_cmc_quotes()
        if not items:
            print("❌ No data fetched")
            return 1
        print(f"✅ Fetched {len(items)} cryptocurrency records")
        
        saved = loader.load(items)
        if not saved:
            print("❌ Failed to save data to ClickHouse")
            return 1
        print("✅ Data saved to ClickHouse successfully")
        
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print(f"[{datetime.now()}] Cryptocurrency data fetch completed successfully!")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)


