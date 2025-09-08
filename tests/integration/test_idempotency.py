from datetime import datetime
import os
import sys

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from adapters.clickhouse.coin_market_cap_clickhouse_adapter import CoinMarketCapClickHouseAdapter


def test_upsert_idempotency():
    repo = CoinMarketCapClickHouseAdapter()
    assert repo.ensure_schema()

    # Prepare deterministic sample rows for same hour key
    sample = [
        {
            'symbol': 'TESTX',
            'name': 'Test Coin X',
            'slug': 'testx',
            'price': 1.23,
            'market_cap': 1000.0,
            'volume_24h': 10.0,
            'percent_change_1h': 0.1,
            'percent_change_24h': 0.2,
            'percent_change_7d': 0.3,
            'last_updated': datetime.now(),
            'cmc_rank': 999,
            'circulating_supply': 100.0,
            'total_supply': 200.0,
            'max_supply': 300.0,
        },
        {
            'symbol': 'TESTY',
            'name': 'Test Coin Y',
            'slug': 'testy',
            'price': 2.34,
            'market_cap': 2000.0,
            'volume_24h': 20.0,
            'percent_change_1h': 0.1,
            'percent_change_24h': 0.2,
            'percent_change_7d': 0.3,
            'last_updated': datetime.now(),
            'cmc_rank': 998,
            'circulating_supply': 100.0,
            'total_supply': 200.0,
            'max_supply': 300.0,
        },
    ]

    # Clean potential previous test rows for these symbols (keep historical data intact otherwise)
    repo.ch.exec(f"ALTER TABLE {repo.db}.crypto_prices DELETE WHERE symbol IN ('TESTX','TESTY')")

    ok1 = repo.upsert_prices(sample)
    assert ok1
    count1 = repo.ch.exec(f"SELECT count() FROM {repo.db}.crypto_prices WHERE symbol IN ('TESTX','TESTY')")
    first = count1[0][0] if count1 else 0

    # Upsert again with slightly different prices (should update, not insert new rows)
    sample[0]['price'] = 1.5
    sample[1]['price'] = 2.6
    ok2 = repo.upsert_prices(sample)
    assert ok2
    count2 = repo.ch.exec(f"SELECT count() FROM {repo.db}.crypto_prices WHERE symbol IN ('TESTX','TESTY')")
    second = count2[0][0] if count2 else 0

    assert first == second, f"Idempotency failed: first={first} second={second}"
