from typing import List, Dict, Any
from datetime import datetime
import pytz
from adapters._bases.clickhouse_adapter import ClickHouseAdapter


class CoinMarketCapClickHouseAdapter:
    def __init__(self):
        self.ch = ClickHouseAdapter()
        self.db = self.ch.database
        self.last_inserted = 0
        self.last_updated = 0

    def ensure_schema(self) -> bool:
        try:
            self.ch.exec(f'CREATE DATABASE IF NOT EXISTS {self.db}')
            q = f"""
            CREATE TABLE IF NOT EXISTS {self.db}.crypto_prices (
                symbol String,
                name String,
                slug String,
                price Float64,
                market_cap Float64,
                volume_24h Float64,
                percent_change_1h Float64,
                percent_change_24h Float64,
                percent_change_7d Float64,
                last_updated String,
                cmc_rank UInt32,
                circulating_supply Float64,
                total_supply Float64,
                max_supply Float64,
                fetched_at String,
                datetime String MATERIALIZED formatDateTime(toStartOfHour(toDateTime(fetched_at)), '%Y-%m-%d %H:%i:%S'),
                date String MATERIALIZED formatDateTime(toDate(toDateTime(fetched_at)), '%Y-%m-%d'),
                version UInt64 MATERIALIZED toUnixTimestamp(toDateTime(fetched_at))
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY (symbol, datetime, fetched_at)
            PRIMARY KEY (symbol, datetime)
            """
            self.ch.exec(q)
            return True
        except Exception:
            return False

    def upsert_prices(self, data: List[Dict[str, Any]], target_hour: datetime | None = None) -> bool:
        if not data:
            return True
        try:
            tehran_tz = pytz.timezone('Asia/Tehran')
            base_time = target_hour.astimezone(tehran_tz) if target_hour else datetime.now(tehran_tz)
            hour = base_time.replace(minute=0, second=0, microsecond=0)
            last_updated = hour.strftime('%Y-%m-%d %H:%M:%S')
            fetched_at = base_time.strftime('%Y-%m-%d %H:%M:%S')
            hour_key = hour.strftime('%Y-%m-%d %H:%M:%S')

            def f(v, d=0.0):
                try:
                    return d if v is None else float(v)
                except Exception:
                    return d

            def i(v, d=0):
                try:
                    return d if v is None else int(v)
                except Exception:
                    return d

            to_insert = []
            inserted = 0
            updated = 0
            for item in data:
                symbol = item.get('symbol', '')
                if not symbol:
                    continue
                exists = self.ch.exec(f"""
                    SELECT 1 FROM {self.db}.crypto_prices
                    WHERE symbol = '{symbol}' AND datetime = '{hour_key}' LIMIT 1
                """)
                if exists:
                    self.ch.write_with_retry(f"""
                        ALTER TABLE {self.db}.crypto_prices UPDATE
                          name = '{item.get('name', '')}',
                          slug = '{item.get('slug', '')}',
                          price = {f(item.get('price'))},
                          market_cap = {f(item.get('market_cap'))},
                          volume_24h = {f(item.get('volume_24h'))},
                          percent_change_1h = {f(item.get('percent_change_1h'))},
                          percent_change_24h = {f(item.get('percent_change_24h'))},
                          percent_change_7d = {f(item.get('percent_change_7d'))},
                          last_updated = '{last_updated}',
                          cmc_rank = {i(item.get('cmc_rank'))},
                          circulating_supply = {f(item.get('circulating_supply'))},
                          total_supply = {f(item.get('total_supply'))},
                          max_supply = {f(item.get('max_supply'))}
                        WHERE symbol = '{symbol}' AND datetime = '{hour_key}'
                    """)
                    updated += 1
                else:
                    to_insert.append([
                        symbol,
                        item.get('name', ''),
                        item.get('slug', ''),
                        f(item.get('price')),
                        f(item.get('market_cap')),
                        f(item.get('volume_24h')),
                        f(item.get('percent_change_1h')),
                        f(item.get('percent_change_24h')),
                        f(item.get('percent_change_7d')),
                        last_updated,
                        i(item.get('cmc_rank')),
                        f(item.get('circulating_supply')),
                        f(item.get('total_supply')),
                        f(item.get('max_supply')),
                        fetched_at
                    ])
            if to_insert:
                self.ch.write_with_retry(f"""
                INSERT INTO {self.db}.crypto_prices
                (symbol, name, slug, price, market_cap, volume_24h,
                 percent_change_1h, percent_change_24h, percent_change_7d,
                 last_updated, cmc_rank, circulating_supply, total_supply, max_supply, fetched_at)
                VALUES
                """, to_insert)
                inserted += len(to_insert)
            self.last_inserted = inserted
            self.last_updated = updated
            return True
        except Exception:
            return False


