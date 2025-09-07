import asyncio
from typing import List, Dict, Any
from datetime import datetime
from clickhouse_driver import Client
from .config import config

class ClickHouseNativeStorage:
    def __init__(self):
        self.host = config.clickhouse_host
        self.port = config.clickhouse_port
        self.user = config.clickhouse_user
        self.password = config.clickhouse_password
        self.database = config.clickhouse_db
        self.client = None

    def _get_client(self):
        if self.client is None:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
        return self.client

    async def test_connection(self) -> bool:
        try:
            client = self._get_client()
            result = client.execute('SELECT 1')
            return result[0][0] == 1
        except Exception as e:
            print(f"ClickHouse connection test failed: {e}")
            return False

    async def create_database(self) -> bool:
        try:
            client = self._get_client()
            client.execute(f'CREATE DATABASE IF NOT EXISTS {self.database}')
            print("✅ Database creation successful")
            return True
        except Exception as e:
            print(f"Error creating database: {e}")
            return False
    
    async def create_table(self) -> bool:
        try:
            client = self._get_client()
            table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.crypto_prices (
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
            client.execute(table_query)
            print("✅ Table creation successful")
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
    
    async def save_crypto_data(self, data: List[Dict[str, Any]]) -> bool:
        if not data:
            return True
        try:
            client = self._get_client()
            import pytz
            tehran_tz = pytz.timezone('Asia/Tehran')
            current_time = datetime.now(tehran_tz)
            hour_time = current_time.replace(minute=0, second=0, microsecond=0)
            consistent_last_updated = hour_time.strftime('%Y-%m-%d %H:%M:%S')
            actual_fetched_at = datetime.now(tehran_tz).strftime('%Y-%m-%d %H:%M:%S')
            hour_datetime = hour_time.strftime('%Y-%m-%d %H:%M:%S')

            def safe_float(value, default=0.0):
                if value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default

            def safe_int(value, default=0):
                if value is None:
                    return default
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default

            values = []
            updated_count = 0
            inserted_count = 0

            for item in data:
                symbol = item.get('symbol', '')
                if not symbol:
                    continue

                last_updated_str = consistent_last_updated

                existing_record = client.execute(f'''
                    SELECT 1 FROM {self.database}.crypto_prices
                    WHERE symbol = '{symbol}' AND datetime = '{hour_datetime}'
                    LIMIT 1
                ''')

                if existing_record:
                    client.execute(f'''
                        ALTER TABLE {self.database}.crypto_prices
                        UPDATE
                            name = '{item.get('name', '')}',
                            slug = '{item.get('slug', '')}',
                            price = {safe_float(item.get('price'))},
                            market_cap = {safe_float(item.get('market_cap'))},
                            volume_24h = {safe_float(item.get('volume_24h'))},
                            percent_change_1h = {safe_float(item.get('percent_change_1h'))},
                            percent_change_24h = {safe_float(item.get('percent_change_24h'))},
                            percent_change_7d = {safe_float(item.get('percent_change_7d'))},
                            last_updated = '{last_updated_str}',
                            cmc_rank = {safe_int(item.get('cmc_rank'))},
                            circulating_supply = {safe_float(item.get('circulating_supply'))},
                            total_supply = {safe_float(item.get('total_supply'))},
                            max_supply = {safe_float(item.get('max_supply'))}
                        WHERE symbol = '{symbol}' AND datetime = '{hour_datetime}'
                    ''')
                    updated_count += 1
                else:
                    values.append([
                        symbol,
                        item.get('name', ''),
                        item.get('slug', ''),
                        safe_float(item.get('price')),
                        safe_float(item.get('market_cap')),
                        safe_float(item.get('volume_24h')),
                        safe_float(item.get('percent_change_1h')),
                        safe_float(item.get('percent_change_24h')),
                        safe_float(item.get('percent_change_7d')),
                        last_updated_str,
                        safe_int(item.get('cmc_rank')),
                        safe_float(item.get('circulating_supply')),
                        safe_float(item.get('total_supply')),
                        safe_float(item.get('max_supply')),
                        actual_fetched_at
                    ])
                    inserted_count += 1

            if values:
                insert_query = f"""
                INSERT INTO {self.database}.crypto_prices
                (symbol, name, slug, price, market_cap, volume_24h,
                 percent_change_1h, percent_change_24h, percent_change_7d,
                 last_updated, cmc_rank, circulating_supply, total_supply, max_supply, fetched_at)
                VALUES
                """
                client.execute(insert_query, values)

            total_processed = updated_count + inserted_count
            print(f"✅ Processed {total_processed} records: {inserted_count} inserted, {updated_count} updated")
            print(f"🕐 Tehran time: {hour_datetime}")
            return True

        except Exception as e:
            print(f"❌ Error saving crypto data: {e}")
            return False
    
    async def get_recent_data(self, limit: int = 10) -> List[Dict[str, Any]]:
        try:
            client = self._get_client()
            query = f"""
            SELECT symbol, name, price, market_cap, volume_24h,
                   percent_change_24h, last_updated, fetched_at, datetime, date
            FROM {self.database}.crypto_prices
            ORDER BY fetched_at DESC
            LIMIT {limit}
            """
            result = client.execute(query)
            data = []
            for row in result:
                data.append({
                    'symbol': row[0],
                    'name': row[1],
                    'price': row[2],
                    'market_cap': row[3],
                    'volume_24h': row[4],
                    'percent_change_24h': row[5],
                    'last_updated': row[6],
                    'fetched_at': row[7],
                    'datetime': row[8],
                    'date': row[9]
                })
            return data
        except Exception as e:
            print(f"Error fetching recent data: {e}")
            return []

storage = ClickHouseNativeStorage()
