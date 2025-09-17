#!/usr/bin/env python3
import sys
sys.path.insert(0, 'src')

from core.config import config
from clickhouse_driver import Client

def check_data():
    ch_config = config.get_clickhouse_config()
    client = Client(
        host=ch_config['host'],
        port=ch_config['port'],
        user=ch_config['user'],
        password=ch_config['password']
    )
    
    tables = ["cmc_latest_quotes", "cmc_hourly", "cmc_daily", "cmc_weekly", "cmc_monthly", "cmc_yearly"]
    
    print("=== Data Counts ===")
    for table in tables:
        try:
            result = client.execute(f"SELECT COUNT(*) FROM data_warehouse.{table}")
            count = result[0][0]
            print(f"{table:20} {count}")
        except Exception as e:
            print(f"{table:20} ERROR: {e}")

if __name__ == "__main__":
    check_data()


