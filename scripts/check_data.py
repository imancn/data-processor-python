#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.config import config
import clickhouse_connect

def check_data():
    ch_config = config.get_clickhouse_config()
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password']
    )
    
    # Get all tables in the database
    tables_result = client.query("SHOW TABLES").result_rows
    tables = [row[0] for row in tables_result if row[0] != 'migrations']
    
    print("=== Data Counts ===")
    for table in tables:
        try:
            qr = client.query(f"SELECT COUNT(*) FROM data_warehouse.{table}")
            count = qr.result_rows[0][0]
            print(f"{table:20} {count}")
        except Exception as e:
            print(f"{table:20} ERROR: {e}")

if __name__ == "__main__":
    check_data()


