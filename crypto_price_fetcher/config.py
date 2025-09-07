import os
from typing import List

def load_env_file():
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

class Config:
    def __init__(self):
        load_env_file()
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', '172.30.63.35')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', 'default')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'default')
        self.clickhouse_db = os.getenv('CLICKHOUSE_DB', 'crypto')
        self.cmc_api_key = os.getenv('CMC_API_KEY', '')
        symbols_str = os.getenv('SYMBOLS', 'BTC,ETH,SOL')
        self.symbols = [s.strip() for s in symbols_str.split(',') if s.strip()]

    def get_cmc_headers(self) -> dict:
        return {
            'X-CMC_PRO_API_KEY': self.cmc_api_key,
            'Accept': 'application/json'
        }

config = Config()