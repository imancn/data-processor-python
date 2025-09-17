# src/core/config.py
import os
from typing import Dict, Any

class Config:
    """
    Centralized configuration management for the application.
    Loads settings from environment variables.
    """
    def __init__(self):
        self._settings = {}
        self._load_env_config()

    def _load_env_config(self):
        """Load configuration from environment variables."""
        self._settings['LOG_LEVEL'] = os.getenv('LOG_LEVEL', 'INFO')
        self._settings['LOG_FILE'] = os.getenv('LOG_FILE', 'logs/application.log')
        self._settings['TIMEOUT'] = int(os.getenv('TIMEOUT', '30'))
        self._settings['BATCH_SIZE'] = int(os.getenv('BATCH_SIZE', '1000'))

        # ClickHouse configuration
        self._settings['CLICKHOUSE_HOST'] = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self._settings['CLICKHOUSE_PORT'] = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        self._settings['CLICKHOUSE_USER'] = os.getenv('CLICKHOUSE_USER', 'default')
        self._settings['CLICKHOUSE_PASSWORD'] = os.getenv('CLICKHOUSE_PASSWORD', '')
        self._settings['CLICKHOUSE_DATABASE'] = os.getenv('CLICKHOUSE_DATABASE', 'invex_data')

        # CoinMarketCap API configuration
        self._settings['CMC_API_KEY'] = os.getenv('CMC_API_KEY')
        self._settings['CMC_API_BASE_URL'] = os.getenv('CMC_API_BASE_URL', 'https://pro-api.coinmarketcap.com/v1')

        # Example external API (e.g., for stock data)
        self._settings['STOCK_API_BASE_URL'] = os.getenv('STOCK_API_BASE_URL', 'https://api.stockdata.com/v1')
        self._settings['STOCK_API_KEY'] = os.getenv('STOCK_API_KEY')

        # Example external API (e.g., for weather data)
        self._settings['WEATHER_API_BASE_URL'] = os.getenv('WEATHER_API_BASE_URL', 'https://api.openweathermap.org/data/2.5')
        self._settings['WEATHER_API_KEY'] = os.getenv('WEATHER_API_KEY')

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self._settings.get(key, default)

    @property
    def log_level(self) -> str:
        return self.get('LOG_LEVEL')

    @property
    def log_file(self) -> str:
        return self.get('LOG_FILE')

    @property
    def timeout(self) -> int:
        return self.get('TIMEOUT')

    @property
    def batch_size(self) -> int:
        return self.get('BATCH_SIZE')

    def get_clickhouse_config(self) -> Dict[str, Any]:
        return {
            'host': self.get('CLICKHOUSE_HOST'),
            'port': self.get('CLICKHOUSE_PORT'),
            'user': self.get('CLICKHOUSE_USER'),
            'password': self.get('CLICKHOUSE_PASSWORD'),
            'database': self.get('CLICKHOUSE_DATABASE')
        }

    def get_cmc_api_config(self) -> Dict[str, Any]:
        return {
            'api_key': self.get('CMC_API_KEY'),
            'base_url': self.get('CMC_API_BASE_URL')
        }

    def get_stock_api_config(self) -> Dict[str, Any]:
        return {
            'api_key': self.get('STOCK_API_KEY'),
            'base_url': self.get('STOCK_API_BASE_URL')
        }

    def get_weather_api_config(self) -> Dict[str, Any]:
        return {
            'api_key': self.get('WEATHER_API_KEY'),
            'base_url': self.get('WEATHER_API_BASE_URL')
        }

config = Config()