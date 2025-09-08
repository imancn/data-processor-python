from clickhouse_driver import Client
from configurations.config import config


class ClickHouseAdapter:
    def __init__(self):
        self._client = None
        self.host = config.clickhouse_host
        self.port = config.clickhouse_port
        self.user = config.clickhouse_user
        self.password = config.clickhouse_password
        self.database = config.clickhouse_db

    def client(self) -> Client:
        if self._client is None:
            self._client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        return self._client

    def exec(self, query: str, params=None):
        return self.client().execute(query, params)

    def ping(self) -> bool:
        try:
            return self.exec('SELECT 1')[0][0] == 1
        except Exception:
            return False

    def write_with_retry(self, query: str, params=None, retries: int = 3, backoff_seconds: float = 0.5):
        attempt = 0
        last_err = None
        while attempt < retries:
            try:
                return self.exec(query, params)
            except Exception as e:
                last_err = e
                attempt += 1
                if attempt >= retries:
                    break
                import time
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
        raise last_err


