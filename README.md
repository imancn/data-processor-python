# Async Crypto Price Fetcher with Poetry, ClickHouse, and Cron

## 1. Project Setup

1. Create a new project with Poetry:
   ```bash
   poetry new crypto_price_fetcher
   cd crypto_price_fetcher
````

2. Add dependencies:

   ```bash
   poetry add httpx clickhouse-driver pydantic python-dotenv
   ```

   * `httpx` → async HTTP client
   * `clickhouse-driver` → Python driver for ClickHouse
   * `pydantic` → for data validation and config
   * `python-dotenv` → load secrets from `.env`

---

## 2. Environment Config

Create `.env` file in project root:

```env
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=crypto
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
API_BASE=https://api.binance.com/api/v3
```

---

## 3. ClickHouse Table Schema

Run this query in ClickHouse:

```sql
CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.prices (
    symbol String,
    price Float64,
    fetched_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (symbol, fetched_at);
```

---

## 4. Python Project Structure

```
crypto_price_fetcher/
│── crypto_price_fetcher/
│   ├── __init__.py
│   ├── config.py
│   ├── fetcher.py
│   ├── storage.py
│   └── main.py
│── tests/
│── pyproject.toml
│── .env
```

---

## 5. Implementation

### `config.py`

```python
from pydantic import BaseSettings

class Settings(BaseSettings):
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str
    symbols: str
    api_base: str

    class Config:
        env_file = ".env"

settings = Settings()
```

### `fetcher.py`

```python
import httpx
from .config import settings

async def fetch_price(symbol: str) -> float:
    url = f"{settings.api_base}/ticker/price"
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(url, params={"symbol": symbol})
        response.raise_for_status()
        data = response.json()
        return float(data["price"])
```

### `storage.py`

```python
from clickhouse_driver import Client
from .config import settings

client = Client(
    host=settings.clickhouse_host,
    port=settings.clickhouse_port,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
    database=settings.clickhouse_db,
)

def save_prices(prices: list[tuple[str, float]]):
    client.execute(
        "INSERT INTO prices (symbol, price) VALUES",
        prices
    )
```

### `main.py`

```python
import asyncio
from datetime import datetime
from .config import settings
from .fetcher import fetch_price
from .storage import save_prices

async def run():
    symbols = settings.symbols.split(",")
    tasks = [fetch_price(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks)

    prices = [(s, p) for s, p in zip(symbols, results)]
    save_prices(prices)
    print(f"[{datetime.now()}] Saved {len(prices)} prices.")

if __name__ == "__main__":
    asyncio.run(run())
```

---

## 6. Cron Job Setup

1. Make a Poetry run script executable:

   ```bash
   poetry run python -m crypto_price_fetcher.main
   ```

2. Open crontab:

   ```bash
   crontab -e
   ```

3. Add entry to run every hour:

   ```cron
   0 * * * * cd /path/to/crypto_price_fetcher && poetry run python -m crypto_price_fetcher.main >> logs.txt 2>&1
   ```

---

## 7. Testing

Manually run:

```bash
poetry run python -m crypto_price_fetcher.main
```

Check data:

```sql
SELECT * FROM crypto.prices ORDER BY fetched_at DESC LIMIT 10;
```

---

## 8. Notes

* Extend `SYMBOLS` in `.env` dynamically without code changes.
* Add retry/backoff for API failures.
* Later improvements:

  * Real-time streaming with Kafka/Flink.
  * Store OHLCV instead of just spot price.

```

---

also **include a Dockerfile + docker-compose** so that Cron + Poetry + ClickHouse run in containers without needing manual setup
```
