Data Processor (Crypto Price Fetcher)

Production-ready, idempotent ETL that fetches cryptocurrency prices from CoinMarketCap and stores hourly snapshots in ClickHouse.

Key features
- Tehran timezone (+03:30) for all timestamps
- Hour-based idempotency with key (symbol, datetime)
- ReplacingMergeTree(version) engine with explicit UPSERT logic
- Date/Datetime stored as String for BI friendliness
- Modular ETL (adapters/extractors/transformers/loaders/crons)
- Poetry for dependency and environment management
- Backfill support and cron management by job name

Project layout (src/)
```
configurations/
  config.py
adapters/
  _bases/
    clickhouse_adapter.py
    http_adapter.py
  http/
    coin_market_cap_http_adapter.py
  clickhouse/
    coin_market_cap_clickhouse_adapter.py
extractors/
  coin_market_cap_extractor.py
transormers/
  coin_market_cap_transformer.py
loaders/
  coin_market_cap_loader.py
crons/
  registry.py
  run.py
  install.py
main.py
```

ClickHouse schema
- Engine: ReplacingMergeTree(version)
- Primary key: (symbol, datetime)
- Order by: (symbol, datetime, fetched_at)
- Materialized (String) columns:
  - datetime: formatDateTime(toStartOfHour(toDateTime(fetched_at)), '%Y-%m-%d %H:%i:%S')
  - date: formatDateTime(toDate(toDateTime(fetched_at)), '%Y-%m-%d')

Timezone policy
- All application timestamps use Asia/Tehran.
- Hour key is toStartOfHour(Tehran time). No second rounding for fetched_at.

Idempotency policy
- Per hour per symbol, perform key-based UPSERT:
  - If (symbol, datetime) exists → ALTER UPDATE non-key columns
  - Else → INSERT
- Historical data is never deleted for production symbols.

Requirements
- Python 3.10–3.12
- ClickHouse reachable from this host
- CoinMarketCap API key

Setup (Poetry)
```
cp env.example .env
./run.sh check
poetry install --no-root
```

Configuration (.env)
- CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
- CMC_API_KEY
- SYMBOLS=BTC,ETH,SOL (comma-separated)

Core commands (run.sh)
- check: verify Python/Poetry installed
- test: run full pytest suite
- setup_db: create database if missing
- drop_db: drop crypto.crypto_prices table
- kill: kill any running cron processes
- cron_run NAME: run a cron job once (default: cmc_hourly_prices)
- setup_cron [NAME]: install cron for a job name (hourly at minute 0)
- cron_backfill NAME HOURS: backfill a job for past HOURS (one-shot)
- clean: kill + drop_db + setup_db + run cmc_hourly_prices once

Cron management
- Registry: src/crons/registry.py (JOBS dict maps job names to async functions)
- Runner: src/crons/run.py
  - Run: `python -m crons.run cmc_hourly_prices`
  - Backfill: `python -m crons.run cmc_hourly_prices backfill 24`
- Installer: src/crons/install.py
  - `./run.sh setup_cron cmc_hourly_prices` installs hourly cron
  - Logs go to logs/cron.log

Backfill
- One-shot foreground operation that iterates past N hours and upserts with explicit hour keys.
- Process exits when done. Can be wrapped with timeout if desired.

Testing
- `./run.sh test` (Poetry + pytest)
- Includes idempotency integration tests and adapter/cron unit tests

Deployment
- Ensure .env configured (CLICKHOUSE_* and CMC_API_KEY)
- Install deps and run tests: `./run.sh check && poetry install --no-root && ./run.sh test`
- Install cron: `./run.sh setup_cron cmc_hourly_prices`
- Optional backfill before enabling cron: `./run.sh cron_backfill cmc_hourly_prices 168` (last 7 days)

Developer guide: Build a new ETL pipeline
1) HTTP adapter (source)
   - Add an adapter in `src/adapters/http/<provider>_http_adapter.py` using `HttpAdapter`.
2) Extractor
   - Add `src/extractors/<provider>_extractor.py` with a function returning normalized rows.
3) Transformer (optional)
   - Add transformations in `src/transormers/<provider>_transformer.py`.
4) Storage adapter (sink)
   - Implement repository under `src/adapters/clickhouse/<provider>_clickhouse_adapter.py`.
   - Follow the idempotent `upsert_prices(rows, target_hour=None)` signature.
5) Loader
   - Wrap storage logic in `src/loaders/<provider>_loader.py` with `ensure_ready()` and `load(rows)`.
6) Cron job
   - Register a job in `src/crons/registry.py` and map it in `JOBS`.
   - Your job should orchestrate: extract → (transform) → load.
7) Run & schedule
   - `./run.sh cron_run <job_name>` to run once
   - `./run.sh setup_cron <job_name>` to schedule hourly
8) Backfill (optional)
   - Extend `backfill()` in `registry.py` if needed for your job.

Notes
- Avoid deleting historical production data. Only UPDATE or INSERT by key.
- Keep all timestamps in Tehran timezone.
- Store Date/Datetime as String if BI tooling requires it.
