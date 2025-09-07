Crypto Price Fetcher

Minimal, idempotent cryptocurrency price fetcher that stores CoinMarketCap data in ClickHouse.

Features:
- Tehran timezone (+03:30) for all timestamps
- Hour-based idempotency by key (symbol, datetime)
- ReplacingMergeTree(version) for automatic deduplication
- Key-based UPSERT: update existing rows or insert new ones
- String datetime storage for BI compatibility
- Hourly cron execution (at minute 0 of every hour)

Quick Start:
1) Install: sudo apt install python3-aiohttp python3-clickhouse-driver python3-tz
2) Configure: cp env.example .env
3) Setup DB: ./run.sh setup_db
4) Run once: ./run.sh run_once
5) Setup cron: ./run.sh setup_cron

Environment Variables:
- CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
- CMC_API_KEY
- SYMBOLS (comma-separated)

Schema (essential):
- Engine: ReplacingMergeTree(version)
- Primary key: (symbol, datetime)
- Order by: (symbol, datetime, fetched_at)
- Materialized columns:
  - datetime: toStartOfHour(toDateTime(fetched_at)) formatted as '%Y-%m-%d %H:%i:%S'
  - date: toDate(toDateTime(fetched_at)) formatted as '%Y-%m-%d'

Idempotency:
- For each run/hour, for every symbol:
  - If (symbol, datetime) exists → UPDATE non-key columns
  - Else → INSERT new row
- Re-running within the same hour only updates existing rows (no duplicates)

Cron:
- Schedule: 0 * * * *
- Command: cd <project> && python3 cron_job.py >> cron.log 2>&1

