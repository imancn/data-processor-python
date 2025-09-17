-- Migration: 004_create_cmc_weekly_table.sql
-- Description: Create table for CMC weekly cryptocurrency data

CREATE TABLE IF NOT EXISTS cmc_weekly (
    id UInt64,
    name String,
    symbol String,
    slug String,
    cmc_rank UInt32,
    circulating_supply Nullable(Decimal64(2)),
    total_supply Nullable(Decimal64(2)),
    max_supply Nullable(Decimal64(2)),
    date_added DateTime('UTC'),
    num_market_pairs UInt32,
    tags Array(String),
    platform Nullable(String),
    last_updated DateTime('UTC'),
    quote_currency String,
    price Decimal64(8),
    volume_24h Nullable(Decimal64(2)),
    volume_change_24h Nullable(Decimal64(4)),
    percent_change_1h Nullable(Decimal64(4)),
    percent_change_24h Nullable(Decimal64(4)),
    percent_change_7d Nullable(Decimal64(4)),
    percent_change_30d Nullable(Decimal64(4)),
    percent_change_60d Nullable(Decimal64(4)),
    percent_change_90d Nullable(Decimal64(4)),
    market_cap Nullable(Decimal64(2)),
    market_cap_dominance Nullable(Decimal64(4)),
    fully_diluted_market_cap Nullable(Decimal64(2)),
    time_scope String,
    timestamp DateTime('UTC'),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    
    -- Materialized columns for Asia/Tehran timezone
    date_added_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(date_added, 'Asia/Tehran'),
    last_updated_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(last_updated, 'Asia/Tehran'),
    timestamp_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(timestamp, 'Asia/Tehran'),
    created_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(created_at, 'Asia/Tehran'),
    updated_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(updated_at, 'Asia/Tehran'),
    
    -- Time interval for incremental processing
    time_interval_start DateTime('UTC') MATERIALIZED toStartOfWeek(timestamp),
    time_interval_start_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(time_interval_start, 'Asia/Tehran')
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 3 YEAR;
