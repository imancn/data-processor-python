-- Migration: 007_recreate_cmc_time_scoped_tables_float.sql
-- Description: Recreate time-scoped CMC tables using Float64 for numeric columns to
--              avoid Decimal range/type issues when ingesting API data.

-- Hourly
DROP TABLE IF EXISTS cmc_hourly;
CREATE TABLE IF NOT EXISTS cmc_hourly (
    id UInt64,
    name String,
    symbol String,
    slug String,
    cmc_rank UInt32,
    circulating_supply Nullable(Float64),
    total_supply Nullable(Float64),
    max_supply Nullable(Float64),
    date_added DateTime('UTC'),
    num_market_pairs UInt32,
    tags Array(String),
    platform Nullable(String),
    last_updated DateTime('UTC'),
    quote_currency String,
    price Float64,
    volume_24h Nullable(Float64),
    volume_change_24h Nullable(Float64),
    percent_change_1h Nullable(Float64),
    percent_change_24h Nullable(Float64),
    percent_change_7d Nullable(Float64),
    percent_change_30d Nullable(Float64),
    percent_change_60d Nullable(Float64),
    percent_change_90d Nullable(Float64),
    market_cap Nullable(Float64),
    market_cap_dominance Nullable(Float64),
    fully_diluted_market_cap Nullable(Float64),
    time_scope String,
    timestamp DateTime('UTC'),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    date_added_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(date_added, 'Asia/Tehran'),
    last_updated_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(last_updated, 'Asia/Tehran'),
    timestamp_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(timestamp, 'Asia/Tehran'),
    created_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(created_at, 'Asia/Tehran'),
    updated_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(updated_at, 'Asia/Tehran'),
    time_interval_start DateTime('UTC') MATERIALIZED toStartOfHour(timestamp),
    time_interval_start_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(time_interval_start, 'Asia/Tehran')
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 1 YEAR;

-- Daily
DROP TABLE IF EXISTS cmc_daily;
CREATE TABLE IF NOT EXISTS cmc_daily AS cmc_hourly
ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 1 YEAR;

-- Weekly
DROP TABLE IF EXISTS cmc_weekly;
CREATE TABLE IF NOT EXISTS cmc_weekly AS cmc_hourly
ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 1 YEAR;

-- Monthly
DROP TABLE IF EXISTS cmc_monthly;
CREATE TABLE IF NOT EXISTS cmc_monthly AS cmc_hourly
ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 1 YEAR;

-- Yearly
DROP TABLE IF EXISTS cmc_yearly;
CREATE TABLE IF NOT EXISTS cmc_yearly AS cmc_hourly
ENGINE = MergeTree()
ORDER BY (symbol, timestamp, id)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (symbol, timestamp)
TTL timestamp + INTERVAL 5 YEAR;



