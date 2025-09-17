-- Migration: 001_create_cmc_latest_quotes_table.sql
-- Description: Create table for CMC latest cryptocurrency quotes

CREATE TABLE IF NOT EXISTS cmc_latest_quotes (
    id UInt64,
    name String,
    symbol String,
    slug String,
    cmc_rank UInt32,
    circulating_supply Nullable(Decimal128(2)),
    total_supply Nullable(Decimal128(2)),
    max_supply Nullable(Decimal128(2)),
    date_added DateTime('UTC'),
    num_market_pairs UInt32,
    tags String,
    platform Nullable(String),
    last_updated DateTime('UTC'),
    quote_currency String,
    price Decimal128(8),
    volume_24h Nullable(Decimal128(2)),
    volume_change_24h Nullable(Decimal128(4)),
    percent_change_1h Nullable(Decimal128(4)),
    percent_change_24h Nullable(Decimal128(4)),
    percent_change_7d Nullable(Decimal128(4)),
    percent_change_30d Nullable(Decimal128(4)),
    percent_change_60d Nullable(Decimal128(4)),
    percent_change_90d Nullable(Decimal128(4)),
    market_cap Nullable(Decimal128(2)),
    market_cap_dominance Nullable(Decimal128(4)),
    fully_diluted_market_cap Nullable(Decimal128(2)),
    is_active UInt8 DEFAULT 1,
    is_fiat UInt8 DEFAULT 0,
    infinite_supply UInt8 DEFAULT 0,
    self_reported_circulating_supply Nullable(Decimal128(2)),
    self_reported_market_cap Nullable(Decimal128(2)),
    tvl_ratio Nullable(Decimal128(4)),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    
    -- Materialized columns for Asia/Tehran timezone
    date_added_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(date_added, 'Asia/Tehran'),
    last_updated_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(last_updated, 'Asia/Tehran'),
    created_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(created_at, 'Asia/Tehran'),
    updated_at_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(updated_at, 'Asia/Tehran'),
    
    -- Time interval for incremental processing
    time_interval_start DateTime('UTC') MATERIALIZED toStartOfHour(last_updated),
    time_interval_start_tehran DateTime('Asia/Tehran') MATERIALIZED toTimeZone(time_interval_start, 'Asia/Tehran')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (symbol)
PARTITION BY toYYYYMM(last_updated)
PRIMARY KEY (symbol)
TTL last_updated + INTERVAL 1 YEAR;