CREATE DATABASE IF NOT EXISTS crypto;
USE crypto;
CREATE TABLE IF NOT EXISTS crypto.crypto_prices (
    symbol String,
    name String,
    slug String,
    price Float64,
    market_cap Float64,
    volume_24h Float64,
    percent_change_1h Float64,
    percent_change_24h Float64,
    percent_change_7d Float64,
    last_updated String,
    cmc_rank UInt32,
    circulating_supply Float64,
    total_supply Float64,
    max_supply Float64,
    fetched_at String,
    datetime String MATERIALIZED formatDateTime(toStartOfHour(toDateTime(fetched_at)), '%Y-%m-%d %H:%i:%S'),
    date String MATERIALIZED formatDateTime(toDate(toDateTime(fetched_at)), '%Y-%m-%d'),
    version UInt64 MATERIALIZED toUnixTimestamp(toDateTime(fetched_at))
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, datetime, fetched_at)
PRIMARY KEY (symbol, datetime);
CREATE INDEX IF NOT EXISTS idx_symbol ON crypto.crypto_prices (symbol) TYPE minmax GRANULARITY 1;
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.crypto_prices_daily
ENGINE = SummingMergeTree()
ORDER BY (symbol, toDate(datetime))
AS SELECT
    symbol,
    toDate(datetime) as date,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    avg(market_cap) as avg_market_cap,
    avg(volume_24h) as avg_volume_24h,
    avg(percent_change_24h) as avg_percent_change_24h,
    count() as data_points
FROM crypto.crypto_prices
GROUP BY symbol, toDate(datetime);
GRANT ALL ON crypto.* TO default;
SHOW TABLES FROM crypto;
