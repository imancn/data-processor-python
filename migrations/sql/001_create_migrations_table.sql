-- Migration 001: Create migrations table
-- This table tracks which migrations have been executed

CREATE TABLE IF NOT EXISTS migrations (
    id UInt32,
    name String,
    executed_at DateTime DEFAULT now(),
    checksum String
) ENGINE = MergeTree()
ORDER BY id;

-- Insert initial migration record
INSERT INTO migrations (id, name, checksum) VALUES (1, '001_create_migrations_table', 'initial');

SELECT 1;
