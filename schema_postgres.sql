-- PostgreSQL 14+ Schema for High Volume Voucher System
-- Run this in your PostgreSQL database

-- 1. Enable extensions if needed (usually standard text is fine)

-- 2. Create the RAW POOL table with PARTITIONING
-- Partitioning is CRITICAL for 6GB/day data to drop old data easily and query faster.
CREATE TABLE raw_pool (
    id BIGSERIAL, -- Use BIGSERIAL for massive amounts of rows
    phone_number VARCHAR(20) NOT NULL,
    provider VARCHAR(20),
    url_source TEXT,
    imported_at TIMESTAMP NOT NULL DEFAULT NOW(),
    file_source TEXT,
    raw_data JSONB -- Optional: store extra columns here
) PARTITION BY RANGE (imported_at);

-- Create Partitions (Example: Monthly or Daily depending on volume)
-- For 6GB/day, Daily partitions are recommended to keep index size manageable.
CREATE TABLE raw_pool_y2025m12d01 PARTITION OF raw_pool
    FOR VALUES FROM ('2025-12-01 00:00:00') TO ('2025-12-02 00:00:00');

CREATE TABLE raw_pool_y2025m12d02 PARTITION OF raw_pool
    FOR VALUES FROM ('2025-12-02 00:00:00') TO ('2025-12-03 00:00:00');
-- You would have a cron job to create these future partitions automatically

-- Indices on the Partitioned Table
-- Postgres automatically propagates these to partitions
CREATE INDEX idx_raw_pool_phone ON raw_pool (phone_number);
CREATE INDEX idx_raw_pool_provider_date ON raw_pool (provider, imported_at);


-- 3. Create SALES HISTORY table
CREATE TABLE sales_history (
    id BIGSERIAL PRIMARY KEY,
    phone_number VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    sold_at TIMESTAMP NOT NULL DEFAULT NOW(),
    transaction_id VARCHAR(50)
);

-- Index for the Anti-Join (Deduplication Check)
CREATE INDEX idx_sales_phone_sold ON sales_history (phone_number, sold_at);


-- 4. THE QUERY (Implementation in Postgres)
-- This is how the extraction query looks in Postgres syntax

/*
WITH Candidates AS (
    SELECT phone_number
    FROM raw_pool
    WHERE provider = 'tsel' 
      AND imported_at >= NOW() - INTERVAL '3 days' -- Postgres uses Partition Pruning here
),
Blacklist AS (
    SELECT phone_number
    FROM sales_history
    WHERE sold_at >= NOW() - INTERVAL '30 days'
)
SELECT c.phone_number
FROM Candidates c
LEFT JOIN Blacklist b ON c.phone_number = b.phone_number
WHERE b.phone_number IS NULL
LIMIT 1000000;
*/

