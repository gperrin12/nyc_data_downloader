-- ============================================================================
-- Athena DDL for MTA Subway Hourly Ridership
--
-- Run these AFTER load_mta_turnstile.py has uploaded Parquet files to S3.
--
-- Replace:
--   <YOUR_DB>     → your Glue database name (the one with gtp_tlc_data)
--   <YOUR_BUCKET> → your S3 bucket
--   <PREFIX>/     → optional prefix; remove if NYC_DATA_PREFIX was empty
--
-- Schema strategy:
-- Unlike the 311/collisions tables (all STRING), this loader writes proper
-- types: transit_timestamp as TIMESTAMP and the ridership/transfers/lat/lon
-- fields as DOUBLE. Derived h3_r8/r9/r10 are STRING, matching the other tables.
-- Full history spans two source resources (wujg-7c2s 2020-2024, 5wq4-mkjj
-- 2025-present), both loaded into this single table.
-- ============================================================================


-- ---- MTA Subway Hourly Ridership ------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.mta_turnstile_hourly (
  transit_timestamp     TIMESTAMP,
  transit_mode          STRING,
  station_complex_id    STRING,
  station_complex       STRING,
  borough               STRING,
  payment_method        STRING,
  fare_class_category   STRING,
  ridership             DOUBLE,
  transfers             DOUBLE,
  latitude              DOUBLE,
  longitude             DOUBLE,
  h3_r8                 STRING,
  h3_r9                 STRING,
  h3_r10                STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/mta_turnstile/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ---- Discover partitions ---------------------------------------------------
-- Run after the loader completes (or after adding new months later).

MSCK REPAIR TABLE <YOUR_DB>.mta_turnstile_hourly;


-- ============================================================================
-- Sanity-check queries — run these after MSCK REPAIR
-- ============================================================================

-- Row counts by year (verify partition pruning: bytes scanned should scale
-- with the number of years touched, not the whole table).
SELECT year, COUNT(*) AS n
FROM <YOUR_DB>.mta_turnstile_hourly
GROUP BY year
ORDER BY year;

-- Busiest station complexes by total ridership in a single month.
SELECT station_complex, SUM(ridership) AS total_ridership
FROM <YOUR_DB>.mta_turnstile_hourly
WHERE year = '2024' AND month = '01'
GROUP BY station_complex
ORDER BY total_ridership DESC
LIMIT 20;

-- Confirm H3 indexing populated for stations with coordinates.
SELECT h3_r9, COUNT(*) AS n
FROM <YOUR_DB>.mta_turnstile_hourly
WHERE year = '2024' AND h3_r9 IS NOT NULL
GROUP BY h3_r9
ORDER BY n DESC
LIMIT 10;
