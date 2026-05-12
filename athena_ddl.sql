-- ============================================================================
-- Athena DDL for NYC 311 + NYPD Collisions
--
-- Run these AFTER the loader has uploaded Parquet files to S3.
--
-- Replace:
--   <YOUR_DB>     → your Glue database name (the one with gtp_tlc_data)
--   <YOUR_BUCKET> → your S3 bucket
--   <PREFIX>/     → optional prefix; remove if NYC_DATA_PREFIX was empty
--
-- Schema strategy:
-- All columns are STRING. The loader writes everything as string to avoid
-- load-time type drift. Cast at query time with CAST/TRY_CAST. This is the
-- same pattern your existing TLC `par` table uses for raw fields.
-- ============================================================================


-- ---- 311 Service Requests --------------------------------------------------
-- Columns reflect the union of fields across the 2010-2019 archive and the
-- 2020-present current dataset. Both share the core ~40-column schema; minor
-- additions in the current dataset land here too. Missing columns return NULL
-- in older partitions, which is the desired behavior.

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.nyc_311 (
  unique_key                       STRING,
  created_date                     STRING,
  closed_date                      STRING,
  agency                           STRING,
  agency_name                      STRING,
  complaint_type                   STRING,
  descriptor                       STRING,
  location_type                    STRING,
  incident_zip                     STRING,
  incident_address                 STRING,
  street_name                      STRING,
  cross_street_1                   STRING,
  cross_street_2                   STRING,
  intersection_street_1            STRING,
  intersection_street_2            STRING,
  address_type                     STRING,
  city                             STRING,
  landmark                         STRING,
  facility_type                    STRING,
  status                           STRING,
  due_date                         STRING,
  resolution_description           STRING,
  resolution_action_updated_date   STRING,
  community_board                  STRING,
  bbl                              STRING,
  borough                          STRING,
  x_coordinate_state_plane         STRING,
  y_coordinate_state_plane         STRING,
  open_data_channel_type           STRING,
  park_facility_name               STRING,
  park_borough                     STRING,
  vehicle_type                     STRING,
  taxi_company_borough             STRING,
  taxi_pick_up_location            STRING,
  bridge_highway_name              STRING,
  bridge_highway_direction         STRING,
  road_ramp                        STRING,
  bridge_highway_segment           STRING,
  latitude                         STRING,
  longitude                        STRING,
  location_latitude                STRING,
  location_longitude               STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/nyc_311/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ---- NYPD Motor Vehicle Collisions ----------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.nypd_collisions (
  collision_id                       STRING,
  crash_date                         STRING,
  crash_time                         STRING,
  borough                            STRING,
  zip_code                           STRING,
  latitude                           STRING,
  longitude                          STRING,
  location_latitude                  STRING,
  location_longitude                 STRING,
  on_street_name                     STRING,
  cross_street_name                  STRING,
  off_street_name                    STRING,
  number_of_persons_injured          STRING,
  number_of_persons_killed           STRING,
  number_of_pedestrians_injured      STRING,
  number_of_pedestrians_killed       STRING,
  number_of_cyclist_injured          STRING,
  number_of_cyclist_killed           STRING,
  number_of_motorist_injured         STRING,
  number_of_motorist_killed          STRING,
  contributing_factor_vehicle_1      STRING,
  contributing_factor_vehicle_2      STRING,
  contributing_factor_vehicle_3      STRING,
  contributing_factor_vehicle_4      STRING,
  contributing_factor_vehicle_5      STRING,
  vehicle_type_code1                 STRING,
  vehicle_type_code2                 STRING,
  vehicle_type_code_3                STRING,
  vehicle_type_code_4                STRING,
  vehicle_type_code_5                STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/nypd_collisions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ---- Discover partitions ---------------------------------------------------
-- Run after each loader completion (or after adding new months later).

MSCK REPAIR TABLE <YOUR_DB>.nyc_311;
MSCK REPAIR TABLE <YOUR_DB>.nypd_collisions;


-- ============================================================================
-- Sanity-check queries — run these after MSCK REPAIR
-- ============================================================================

-- Row counts by year (verify partition pruning works: bytes scanned should
-- scale with the number of years touched, not the whole table).
SELECT year, COUNT(*) AS n
FROM <YOUR_DB>.nyc_311
GROUP BY year
ORDER BY year;

SELECT year, COUNT(*) AS n
FROM <YOUR_DB>.nypd_collisions
GROUP BY year
ORDER BY year;

-- Confirm lat/lon parses as numeric.
SELECT
  CAST(latitude  AS DOUBLE) AS lat,
  CAST(longitude AS DOUBLE) AS lon,
  COUNT(*) AS n
FROM <YOUR_DB>.nypd_collisions
WHERE year = '2024'
  AND latitude IS NOT NULL
GROUP BY 1, 2
ORDER BY n DESC
LIMIT 10;

-- Cross-table join sanity: top complaint types in zones with the most crashes.
-- Uses ST_CONTAINS to map collision lat/lon to taxi_zones polygons.
WITH crash_points AS (
  SELECT ST_POINT(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)) AS pt
  FROM <YOUR_DB>.nypd_collisions
  WHERE year = '2024' AND latitude IS NOT NULL AND longitude IS NOT NULL
),
crash_zones AS (
  SELECT z.zone_id, COUNT(*) AS crashes
  FROM crash_points c
  JOIN <YOUR_DB>.taxi_zones z
    ON ST_CONTAINS(z.geometry, c.pt)  -- adjust column name to match your taxi_zones table
  GROUP BY z.zone_id
)
SELECT * FROM crash_zones ORDER BY crashes DESC LIMIT 10;
