-- ============================================================================
-- Athena DDL for ACS census tract demographics.
--
-- Schema: one row per tract, joined to census_tracts on geoid.
-- Columns are STRING (consistent with our other tables); cast at query time.
-- Two vintages stored side-by-side: _2018 (2014-2018 5yr) and _2023 (2019-2023 5yr).
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.census_tract_demographics (
  geoid                                STRING,

  -- 2018 vintage (2014-2018 5-year)
  total_pop_2018                       STRING,
  median_age_2018                      STRING,
  median_household_income_2018         STRING,
  poverty_universe_2018                STRING,
  poverty_below_2018                   STRING,
  race_white_alone_2018                STRING,
  race_black_alone_2018                STRING,
  race_asian_alone_2018                STRING,
  hispanic_or_latino_2018              STRING,
  edu_universe_25plus_2018             STRING,
  edu_bachelors_2018                   STRING,
  edu_masters_2018                     STRING,
  edu_professional_2018                STRING,
  edu_doctorate_2018                   STRING,
  housing_universe_2018                STRING,
  housing_owner_occupied_2018          STRING,
  median_gross_rent_2018               STRING,
  median_household_size_2018           STRING,
  lang_universe_2018                   STRING,
  lang_lim_eng_spanish_2018            STRING,
  lang_lim_eng_other_indo_european_2018 STRING,
  lang_lim_eng_asian_pacific_2018      STRING,
  lang_lim_eng_other_2018              STRING,

  -- 2023 vintage (2019-2023 5-year)
  total_pop_2023                       STRING,
  median_age_2023                      STRING,
  median_household_income_2023         STRING,
  poverty_universe_2023                STRING,
  poverty_below_2023                   STRING,
  race_white_alone_2023                STRING,
  race_black_alone_2023                STRING,
  race_asian_alone_2023                STRING,
  hispanic_or_latino_2023              STRING,
  edu_universe_25plus_2023             STRING,
  edu_bachelors_2023                   STRING,
  edu_masters_2023                     STRING,
  edu_professional_2023                STRING,
  edu_doctorate_2023                   STRING,
  housing_universe_2023                STRING,
  housing_owner_occupied_2023          STRING,
  median_gross_rent_2023               STRING,
  median_household_size_2023           STRING,
  lang_universe_2023                   STRING,
  lang_lim_eng_spanish_2023            STRING,
  lang_lim_eng_other_indo_european_2023 STRING,
  lang_lim_eng_asian_pacific_2023      STRING,
  lang_lim_eng_other_2023              STRING
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/census_tract_demographics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ============================================================================
-- Sanity checks
-- ============================================================================

-- ~2,300 tracts.
SELECT COUNT(*) FROM <YOUR_DB>.census_tract_demographics;

-- Quick look at NYC median income range.
SELECT
  MIN(TRY_CAST(median_household_income_2023 AS BIGINT)) AS min_income,
  MAX(TRY_CAST(median_household_income_2023 AS BIGINT)) AS max_income,
  APPROX_PERCENTILE(TRY_CAST(median_household_income_2023 AS BIGINT), 0.5) AS median_income
FROM <YOUR_DB>.census_tract_demographics;

-- Top 10 tracts by income change 2018 → 2023.
SELECT
  d.geoid,
  t.ntaname,
  t.boroname,
  TRY_CAST(d.median_household_income_2018 AS BIGINT) AS inc_2018,
  TRY_CAST(d.median_household_income_2023 AS BIGINT) AS inc_2023,
  TRY_CAST(d.median_household_income_2023 AS BIGINT)
    - TRY_CAST(d.median_household_income_2018 AS BIGINT) AS delta
FROM <YOUR_DB>.census_tract_demographics d
JOIN <YOUR_DB>.census_tracts t USING (geoid)
WHERE TRY_CAST(d.median_household_income_2018 AS BIGINT) IS NOT NULL
  AND TRY_CAST(d.median_household_income_2023 AS BIGINT) IS NOT NULL
ORDER BY delta DESC
LIMIT 10;

-- Crashes per capita by tract (2024 crashes vs 2023 ACS population).
WITH crash_pts AS (
  SELECT ST_POINT(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)) AS pt
  FROM <YOUR_DB>.nypd_collisions
  WHERE year = '2024'
    AND TRY_CAST(latitude AS DOUBLE) BETWEEN 40.4 AND 41.0
    AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
),
crash_by_tract AS (
  SELECT t.geoid, COUNT(*) AS crashes
  FROM crash_pts c
  JOIN <YOUR_DB>.census_tracts t
    ON ST_CONTAINS(ST_GEOMETRY_FROM_TEXT(t.geometry_wkt), c.pt)
  GROUP BY t.geoid
)
SELECT
  c.geoid,
  t.ntaname,
  c.crashes,
  TRY_CAST(d.total_pop_2023 AS BIGINT) AS pop,
  1000.0 * c.crashes / NULLIF(TRY_CAST(d.total_pop_2023 AS BIGINT), 0) AS crashes_per_1k
FROM crash_by_tract c
JOIN <YOUR_DB>.census_tract_demographics d USING (geoid)
JOIN <YOUR_DB>.census_tracts t USING (geoid)
WHERE TRY_CAST(d.total_pop_2023 AS BIGINT) > 100  -- exclude near-empty tracts
ORDER BY crashes_per_1k DESC
LIMIT 20;
