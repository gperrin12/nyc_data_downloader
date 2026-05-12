-- ============================================================================
-- Athena DDL for taxi_zones and census_tracts.
--
-- Run AFTER load_geographies.py uploads to S3.
-- Replace <YOUR_DB>, <YOUR_BUCKET>, <PREFIX> with your values.
--
-- Geometry pattern: stored as WKT string. Wrap with ST_GEOMETRY_FROM_TEXT()
-- at query time. Reprojected to WGS84 by the loader.
-- ============================================================================


-- ---- taxi_zones ------------------------------------------------------------
-- 263 zones, source: TLC official shapefile.
-- Note: the original column is `LocationID` in the shapefile. Loader
-- lowercases all columns, so it becomes `locationid` here.

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.taxi_zones (
  objectid       STRING,
  shape_leng     STRING,
  the_geom       STRING,    -- not used; placeholder if shapefile included it
  shape_area     STRING,
  zone           STRING,    -- e.g. "Crown Heights North"
  locationid     STRING,    -- joins to TLC pulocationid / dolocationid
  borough        STRING,    -- "Brooklyn", "Manhattan", etc.
  geometry_wkt   STRING     -- WKT polygon, WGS84
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/taxi_zones/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ---- census_tracts ---------------------------------------------------------
-- ~2,300 NYC tracts (2020 Census), NYC DCP shoreline-clipped version.
-- Schema below reflects the typical NYC DCP fields. If load_geographies.py
-- logs different columns, adjust this DDL accordingly.

CREATE EXTERNAL TABLE IF NOT EXISTS <YOUR_DB>.census_tracts (
  boroct2020     STRING,    -- borough + tract code, NYC standard ID
  ct2020         STRING,    -- tract code within borough
  boroname       STRING,    -- borough name
  borocode       STRING,    -- 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=SI
  ctlabel        STRING,    -- human-readable tract label
  nta2020        STRING,    -- NTA code (joins to NTA reference if loaded)
  ntaname        STRING,    -- NTA name e.g. "Crown Heights (North)"
  cdta2020       STRING,    -- Community District Tabulation Area
  cdtaname       STRING,
  geoid          STRING,    -- 11-digit federal census tract ID
  shape_leng     STRING,
  shape_area     STRING,
  geometry_wkt   STRING     -- WKT polygon, WGS84
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/<PREFIX>/census_tracts/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ============================================================================
-- Sanity-check queries
-- ============================================================================

-- Should return 263.
SELECT COUNT(*) FROM <YOUR_DB>.taxi_zones;

-- Should return ~2,300.
SELECT COUNT(*) FROM <YOUR_DB>.census_tracts;

-- Confirm WKT parses and has reasonable area (sq degrees, will be tiny).
SELECT
  zone,
  borough,
  ST_AREA(ST_GEOMETRY_FROM_TEXT(geometry_wkt)) AS area_sqdeg
FROM <YOUR_DB>.taxi_zones
ORDER BY area_sqdeg DESC
LIMIT 5;

-- Spatial join example: 2024 crashes per census tract.
-- This is the core eval-relevant pattern: point lat/lon → polygon containment.
WITH crash_pts AS (
  SELECT
    collision_id,
    ST_POINT(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)) AS pt
  FROM <YOUR_DB>.nypd_collisions
  WHERE year = '2024'
    AND latitude  IS NOT NULL
    AND longitude IS NOT NULL
    AND TRY_CAST(latitude  AS DOUBLE) BETWEEN 40.4 AND 41.0
    AND TRY_CAST(longitude AS DOUBLE) BETWEEN -74.3 AND -73.6
)
SELECT
  t.boroname,
  t.ntaname,
  t.geoid,
  COUNT(*) AS crashes
FROM crash_pts c
JOIN <YOUR_DB>.census_tracts t
  ON ST_CONTAINS(ST_GEOMETRY_FROM_TEXT(t.geometry_wkt), c.pt)
GROUP BY 1, 2, 3
ORDER BY crashes DESC
LIMIT 20;

-- Zone-to-tract crosswalk (run once, materialize as a table if you'll use it
-- a lot — saves the spatial join cost per query).
-- Uses tract centroid → zone polygon; coarsest mapping but cheapest.
CREATE TABLE IF NOT EXISTS <YOUR_DB>.zone_tract_xwalk
WITH (format = 'PARQUET', external_location = 's3://<YOUR_BUCKET>/<PREFIX>/zone_tract_xwalk/') AS
SELECT
  z.locationid AS zone_id,
  z.zone,
  z.borough,
  t.geoid     AS tract_geoid,
  t.ntaname
FROM <YOUR_DB>.census_tracts t
JOIN <YOUR_DB>.taxi_zones z
  ON ST_CONTAINS(
       ST_GEOMETRY_FROM_TEXT(z.geometry_wkt),
       ST_CENTROID(ST_GEOMETRY_FROM_TEXT(t.geometry_wkt))
     );
