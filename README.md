# NYC Open Data → Athena Loader

Pulls NYC 311 (full history) and NYPD Motor Vehicle Collisions from the
Socrata API, writes Hive-partitioned Parquet to S3, registers as Athena tables.

## Setup

```bash
cd nyc-data-loader
python -m venv .venv && source .venv/bin/activate
pip install sodapy pyarrow boto3
```

Environment:

```bash
export SOCRATA_APP_TOKEN="..."        # https://data.cityofnewyork.us → Sign in → Developer Settings
export NYC_DATA_BUCKET="your-bucket"  # same bucket as TLC data
export NYC_DATA_PREFIX=""             # optional, e.g. "data/raw"
export AWS_PROFILE="your-profile"     # or rely on default credentials
```

## Before running: verify the 311_current resource ID

The loader has `erm2-nwe9` set for both the legacy and current 311 datasets,
but NYC has historically split 311 into separate "2010-2019 archive" and
"2020-present" datasets with different 4x4 IDs. **Verify** the current
endpoint at:

  https://data.cityofnewyork.us → search "311 Service Requests from 2020 to Present"

Look at the URL path — the trailing 4-char-dash-4-char string (e.g. `erm2-nwe9`)
is the resource ID. If the legacy and current datasets have the same ID, NYC
has consolidated them — just run `311_legacy` and skip `311_current`. If they
differ, update the `resource_id` in `DATASETS["311_current"]`.

## Run

Dry run first (no S3 writes, just confirms the API works):

```bash
python load_nyc_data.py --dataset crashes --dry-run
```

Then real loads. Crashes is the smallest — start there:

```bash
python load_nyc_data.py --dataset crashes        # ~30 min
python load_nyc_data.py --dataset 311_legacy     # ~2-3 hours
python load_nyc_data.py --dataset 311_current    # ~3-4 hours
```

The loader is **resumable**. State lives in `./checkpoints/<dataset>.json`.
If your laptop sleeps or the script crashes, just re-run the same command.

## Register tables in Athena

After at least one dataset finishes:

1. Open `athena_ddl.sql`
2. Replace `<YOUR_DB>`, `<YOUR_BUCKET>`, `<PREFIX>` with your values
3. Paste into the Athena console one statement at a time
4. Run `MSCK REPAIR TABLE` after each subsequent loader completion to pick
   up new partitions

## Schema notes

All columns are STRING. Cast at query time:

```sql
SELECT *
FROM nypd_collisions
WHERE TRY_CAST(number_of_persons_killed AS INTEGER) >= 1
  AND year = '2024'
```

This matches the convention used for the `par` table and avoids load-time
type drift. Athena's columnar Parquet scan only reads the columns referenced
by the query, so unused columns cost nothing.

## Known sharp edges

- **Socrata rate limits.** With an app token you're at ~1000 req/hour
  unauthenticated, much higher with the token. The loader retries with
  exponential backoff but if you see repeated 429s, slow down or wait.
- **Schema drift.** New columns added by NYC over the years show up
  automatically (loader unions keys across rows). Older partitions return
  NULL for newer columns — this is expected.
- **The `location` field.** Socrata returns it as a nested object with
  `latitude`, `longitude`, `human_address`. The loader hoists lat/lon to
  flat columns (`location_latitude`, `location_longitude`) and drops the
  human address. The top-level `latitude`/`longitude` columns (separate from
  the nested `location` object) are also preserved.
- **`taxi_zones` geometry column.** The DDL's sanity-check query references
  `z.geometry` — adjust to match your existing taxi_zones table's column
  name (might be `the_geom`, `wkt`, etc.).

## Cost estimate

- S3 storage: ~5GB total at $0.023/GB/month ≈ **$0.12/month**
- Athena scans: depends on usage. Partition pruning by `year`/`month` keeps
  most queries under 100MB ≈ <$0.001 each.
- Socrata API: free with token.
