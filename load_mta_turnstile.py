"""
MTA Subway Hourly Ridership → S3 Parquet loader for Athena.

Pulls MTA subway hourly ridership from the NY State Open Data Socrata API
(data.ny.gov) and writes month-partitioned, typed Parquet to S3. Full history
spans two resources, both loaded into the same logical `mta_turnstile` dataset:
    - wujg-7c2s : 2020-2024
    - 5wq4-mkjj : 2025-present

Resumable at offset granularity: each 50k page is written as its own
``part-N.parquet`` and the checkpoint records the next offset within the
in-progress month. Re-running picks up from the last successful batch.

Usage:
    Put ``NYC_*`` vars (and optionally ``SOCRATA_APP_TOKEN``) in a ``.env`` file
    next to this script, or export them.
    export AWS_PROFILE="your_profile"  # or use default credentials
    python load_mta_turnstile.py --source all --dry-run
    python load_mta_turnstile.py --source all
    python load_mta_turnstile.py --source 2020_2024
    python load_mta_turnstile.py --source 2025_present

No Socrata app token is required (NY State Open Data), but one will be used if
``SOCRATA_APP_TOKEN`` is set to raise rate limits.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import boto3
import h3
import pyarrow as pa
import pyarrow.parquet as pq
from sodapy import Socrata

_PROJECT_ROOT = Path(__file__).resolve().parent


def _load_env_file(path: Path) -> None:
    """Load ``KEY=value`` lines into the environment if not already set (no extra deps)."""
    if not path.is_file():
        return
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ[key] = value


_load_env_file(_PROJECT_ROOT / ".env")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_BUCKET = os.environ.get("NYC_DATA_BUCKET", "gtp-nyc-data-bucket")
S3_PREFIX = os.environ.get("NYC_DATA_PREFIX", "")  # optional sub-prefix
LOCAL_STAGE = Path(os.environ.get("LOCAL_STAGE", "./stage"))
CHECKPOINT_DIR = Path("./checkpoints")

PAGE_SIZE = 50_000  # Socrata max
SOCRATA_DOMAIN = "data.ny.gov"  # NY State Open Data (not NYC)

DATASET_NAME = "mta_turnstile"          # S3 path + logical name
DATE_COLUMN = "transit_timestamp"        # partition + $where column
ORDER_COLUMN = "transit_timestamp"       # stable sort for pagination

# MTA subway hourly ridership is split across two resources by date range.
# Both write into the same `mta_turnstile` dataset (same S3 path + table).
SOURCES: dict[str, dict] = {
    "2020_2024": {"resource_id": "wujg-7c2s", "start_year": 2020, "end_year": 2024},
    "2025_present": {"resource_id": "5wq4-mkjj", "start_year": 2025, "end_year": 2027},
}

# H3 resolutions for the station point (lat/lon).
H3_RESOLUTIONS = (8, 9, 10)

# Numeric columns cast to float64 on load.
FLOAT_COLUMNS = ("ridership", "transfers", "latitude", "longitude")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("mta_loader.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("mta-loader")


# ---------------------------------------------------------------------------
# H3 helpers (mirrors load_nyc_data.py)
# ---------------------------------------------------------------------------

def _h3_cell(lat: float, lon: float, resolution: int) -> str:
    """h3 v4: latlng_to_cell; v3: geo_to_h3."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, resolution)
    return h3.geo_to_h3(lat, lon, resolution)


def _h3_for(lat: float | None, lon: float | None) -> dict[str, str | None]:
    """Return {h3_r8, h3_r9, h3_r10} for a point, or all-None when unavailable."""
    if (
        lat is None
        or lon is None
        or not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0)
    ):
        return {f"h3_r{res}": None for res in H3_RESOLUTIONS}
    try:
        return {f"h3_r{res}": _h3_cell(lat, lon, res) for res in H3_RESOLUTIONS}
    except Exception:
        return {f"h3_r{res}": None for res in H3_RESOLUTIONS}


# ---------------------------------------------------------------------------
# Checkpointing (offset granularity)
# ---------------------------------------------------------------------------

def checkpoint_path() -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{DATASET_NAME}.json"


def load_checkpoint() -> tuple[set[tuple[int, int]], dict | None]:
    """Return (completed months set, in_progress dict | None)."""
    p = checkpoint_path()
    if not p.exists():
        return set(), None
    raw = json.loads(p.read_text())
    completed = {(item["year"], item["month"]) for item in raw.get("completed", [])}
    return completed, raw.get("in_progress")


def save_checkpoint(completed: set[tuple[int, int]], in_progress: dict | None) -> None:
    p = checkpoint_path()
    payload = {
        "completed": [{"year": y, "month": m} for (y, m) in sorted(completed)],
        "in_progress": in_progress,
        "updated_at": datetime.utcnow().isoformat(),
    }
    p.write_text(json.dumps(payload, indent=2))


# ---------------------------------------------------------------------------
# Pulling data
# ---------------------------------------------------------------------------

def month_ranges(start_year: int, end_year: int) -> Iterator[tuple[int, int, str, str]]:
    """Yield (year, month, where_start, where_end) tuples for each month."""
    today = date.today()
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            if y == today.year and m > today.month:
                return
            start_str = f"{y:04d}-{m:02d}-01T00:00:00.000"
            # Socrata $where uses inclusive lower, exclusive upper bound.
            if m == 12:
                end_str = f"{y + 1:04d}-01-01T00:00:00.000"
            else:
                end_str = f"{y:04d}-{m + 1:02d}-01T00:00:00.000"
            yield y, m, start_str, end_str


def fetch_page(
    client: Socrata,
    resource_id: str,
    where_start: str,
    where_end: str,
    offset: int,
) -> list[dict]:
    """Fetch a single page with 5-attempt exponential backoff."""
    where = f"{DATE_COLUMN} >= '{where_start}' AND {DATE_COLUMN} < '{where_end}'"
    for attempt in range(5):
        try:
            return client.get(
                resource_id,
                where=where,
                order=ORDER_COLUMN,
                limit=PAGE_SIZE,
                offset=offset,
            )
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"  fetch error ({e}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed after 5 retries at offset {offset}")


# ---------------------------------------------------------------------------
# Typed Parquet
# ---------------------------------------------------------------------------

def _to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_timestamp(v) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Socrata floating timestamps look like "2020-02-01T00:00:00.000".
        if s.endswith("Z"):
            s = s[:-1]
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    if isinstance(v, datetime):
        return v
    return None


def rows_to_typed_table(rows: list[dict]) -> pa.Table:
    """
    Convert list-of-dicts to a typed Arrow table.

    Unlike the all-strings 311/crashes loader, MTA columns get proper types:
    transit_timestamp -> timestamp, ridership/transfers/latitude/longitude ->
    float64, derived h3_r8/9/10 -> string, everything else -> string. Unknown
    columns default to string so schema drift is tolerated. Nested objects
    (e.g. georeference) are serialized to JSON strings.
    """
    if not rows:
        return pa.table({})

    timestamps: list[datetime | None] = []
    floats: dict[str, list[float | None]] = {c: [] for c in FLOAT_COLUMNS}
    h3_cols: dict[str, list[str | None]] = {f"h3_r{res}": [] for res in H3_RESOLUTIONS}
    string_cols: dict[str, list[str | None]] = {}

    for r in rows:
        timestamps.append(_to_timestamp(r.get(DATE_COLUMN)))

        for c in FLOAT_COLUMNS:
            floats[c].append(_to_float(r.get(c)))

        lat = _to_float(r.get("latitude"))
        lon = _to_float(r.get("longitude"))
        for k, v in _h3_for(lat, lon).items():
            h3_cols[k].append(v)

        # Remaining keys -> string (or JSON for nested objects/lists).
        for k, v in r.items():
            if k == DATE_COLUMN or k in FLOAT_COLUMNS:
                continue
            if k not in string_cols:
                string_cols[k] = [None] * len(timestamps[:-1])
            if isinstance(v, (dict, list)):
                string_cols[k].append(json.dumps(v))
            else:
                string_cols[k].append(str(v) if v is not None else None)

        # Backfill any string column not present in this row.
        for k, col in string_cols.items():
            if len(col) < len(timestamps):
                col.append(None)

    arrays: dict[str, pa.Array] = {
        DATE_COLUMN: pa.array(timestamps, type=pa.timestamp("us")),
    }
    for c in FLOAT_COLUMNS:
        arrays[c] = pa.array(floats[c], type=pa.float64())
    for k in h3_cols:
        arrays[k] = pa.array(h3_cols[k], type=pa.string())
    for k in sorted(string_cols):
        arrays[k] = pa.array(string_cols[k], type=pa.string())

    return pa.table(arrays)


def write_batch(table: pa.Table, year: int, month: int, part: int) -> Path:
    """Write a single 50k page as part-N.parquet locally; return path."""
    out_dir = LOCAL_STAGE / DATASET_NAME / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"part-{part}.parquet"
    pq.write_table(table, out_path, compression="snappy")
    return out_path


def upload_batch(local_path: Path, year: int, month: int, part: int, s3) -> str:
    """Upload a part file to S3 in Hive-style partitioned layout."""
    prefix = f"{S3_PREFIX.rstrip('/')+'/' if S3_PREFIX else ''}{DATASET_NAME}/year={year}/month={month:02d}"
    key = f"{prefix}/part-{part}.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, key)
    return f"s3://{S3_BUCKET}/{key}"


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def dry_run(client: Socrata) -> None:
    """Validate API connectivity and print the first batch schema; no S3 writes."""
    first_key = next(iter(SOURCES))
    src = SOURCES[first_key]
    log.info(f"[dry-run] probing {src['resource_id']} on {SOCRATA_DOMAIN}")
    rows = client.get(src["resource_id"], order=ORDER_COLUMN, limit=5)
    if not rows:
        log.warning("[dry-run] no rows returned")
        return
    table = rows_to_typed_table(rows)
    log.info(f"[dry-run] fetched {len(rows)} rows; resolved schema:")
    for field in table.schema:
        log.info(f"    {field.name}: {field.type}")
    sample_ts = _to_timestamp(rows[0].get(DATE_COLUMN))
    if sample_ts is not None:
        log.info(
            f"[dry-run] first partition key: year={sample_ts.year} "
            f"month={sample_ts.month:02d}"
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def load_source(
    source_key: str,
    client: Socrata,
    s3,
    completed: set[tuple[int, int]],
    in_progress: dict | None,
) -> int:
    """Load one resource into the shared mta_turnstile dataset. Returns rows written."""
    src = SOURCES[source_key]
    resource_id = src["resource_id"]
    log.info(f"[{source_key}] resource {resource_id} ({src['start_year']}-{src['end_year']})")

    total_rows = 0
    for year, month, ws, we in month_ranges(src["start_year"], src["end_year"]):
        if (year, month) in completed:
            continue

        # Resume mid-month from the saved offset, if this is the in-progress month.
        start_offset = 0
        if in_progress and in_progress.get("year") == year and in_progress.get("month") == month:
            start_offset = int(in_progress.get("next_offset", 0))
            log.info(f"[{source_key}] {year}-{month:02d} resuming at offset {start_offset}")

        log.info(f"[{source_key}] {year}-{month:02d} pulling…")
        offset = start_offset
        month_rows = 0
        empty_month = True

        while True:
            t0 = time.time()
            rows = fetch_page(client, resource_id, ws, we, offset)
            if not rows:
                break

            empty_month = False
            part = offset // PAGE_SIZE
            table = rows_to_typed_table(rows)
            log.info(
                f"  {year}-{month:02d} part-{part}: {len(rows):,} rows "
                f"fetched in {time.time()-t0:.1f}s"
            )

            if s3 is not None:
                local = write_batch(table, year, month, part)
                uri = upload_batch(local, year, month, part, s3)
                log.info(f"  uploaded {uri}")
                local.unlink()  # free disk

            month_rows += len(rows)
            total_rows += len(rows)

            # Checkpoint after each successful batch (offset granularity).
            in_progress = {"year": year, "month": month, "next_offset": offset + PAGE_SIZE}
            save_checkpoint(completed, in_progress)

            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if empty_month:
            log.info(f"  {year}-{month:02d} empty, skipping")
        else:
            log.info(f"  {year}-{month:02d} DONE: {month_rows:,} rows")

        completed.add((year, month))
        in_progress = None
        save_checkpoint(completed, in_progress)

    log.info(f"[{source_key}] DONE. Rows this source: {total_rows:,}")
    return total_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--source",
        default="all",
        choices=list(SOURCES.keys()) + ["all"],
        help="which resource(s) to load into the mta_turnstile dataset",
    )
    ap.add_argument("--dry-run", action="store_true", help="probe API + print schema; no S3 writes")
    args = ap.parse_args()

    token = os.environ.get("SOCRATA_APP_TOKEN")
    if not token and not args.dry_run:
        log.warning("No SOCRATA_APP_TOKEN set — using anonymous (slower, throttled).")

    client = Socrata(SOCRATA_DOMAIN, token, timeout=120)

    if args.dry_run:
        dry_run(client)
        client.close()
        return

    if S3_BUCKET == "REPLACE_ME":
        sys.exit("Set NYC_DATA_BUCKET env var to your S3 bucket.")

    s3 = boto3.client("s3")
    completed, in_progress = load_checkpoint()
    log.info(f"[{DATASET_NAME}] {len(completed)} months already completed; resuming.")

    keys = list(SOURCES.keys()) if args.source == "all" else [args.source]
    grand_total = 0
    for key in keys:
        grand_total += load_source(key, client, s3, completed, in_progress)
        in_progress = None  # only the first processed month can be mid-flight

    client.close()
    log.info(f"[{DATASET_NAME}] ALL DONE. Total rows this run: {grand_total:,}")


if __name__ == "__main__":
    main()
