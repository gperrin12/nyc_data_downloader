"""
NYC Open Data → S3 Parquet loader for Athena.

Pulls 311 Service Requests (2010-2019 + 2020-present) and NYPD Motor Vehicle
Collisions from the Socrata API, writes month-partitioned Parquet to S3.

Resumable: writes a checkpoint after each successful month. Re-running picks
up from the last completed month.

Usage:
    Put ``SOCRATA_APP_TOKEN`` (and optionally ``NYC_*`` vars) in a ``.env`` file
    next to this script, or export them.
    export AWS_PROFILE="your_profile"  # or use default credentials
    python load_nyc_data.py --dataset crashes
    python load_nyc_data.py --dataset 311_current
    python load_nyc_data.py --dataset 311_legacy

Datasets configured below. Add more by extending DATASETS.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import boto3
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
SOCRATA_DOMAIN = "data.cityofnewyork.us"


@dataclass
class DatasetConfig:
    """Configuration for one Socrata dataset."""

    name: str                 # logical name, used for S3 path & table name
    resource_id: str          # Socrata 4x4 ID
    date_column: str          # column to partition by (year/month)
    start_year: int           # earliest year to attempt
    end_year: int             # latest year (inclusive); use current year + 1 to be safe
    order_column: str         # stable sort column for pagination


DATASETS: dict[str, DatasetConfig] = {
    # 311: pre-2020 archive. ~24M rows.
    "311_legacy": DatasetConfig(
        name="nyc_311",
        resource_id="erm2-nwe9",
        date_column="created_date",
        start_year=2010,
        end_year=2019,
        order_column="unique_key",
    ),
    # 311: current. ~40M+ rows, refreshed daily.
    "311_current": DatasetConfig(
        name="nyc_311",
        resource_id="erm2-nwe9",  # NOTE: verify the current dataset's 4x4 — see README
        date_column="created_date",
        start_year=2020,
        end_year=2027,
        order_column="unique_key",
    ),
    # NYPD MV Collisions. ~2M rows.
    "crashes": DatasetConfig(
        name="nypd_collisions",
        resource_id="h9gi-nx95",
        date_column="crash_date",
        start_year=2012,
        end_year=2027,
        order_column="collision_id",
    ),
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("loader.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("nyc-loader")


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def checkpoint_path(dataset_key: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{dataset_key}.json"


def load_checkpoint(dataset_key: str) -> set[tuple[int, int]]:
    """Return the set of (year, month) tuples already completed."""
    p = checkpoint_path(dataset_key)
    if not p.exists():
        return set()
    raw = json.loads(p.read_text())
    return {(item["year"], item["month"]) for item in raw.get("completed", [])}


def save_checkpoint(dataset_key: str, completed: set[tuple[int, int]]) -> None:
    p = checkpoint_path(dataset_key)
    payload = {
        "completed": [
            {"year": y, "month": m} for (y, m) in sorted(completed)
        ],
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


def fetch_month(
    client: Socrata,
    cfg: DatasetConfig,
    where_start: str,
    where_end: str,
) -> Iterator[list[dict]]:
    """
    Yield pages of rows for a single month.

    Uses date-bounded queries with offset pagination. Within a month, rows fit
    comfortably under Socrata's 1000-page hard limit even for 311 (~500k/month
    peak ÷ 50k page size = 10 pages).
    """
    where = f"{cfg.date_column} >= '{where_start}' AND {cfg.date_column} < '{where_end}'"
    offset = 0
    while True:
        for attempt in range(5):
            try:
                rows = client.get(
                    cfg.resource_id,
                    where=where,
                    order=cfg.order_column,
                    limit=PAGE_SIZE,
                    offset=offset,
                )
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning(f"  fetch error ({e}); retrying in {wait}s")
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed after 5 retries at offset {offset}")

        if not rows:
            return
        yield rows
        if len(rows) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


# ---------------------------------------------------------------------------
# Writing Parquet
# ---------------------------------------------------------------------------

def rows_to_table(rows: list[dict]) -> pa.Table:
    """
    Convert list-of-dicts to Arrow table, treating everything as string.

    Socrata returns JSON, so types are loose. We cast everything to string at
    load time — Athena can CAST() at query time. This avoids load-time schema
    drift surprises (a column that was numeric for 5 years suddenly returning
    a string) and keeps the loader dataset-agnostic.

    Trade-off: queries pay a small CAST cost. Acceptable for an eval setup.
    """
    if not rows:
        return pa.table({})

    # Flatten any nested location dicts (Socrata returns these for point fields).
    flat = []
    for r in rows:
        out = {}
        for k, v in r.items():
            if isinstance(v, dict):
                # Socrata location fields: {"latitude": "...", "longitude": "...", "human_address": "..."}
                # Hoist lat/lon to top-level columns; drop the rest.
                if "latitude" in v:
                    out[f"{k}_latitude"] = str(v["latitude"]) if v.get("latitude") is not None else None
                if "longitude" in v:
                    out[f"{k}_longitude"] = str(v["longitude"]) if v.get("longitude") is not None else None
            elif isinstance(v, list):
                out[k] = json.dumps(v)
            else:
                out[k] = str(v) if v is not None else None
        flat.append(out)

    # Union of keys across rows; missing keys become None.
    all_keys = set()
    for r in flat:
        all_keys.update(r.keys())
    cols = {k: [r.get(k) for r in flat] for k in sorted(all_keys)}
    return pa.table(cols)


def write_partition(table: pa.Table, dataset_name: str, year: int, month: int) -> Path:
    """Write a single year/month Parquet file locally; return path."""
    out_dir = LOCAL_STAGE / dataset_name / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0.parquet"
    pq.write_table(table, out_path, compression="snappy")
    return out_path


def upload_partition(local_path: Path, dataset_name: str, year: int, month: int, s3) -> str:
    """Upload local Parquet to S3 in Hive-style partitioned layout."""
    prefix = f"{S3_PREFIX.rstrip('/')+'/' if S3_PREFIX else ''}{dataset_name}/year={year}/month={month:02d}"
    key = f"{prefix}/part-0.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, key)
    return f"s3://{S3_BUCKET}/{key}"


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def load_dataset(dataset_key: str, dry_run: bool = False) -> None:
    cfg = DATASETS[dataset_key]
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if not token and not dry_run:
        log.warning("No SOCRATA_APP_TOKEN set — using anonymous (slower, throttled).")

    client = Socrata(SOCRATA_DOMAIN, token, timeout=120)
    s3 = boto3.client("s3") if not dry_run else None

    completed = load_checkpoint(dataset_key)
    log.info(f"[{dataset_key}] {len(completed)} months already completed; resuming.")

    total_rows = 0
    for year, month, ws, we in month_ranges(cfg.start_year, cfg.end_year):
        if (year, month) in completed:
            continue

        log.info(f"[{dataset_key}] {year}-{month:02d} pulling…")
        t0 = time.time()
        all_rows: list[dict] = []
        for page in fetch_month(client, cfg, ws, we):
            all_rows.extend(page)

        if not all_rows:
            log.info(f"  empty month, skipping")
            completed.add((year, month))
            save_checkpoint(dataset_key, completed)
            continue

        table = rows_to_table(all_rows)
        log.info(f"  {len(all_rows):,} rows fetched in {time.time()-t0:.1f}s")

        if dry_run:
            log.info(f"  [dry-run] would write {table.num_rows} rows × {table.num_columns} cols")
        else:
            local = write_partition(table, cfg.name, year, month)
            uri = upload_partition(local, cfg.name, year, month, s3)
            log.info(f"  uploaded {uri}")
            local.unlink()  # free disk

        total_rows += len(all_rows)
        completed.add((year, month))
        save_checkpoint(dataset_key, completed)

    client.close()
    log.info(f"[{dataset_key}] DONE. Total rows this run: {total_rows:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=list(DATASETS.keys()))
    ap.add_argument("--dry-run", action="store_true", help="fetch but don't write/upload")
    args = ap.parse_args()

    if S3_BUCKET == "REPLACE_ME" and not args.dry_run:
        sys.exit("Set NYC_DATA_BUCKET env var to your S3 bucket.")

    load_dataset(args.dataset, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
