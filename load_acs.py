"""
ACS demographics loader → Athena.

Pulls a curated set of ~15 variables from the Census ACS 5-year API for NYC
census tracts, for two vintages (2018 and 2023 5-year releases), denormalizes
into one wide row per tract, writes Parquet to S3.

NYC = state 36, counties 005 (Bronx), 047 (Kings/Brooklyn), 061 (New York/
Manhattan), 081 (Queens), 085 (Richmond/Staten Island).

Schema joins to census_tracts on geoid.

Usage:
    Put ``CENSUS_API_KEY`` in a ``.env`` file next to this script (or export it).
    export NYC_DATA_BUCKET="..."
    export NYC_DATA_PREFIX=""
    python load_acs.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

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
S3_PREFIX = os.environ.get("NYC_DATA_PREFIX", "")
LOCAL_STAGE = Path("./stage_acs")

NY_STATE = "36"
NYC_COUNTIES = ["005", "047", "061", "081", "085"]

# Two non-overlapping 5-year ACS releases.
VINTAGES = [
    {"year": 2018, "endyear": 2018},  # 2014-2018 5yr
    {"year": 2023, "endyear": 2023},  # 2019-2023 5yr
]

# Curated variables. (raw_code, output_name).
# We pull raw counts/medians and let consumers compute rates in SQL.
VARIABLES: list[tuple[str, str]] = [
    # Core counts
    ("B01003_001E", "total_pop"),
    ("B01002_001E", "median_age"),

    # Income & poverty (raw counts; rates computed at query time)
    ("B19013_001E", "median_household_income"),
    ("B17001_001E", "poverty_universe"),
    ("B17001_002E", "poverty_below"),

    # Race (counts)
    ("B02001_002E", "race_white_alone"),
    ("B02001_003E", "race_black_alone"),
    ("B02001_005E", "race_asian_alone"),
    ("B03003_003E", "hispanic_or_latino"),

    # Educational attainment (25+ population)
    # B15003 has 25 categories; we'll pull the universe and the "bachelor's+"
    # components. Bachelor's+ = sum of codes 022, 023, 024, 025.
    ("B15003_001E", "edu_universe_25plus"),
    ("B15003_022E", "edu_bachelors"),
    ("B15003_023E", "edu_masters"),
    ("B15003_024E", "edu_professional"),
    ("B15003_025E", "edu_doctorate"),

    # Housing
    ("B25003_001E", "housing_universe"),
    ("B25003_002E", "housing_owner_occupied"),
    ("B25064_001E", "median_gross_rent"),
    ("B25010_001E", "median_household_size"),

    # Limited English (C16002 — collapsed table, stable across vintages)
    ("C16002_001E", "lang_universe"),
    ("C16002_004E", "lang_lim_eng_spanish"),
    ("C16002_007E", "lang_lim_eng_other_indo_european"),
    ("C16002_010E", "lang_lim_eng_asian_pacific_island"),
    ("C16002_013E", "lang_lim_eng_other"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("acs-loader")


# ---------------------------------------------------------------------------
# Census API
# ---------------------------------------------------------------------------

def fetch_vintage(year: int, api_key: str) -> pd.DataFrame:
    """
    Pull all NYC tracts for one ACS 5-year vintage.

    The ACS 5yr API is at /data/{endyear}/acs/acs5. Tract-level pulls require
    state and county filters; we union across the 5 NYC counties.

    Returns a DataFrame with columns: geoid + all VARIABLES (raw_code names).
    """
    base = f"https://api.census.gov/data/{year}/acs/acs5"
    var_codes = [v[0] for v in VARIABLES]
    # API requires GEO_ID for proper geoid construction; "NAME" for sanity.
    get_clause = ",".join(["NAME"] + var_codes)

    frames = []
    for county in NYC_COUNTIES:
        params = {
            "get": get_clause,
            "for": "tract:*",
            "in": f"state:{NY_STATE} county:{county}",
            "key": api_key,
        }
        log.info(f"  fetching {year} county {county}…")
        r = requests.get(base, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        header, rows = data[0], data[1:]
        df = pd.DataFrame(rows, columns=header)
        # Construct 11-digit GEOID from state + county + tract.
        df["geoid"] = df["state"] + df["county"] + df["tract"]
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)

    # Keep only geoid + variable columns; rename to friendly names.
    rename = {raw: friendly for raw, friendly in VARIABLES}
    out = out[["geoid"] + [v[0] for v in VARIABLES]].rename(columns=rename)

    # Convert numeric columns. Census uses negative sentinels (-666666666 etc.)
    # for "not available" — coerce those to NaN.
    for _, friendly in VARIABLES:
        out[friendly] = pd.to_numeric(out[friendly], errors="coerce")
        out.loc[out[friendly] < -100_000_000, friendly] = pd.NA

    log.info(f"  {year}: {len(out):,} tracts")
    return out


# ---------------------------------------------------------------------------
# Combine vintages, write
# ---------------------------------------------------------------------------

def build_wide_table(api_key: str) -> pd.DataFrame:
    """Pull both vintages, merge on geoid, suffix columns by vintage year."""
    frames = []
    for v in VINTAGES:
        df = fetch_vintage(v["year"], api_key)
        # Suffix every column except geoid with the vintage year.
        suffix = f"_{v['year']}"
        df = df.rename(columns={
            c: f"{c}{suffix}" for c in df.columns if c != "geoid"
        })
        frames.append(df)

    # Outer-join on geoid so tracts present in only one vintage still show up.
    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on="geoid", how="outer")

    log.info(f"Final wide table: {len(merged):,} rows × {len(merged.columns)} cols")
    return merged


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Cast everything to string for consistency with our other tables.
    # Demographics queries will TRY_CAST in Athena.
    str_df = df.astype("object").where(df.notna(), None).astype(str)
    # Restore None for nulls (astype(str) turns None into 'None').
    str_df = str_df.where(str_df != "None", None)
    table = pa.Table.from_pandas(str_df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")
    log.info(f"  wrote {out_path}")


def upload(local_path: Path, dataset_name: str, s3) -> str:
    prefix = f"{S3_PREFIX.rstrip('/')+'/' if S3_PREFIX else ''}{dataset_name}"
    key = f"{prefix}/data.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, key)
    uri = f"s3://{S3_BUCKET}/{key}"
    log.info(f"  uploaded {uri}")
    return uri


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        sys.exit(
            "Set CENSUS_API_KEY in .env next to load_acs.py or in the environment "
            "(https://api.census.gov/data/key_signup.html)"
        )
    if S3_BUCKET == "REPLACE_ME":
        sys.exit("Set NYC_DATA_BUCKET")

    df = build_wide_table(api_key)

    out = LOCAL_STAGE / "census_tract_demographics" / "data.parquet"
    write_parquet(df, out)

    s3 = boto3.client("s3")
    upload(out, "census_tract_demographics", s3)
    log.info("Done.")


if __name__ == "__main__":
    main()
