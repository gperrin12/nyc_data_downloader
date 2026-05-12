"""
NYC geography loader: taxi zones (TLC) + census tracts (NYC DCP).

Downloads source shapefiles, reprojects to WGS84 (EPSG:4326) so geometries
work with Athena's ST_* functions, serializes geometry as WKT, writes
Parquet to S3.

These are small reference datasets (~2,500 rows total) — no partitioning,
no chunking, no checkpointing.

Usage:
    export NYC_DATA_PREFIX=""           # optional
    python load_geographies.py taxi_zones
    python load_geographies.py census_tracts
    python load_geographies.py all

After uploading, run the DDL in geographies_ddl.sql in Athena.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import zipfile
from pathlib import Path

import boto3
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# NYC DCP "Census Tracts for 2020 US Census" — clipped to shoreline,
# enriched with NTA/CDTA codes. Hosted on NYC Open Data as a Socrata asset.
# This URL pattern returns the shapefile zip directly.
CENSUS_TRACTS_URL = (
    "https://data.cityofnewyork.us/api/geospatial/63ge-mke6"
    "?method=export&format=Shapefile"
)


def _parse_s3_bucket_and_prefix(spec: str) -> tuple[str, str]:
    """
    boto3 expects the bucket name only; any path belongs in the object key.

    Accepts: ``bucket``, ``bucket/prefix/``, ``s3://bucket/prefix/``.
    """
    s = spec.strip()
    if not s:
        return "", ""
    if s.startswith("s3://"):
        s = s[5:]
    s = s.lstrip("/")
    if "/" not in s:
        return s, ""
    bucket, rest = s.split("/", 1)
    return bucket, rest.strip("/")


# Bucket name alone, or with base key prefix / full s3 URI (see _parse_s3_bucket_and_prefix).
NYC_DATA_BUCKET = "gtp-nyc-data-bucket"

S3_PREFIX = os.environ.get("NYC_DATA_PREFIX", "")
S3_BUCKET, S3_KEY_PREFIX = _parse_s3_bucket_and_prefix(NYC_DATA_BUCKET)
LOCAL_STAGE = Path("./stage_geo")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("geo-loader")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def download_zip(url: str, dest_dir: Path) -> Path:
    """Download a zipped shapefile, extract to dest_dir, return dir path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Downloading {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(dest_dir)
    log.info(f"  extracted to {dest_dir}")
    return dest_dir


def find_shapefile(directory: Path) -> Path:
    """Recursively find the .shp inside a directory tree."""
    candidates = list(directory.rglob("*.shp"))
    if not candidates:
        raise FileNotFoundError(f"No .shp under {directory}")
    if len(candidates) > 1:
        log.warning(f"Multiple .shp found, using first: {candidates}")
    return candidates[0]


def gdf_to_parquet(gdf: gpd.GeoDataFrame, out_path: Path) -> None:
    """
    Reproject to WGS84, serialize geometry to WKT, write Parquet.

    Athena ST_* functions expect WGS84 (lon/lat) coordinates. Source shapefiles
    are typically in NY State Plane (EPSG:2263) — we always reproject.
    """
    if gdf.crs is None:
        raise ValueError("Source has no CRS — refusing to guess.")
    if gdf.crs.to_epsg() != 4326:
        log.info(f"  reprojecting from EPSG:{gdf.crs.to_epsg()} → 4326")
        gdf = gdf.to_crs(epsg=4326)

    # Lowercase column names for Athena friendliness.
    gdf.columns = [c.lower() for c in gdf.columns]

    # Serialize geometry to WKT string. Drop the GeoSeries column.
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    df = gdf.drop(columns=["geometry"])

    # Cast everything else to string for consistency with our other tables.
    # (Numeric IDs etc. can be CAST at query time.)
    for col in df.columns:
        if col != "geometry_wkt":
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path, compression="snappy")
    log.info(f"  wrote {out_path} ({table.num_rows:,} rows × {table.num_columns} cols)")


def upload(local_path: Path, dataset_name: str, s3) -> str:
    """Upload to s3://bucket/<base>/<optional env prefix>/<dataset_name>/data.parquet"""
    segs: list[str] = []
    for p in (S3_KEY_PREFIX, S3_PREFIX, dataset_name):
        q = str(p).strip().strip("/")
        if q:
            segs.append(q)
    prefix = "/".join(segs)
    key = f"{prefix}/data.parquet"
    s3.upload_file(str(local_path), S3_BUCKET, key)
    uri = f"s3://{S3_BUCKET}/{key}"
    log.info(f"  uploaded {uri}")
    return uri


# ---------------------------------------------------------------------------
# Per-dataset loaders
# ---------------------------------------------------------------------------

def load_taxi_zones(s3) -> None:
    extract_dir = LOCAL_STAGE / "taxi_zones_src"
    download_zip(TAXI_ZONES_URL, extract_dir)
    shp = find_shapefile(extract_dir)
    gdf = gpd.read_file(shp)
    log.info(f"  taxi_zones loaded: {len(gdf)} rows, columns: {list(gdf.columns)}")

    out = LOCAL_STAGE / "taxi_zones" / "data.parquet"
    gdf_to_parquet(gdf, out)
    upload(out, "taxi_zones", s3)


def load_census_tracts(s3) -> None:
    extract_dir = LOCAL_STAGE / "census_tracts_src"
    download_zip(CENSUS_TRACTS_URL, extract_dir)
    shp = find_shapefile(extract_dir)
    gdf = gpd.read_file(shp)
    log.info(f"  census_tracts loaded: {len(gdf)} rows, columns: {list(gdf.columns)}")

    out = LOCAL_STAGE / "census_tracts" / "data.parquet"
    gdf_to_parquet(gdf, out)
    upload(out, "census_tracts", s3)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

LOADERS = {
    "taxi_zones": load_taxi_zones,
    "census_tracts": load_census_tracts,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", choices=list(LOADERS.keys()) + ["all"])
    args = ap.parse_args()

    if S3_BUCKET == "REPLACE_ME" or not S3_BUCKET:
        sys.exit("Set NYC_DATA_BUCKET in load_geographies.py (bucket or bucket/prefix/).")

    s3 = boto3.client("s3")
    targets = LOADERS.keys() if args.dataset == "all" else [args.dataset]
    for name in targets:
        log.info(f"=== {name} ===")
        LOADERS[name](s3)
    log.info("Done.")


if __name__ == "__main__":
    main()
