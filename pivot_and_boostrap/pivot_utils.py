"""
Core utilities for taxi trip pivoting: column detection, path helpers,
pivoting, cleanup, and S3/local file discovery.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional, Tuple, Union

import pandas as pd

# ---------------------------------------------------------------------------
# Part 1: Column detection and path helpers
# ---------------------------------------------------------------------------


def find_pickup_datetime_col(df_or_schema: Union[pd.DataFrame, Any]) -> Optional[str]:
    """
    Return the column name for pickup datetime, handling common variants (case-insensitive).
    Different NYC taxi files use different names: tpep_pickup_datetime (yellow),
    lpep_pickup_datetime (green), Trip_Pickup_DateTime (older yellow), request_datetime (FHV), etc.
    """
    if hasattr(df_or_schema, "column_names"):
        names = [c.lower() for c in df_or_schema.column_names]
        cols = df_or_schema.column_names
    elif hasattr(df_or_schema, "names"):
        cols = list(df_or_schema.names)
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "fields"):
        cols = [getattr(f, "name", str(f)) for f in df_or_schema.fields]
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "columns"):
        names = [str(c).lower() for c in df_or_schema.columns]
        cols = list(df_or_schema.columns)
    else:
        return None
    # Explicit candidates across yellow / green / FHV / older formats
    candidates = [
        "pickup_datetime",
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "trip_pickup_datetime",
        "pickup_datetime_utc",
        "pickup_date",
        "pickup_time",
        "request_datetime",
        "request_date",
    ]
    for c in candidates:
        if c in names:
            idx = names.index(c)
            return cols[idx]
    # Fuzzy: any column with pickup + date/time
    for i, n in enumerate(names):
        if "pickup" in n and ("datetime" in n or "date" in n or "time" in n):
            return cols[i]
    for i, n in enumerate(names):
        if "request" in n and ("datetime" in n or "date" in n or "time" in n):
            return cols[i]
    return None


def find_pickup_location_col(df_or_schema: Union[pd.DataFrame, Any]) -> Optional[str]:
    """
    Return the column name for pickup location (e.g. PULocationID, pickup_location_id).
    Case-insensitive. Names vary by file: PULocationID (TLC), pickup_zone_id, etc.
    """
    if hasattr(df_or_schema, "column_names"):
        names = [c.lower() for c in df_or_schema.column_names]
        cols = df_or_schema.column_names
    elif hasattr(df_or_schema, "names"):
        cols = list(df_or_schema.names)
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "fields"):
        cols = [getattr(f, "name", str(f)) for f in df_or_schema.fields]
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "columns"):
        names = [str(c).lower() for c in df_or_schema.columns]
        cols = list(df_or_schema.columns)
    else:
        return None
    candidates = [
        "pulocationid",
        "pu_location_id",
        "pickup_location_id",
        "pickup_location",
        "pickup_locationid",
        "pickup_zone_id",
        "pu_zone",
        "origin_location_id",
        "location_id",
    ]
    for c in candidates:
        if c in names:
            idx = names.index(c)
            return cols[idx]
    for i, n in enumerate(names):
        if "pickup" in n and ("location" in n or "zone" in n):
            return cols[i]
    for i, n in enumerate(names):
        if "pu_" in n and ("location" in n or "zone" in n):
            return cols[i]
    return None


def find_pickup_lat_lon_cols(df_or_schema: Union[pd.DataFrame, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (lat_col, lon_col) for pickup when no PULocationID; e.g. Start_Lat, Start_Lon.
    Case-insensitive. Names vary by file: Start_Lat/Lon (older yellow), pickup_latitude/longitude, etc.
    """
    if hasattr(df_or_schema, "column_names"):
        names = [c.lower() for c in df_or_schema.column_names]
        cols = df_or_schema.column_names
    elif hasattr(df_or_schema, "names"):
        cols = list(df_or_schema.names)
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "fields"):
        cols = [getattr(f, "name", str(f)) for f in df_or_schema.fields]
        names = [str(c).lower() for c in cols]
    elif hasattr(df_or_schema, "columns"):
        names = [str(c).lower() for c in df_or_schema.columns]
        cols = list(df_or_schema.columns)
    else:
        return None, None
    lat_col = None
    lon_col = None
    lat_candidates = ("start_lat", "start_latitude", "pickup_lat", "pickup_latitude", "origin_lat")
    lon_candidates = ("start_lon", "start_longitude", "pickup_lon", "pickup_longitude", "origin_lon")
    for c, n in zip(cols, names):
        if n in lat_candidates:
            lat_col = c
        if n in lon_candidates:
            lon_col = c
    if lat_col and lon_col:
        return lat_col, lon_col
    return None, None


def infer_taxi_type_from_path(file_path: str) -> str:
    """
    Infer taxi type from file path (e.g. yellow_tripdata_2023-01.parquet -> 'yellow').
    """
    path_lower = file_path.lower()
    name = Path(file_path).name if "/" in file_path or "\\" in file_path else file_path
    name_lower = name.lower()
    if "yellow" in name_lower or "yellow" in path_lower:
        return "yellow"
    if "green" in name_lower or "green" in path_lower:
        return "green"
    if "fhv" in name_lower or "fhv" in path_lower:
        return "fhv"
    return "unknown"


def infer_month_from_path(file_path: str) -> Optional[Tuple[int, int]]:
    """
    Infer (year, month) from file path. Supports patterns like:
    - ...2023-01..., yellow_tripdata_2023-01.parquet
    - year=2023/month=01
    Returns (year, month) or None if not inferrable.
    """
    path = file_path.replace("\\", "/")
    # Pattern: year=YYYY/month=MM
    m = re.search(r"year[=_]?(\d{4})[/_]month[=_]?(\d{1,2})", path, re.I)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    # Pattern: YYYY-MM or YYYY_MM in filename/path
    m = re.search(r"(\d{4})[-_](\d{1,2})(?:\.parquet|/|$)", path, re.I)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = re.search(r"(\d{4})[-_](\d{1,2})", path)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def pivot_counts_date_taxi_type_location(
    pdf: pd.DataFrame,
) -> pd.DataFrame:
    """
    Pivot aggregated counts: one row per (taxi_type, date, pickup_place) with
    columns hour_0..hour_23. Input df must have columns: taxi_type, date, pickup_place, hour, count
    (or similar). Fill missing hours with 0.
    """
    if pdf.empty:
        cols = ["taxi_type", "date", "pickup_place"] + [f"hour_{h}" for h in range(24)]
        return pd.DataFrame(columns=cols)
    # Ensure we have a count column
    if "count" not in pdf.columns and "cnt" in pdf.columns:
        pdf = pdf.rename(columns={"cnt": "count"})
    if "count" not in pdf.columns:
        pdf = pdf.copy()
        pdf["count"] = 1
    pivoted = pdf.pivot_table(
        index=["taxi_type", "date", "pickup_place"],
        columns="hour",
        values="count",
        aggfunc="sum",
        fill_value=0,
    )
    pivoted.columns = [f"hour_{int(c)}" for c in pivoted.columns]
    for h in range(24):
        if f"hour_{h}" not in pivoted.columns:
            pivoted[f"hour_{h}"] = 0
    pivoted = pivoted[[f"hour_{h}" for h in range(24)]]
    return pivoted.reset_index()


def cleanup_low_count_rows(
    df: pd.DataFrame,
    min_rides: int = 50,
) -> Tuple[pd.DataFrame, dict]:
    """
    Discard rows where sum of hour_0..hour_23 < min_rides. Return (filtered df, stats).
    """
    hour_cols = [c for c in df.columns if re.match(r"hour_\d+", str(c))]
    if not hour_cols:
        return df, {"dropped": 0, "kept": len(df)}
    total = df[hour_cols].sum(axis=1)
    kept = df[total >= min_rides]
    dropped = len(df) - len(kept)
    return kept, {"dropped": dropped, "kept": len(kept)}


# ---------------------------------------------------------------------------
# Part 2: S3 and file discovery
# ---------------------------------------------------------------------------


def is_s3_path(path: str) -> bool:
    """True if path is an S3 URI (s3://...)."""
    return path.strip().lower().startswith("s3://")


def get_storage_options(path: str) -> dict:
    """
    Return storage options dict for PyArrow/pandas when reading from S3.
    Uses default AWS credentials from environment when path is S3.
    """
    if is_s3_path(path):
        return {}
    return {}


def get_filesystem(path: str):
    """
    Return filesystem for path: S3FileSystem for s3://, else default local.
    """
    if is_s3_path(path):
        try:
            import s3fs
            return s3fs.S3FileSystem()
        except ImportError:
            from pyarrow import fs
            return fs.S3FileSystem()
    return None


def discover_parquet_files(input_path: str) -> list[str]:
    """
    Recursively discover all .parquet files under input_path (local or S3).
    Returns sorted list of full paths.
    """
    if is_s3_path(input_path):
        # Normalize: s3://bucket/prefix -> bucket/prefix
        normalized = input_path.replace("s3://", "").rstrip("/")
        try:
            import s3fs
            fs = s3fs.S3FileSystem()
            # s3fs find() returns paths like "bucket/prefix/file.parquet"
            all_paths = fs.find(normalized)
            out = [f for f in all_paths if f.endswith(".parquet")]
            return sorted("s3://" + p for p in out)
        except ImportError:
            from pyarrow.fs import FileSelector, FileSystem
            fs = FileSystem.from_uri(input_path)[0]
            base = input_path.rstrip("/")
            sel = FileSelector(base, recursive=True)
            infos = fs.get_file_info(sel)
            out = []
            for info in infos:
                if info.path.endswith(".parquet"):
                    p = info.path
                    if not p.startswith("s3://"):
                        p = "s3://" + p.lstrip("/")
                    out.append(p)
            return sorted(out)
    # Local path
    input_p = Path(input_path).expanduser().resolve()
    if not input_p.exists():
        return []
    if input_p.is_file() and input_p.suffix.lower() == ".parquet":
        return [str(input_p)]
    out = [str(p) for p in input_p.rglob("*.parquet") if p.is_file()]
    return sorted(out)
