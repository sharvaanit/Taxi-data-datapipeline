"""
Main pipeline: discover Parquet files, process by month with streaming,
combine into single wide table, upload to S3, generate report.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from .partition_optimization import find_optimal_partition_size, parse_size
from .pivot_utils import (
    cleanup_low_count_rows,
    discover_parquet_files,
    find_pickup_datetime_col,
    find_pickup_lat_lon_cols,
    find_pickup_location_col,
    get_filesystem,
    infer_month_from_path,
    infer_taxi_type_from_path,
    is_s3_path,
    pivot_counts_date_taxi_type_location,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Canonical column names used after normalization
PICKUP_DATETIME = "pickup_datetime"
PICKUP_LOCATION = "pickup_location"

HOUR_COLS = [f"hour_{h}" for h in range(24)]
INDEX_COLS = ["taxi_type", "date", "pickup_place"]


def _get_rss_bytes() -> int:
    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss * 1024 if rss else 0
    except Exception:
        return 0


def _open_parquet_file(path: str):
    """Open Parquet file (local or S3); returns (ParquetFile, closeable or None)."""
    if is_s3_path(path):
        # In worker processes, use PyArrow S3 only to avoid s3fs/aiobotocore fork issues
        try:
            from multiprocessing import current_process
            in_worker = current_process().name != "MainProcess"
        except Exception:
            in_worker = False
        if not in_worker:
            fs = get_filesystem(path)
            if fs is not None:
                try:
                    normalized = path.replace("s3://", "").lstrip("/")
                    f = fs.open(normalized, "rb")
                    return pq.ParquetFile(f), f
                except Exception:
                    pass
        from pyarrow.fs import FileSystem
        fs2, p = FileSystem.from_uri(path)
        f = fs2.open_input_file(p)
        return pq.ParquetFile(f), f
    return pq.ParquetFile(path), None


def _read_schema_only(path: str) -> pa.Schema:
    """Read only Parquet metadata/schema (no data)."""
    pf, f = _open_parquet_file(path)
    try:
        return getattr(pf, "schema_arrow", None) or getattr(pf, "schema")
    finally:
        if f is not None:
            try:
                f.close()
            except Exception:
                pass


def _normalize_batch_to_common_schema(
    batch: pa.RecordBatch,
    dt_col: str,
    loc_col: Optional[str] = None,
    lat_col: Optional[str] = None,
    lon_col: Optional[str] = None,
) -> pd.DataFrame:
    """Normalize a RecordBatch to canonical columns pickup_datetime, pickup_location."""
    df = batch.to_pandas(timestamp_as_object=True)
    df = df.rename(columns={dt_col: PICKUP_DATETIME})
    if loc_col is not None:
        df = df.rename(columns={loc_col: PICKUP_LOCATION})
        df = df[[PICKUP_DATETIME, PICKUP_LOCATION]]
    elif lat_col is not None and lon_col is not None:
        df[PICKUP_LOCATION] = (
            df[lat_col].astype(float).round(3).astype(str)
            + "_"
            + df[lon_col].astype(float).round(3).astype(str)
        )
        df = df[[PICKUP_DATETIME, PICKUP_LOCATION]]
    else:
        df = df[[PICKUP_DATETIME]]
        df[PICKUP_LOCATION] = ""
    return df[[PICKUP_DATETIME, PICKUP_LOCATION]]


def _aggregate_batch(
    df: pd.DataFrame,
    taxi_type: str,
    expected_year: int,
    expected_month: int,
) -> Tuple[pd.DataFrame, int, int]:
    """
    Extract date, hour, pickup_place; aggregate counts; count month mismatches.
    Returns (aggregated df with columns date, pickup_place, hour, count), total_rows, mismatch_count.
    """
    if df.empty or PICKUP_DATETIME not in df.columns or PICKUP_LOCATION not in df.columns:
        return pd.DataFrame(columns=["date", "pickup_place", "hour", "count"]), 0, 0
    df = df.copy()
    ser = df[PICKUP_DATETIME]
    if pd.api.types.is_datetime64_any_dtype(ser):
        pass  # already datetime
    elif pd.api.types.is_numeric_dtype(ser):
        # Epoch ms or seconds
        try:
            df[PICKUP_DATETIME] = pd.to_datetime(ser, unit="ms", errors="coerce")
        except Exception:
            df[PICKUP_DATETIME] = pd.to_datetime(ser, unit="s", errors="coerce")
    else:
        df[PICKUP_DATETIME] = pd.to_datetime(ser, errors="coerce")
    df = df.dropna(subset=[PICKUP_DATETIME])
    if df.empty:
        return pd.DataFrame(columns=["date", "pickup_place", "hour", "count"]), 0, 0
    df["date"] = df[PICKUP_DATETIME].dt.date
    df["hour"] = df[PICKUP_DATETIME].dt.hour.astype(int)
    df["pickup_place"] = df[PICKUP_LOCATION].astype(str)
    df["taxi_type"] = taxi_type
    total = len(df)
    row_year = df[PICKUP_DATETIME].dt.year
    row_month = df[PICKUP_DATETIME].dt.month
    mismatch = int(((row_year != expected_year) | (row_month != expected_month)).sum())
    agg = df.groupby(["date", "taxi_type", "pickup_place", "hour"], dropna=False).size().reset_index(name="count")
    return agg, total, mismatch


def process_single_file(
    file_path: str,
    output_dir: str,
    batch_size: int,
    min_rides: int = 50,
) -> Dict[str, Any]:
    """
    Process one Parquet file: stream with iter_batches, normalize, aggregate, pivot,
    drop rows with < min_rides, write intermediate Parquet. Return metadata.

    Column names (datetime, location, lat/lon) are detected per file, so different
    files with slightly different column names (e.g. Trip_Pickup_DateTime vs
    tpep_pickup_datetime, Start_Lat/Lon vs PULocationID) are handled automatically.
    """
    expected = infer_month_from_path(file_path)
    if expected is None:
        expected = (0, 0)
    expected_year, expected_month = expected
    taxi_type = infer_taxi_type_from_path(file_path)

    lat_col: Optional[str] = None
    lon_col: Optional[str] = None

    pf, f = _open_parquet_file(file_path)
    try:
        schema = getattr(pf, "schema_arrow", None) or getattr(pf, "schema")
        dt_col = find_pickup_datetime_col(schema)
        loc_col = find_pickup_location_col(schema)
        if not loc_col:
            lat_col, lon_col = find_pickup_lat_lon_cols(schema)
        if not dt_col or (not loc_col and not (lat_col and lon_col)):
            # Fallback: try first batch (RecordBatch has .column_names)
            first_batch = next(pf.iter_batches(batch_size=1000), None)
            if first_batch is not None:
                dt_col = dt_col or find_pickup_datetime_col(first_batch)
                loc_col = loc_col or find_pickup_location_col(first_batch)
                if not loc_col:
                    lat_col, lon_col = find_pickup_lat_lon_cols(first_batch)
        has_location = loc_col is not None or (lat_col is not None and lon_col is not None)
        if not dt_col or not has_location:
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass
            return {
                "file": file_path,
                "input_rows": 0,
                "output_rows": 0,
                "month_mismatch": 0,
                "error": "missing pickup datetime or location (PULocationID or Start_Lat/Start_Lon)",
            }
    finally:
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    all_aggs: List[pd.DataFrame] = []
    total_rows = 0
    month_mismatch_total = 0
    first_batch = True
    debug_pivot = os.environ.get("DEBUG_PIVOT", "").strip() in ("1", "true", "yes")

    pf, f = _open_parquet_file(file_path)
    try:
        for batch in pf.iter_batches(batch_size=batch_size):
            df = _normalize_batch_to_common_schema(batch, dt_col, loc_col=loc_col, lat_col=lat_col, lon_col=lon_col)
            before_dropna = len(df) if (first_batch and debug_pivot) else None
            sample_vals = []
            if first_batch and debug_pivot and not df.empty and PICKUP_DATETIME in df.columns:
                ser = df[PICKUP_DATETIME]
                non_null = ser.dropna()
                sample_vals = non_null.head(3).tolist() if len(non_null) else []
            agg, n, mismatch = _aggregate_batch(df, taxi_type, expected_year, expected_month)
            if first_batch and debug_pivot and before_dropna is not None:
                ser = df[PICKUP_DATETIME] if PICKUP_DATETIME in df.columns else None
                logger.info(
                    "[DEBUG_PIVOT] First batch: file=%s dt_col=%s loc_col=%s dtype=%s "
                    "before_dropna=%d after_dropna=%d sample=%s",
                    file_path, dt_col, loc_col, ser.dtype if ser is not None else None,
                    before_dropna, n, sample_vals,
                )
            total_rows += n
            month_mismatch_total += mismatch
            if first_batch:
                first_batch = False
            if not agg.empty:
                all_aggs.append(agg)
            del batch
            del df
    finally:
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    if not all_aggs:
        return {
            "file": file_path,
            "input_rows": total_rows,
            "output_rows": 0,
            "month_mismatch": month_mismatch_total,
        }

    combined = pd.concat(all_aggs, ignore_index=True)
    combined = combined.groupby(["date", "taxi_type", "pickup_place", "hour"], dropna=False)["count"].sum().reset_index()
    pivoted = pivot_counts_date_taxi_type_location(combined)
    pivoted, cleanup_stats = cleanup_low_count_rows(pivoted, min_rides=min_rides)
    output_rows = len(pivoted)

    # Write intermediate Parquet
    out_path = Path(output_dir) / "intermediate"
    out_path.mkdir(parents=True, exist_ok=True)
    base_name = Path(file_path).name.replace(".parquet", "") + "_pivoted.parquet"
    out_file = out_path / base_name
    pivoted.to_parquet(out_file, index=False)

    return {
        "file": file_path,
        "input_rows": total_rows,
        "output_rows": output_rows,
        "month_mismatch": month_mismatch_total,
        "low_count_dropped": cleanup_stats.get("dropped", 0),
        "intermediate_path": str(out_file),
    }


def _process_one_file_task(args_tuple: Tuple[str, str, int, int]) -> Dict[str, Any]:
    """
    Worker for parallel processing: (file_path, output_dir, batch_size, min_rides) -> result dict.
    Used with ProcessPoolExecutor; workers use PyArrow S3 to avoid s3fs/async fork issues.
    """
    file_path, output_dir, batch_size, min_rides = args_tuple
    try:
        return process_single_file(file_path, output_dir, batch_size=batch_size, min_rides=min_rides)
    except Exception as e:
        return {"file": file_path, "input_rows": 0, "output_rows": 0, "month_mismatch": 0, "error": str(e)}


def combine_into_wide_table(
    intermediate_dir: str,
    output_path: str,
) -> int:
    """
    Read all intermediate Parquet files, aggregate by (taxi_type, date, pickup_place),
    sum hour columns, write single wide table. Returns output row count.
    """
    inter_path = Path(intermediate_dir)
    files = list(inter_path.glob("*.parquet"))
    if not files:
        # Empty wide table
        empty = pd.DataFrame(columns=INDEX_COLS + HOUR_COLS)
        empty.to_parquet(output_path, index=False)
        return 0

    dfs = []
    for p in files:
        df = pd.read_parquet(p)
        for c in HOUR_COLS:
            if c not in df.columns:
                df[c] = 0
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    hour_cols_present = [c for c in HOUR_COLS if c in combined.columns]
    if not hour_cols_present:
        hour_cols_present = HOUR_COLS
    wide = combined.groupby(INDEX_COLS, dropna=False)[hour_cols_present].sum().reset_index()
    for c in HOUR_COLS:
        if c not in wide.columns:
            wide[c] = 0
    wide = wide[INDEX_COLS + HOUR_COLS]
    wide.to_parquet(output_path, index=False)
    return len(wide)


def run_schema_check(files: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Check schemas of first few files; return (dt_col, loc_col, error_message).
    If schemas differ, we use column detection per file; dt_col/loc_col can be None.
    """
    if not files:
        return None, None, "no files"
    dt_col = None
    loc_col = None
    for path in files[: min(5, len(files))]:
        try:
            schema = _read_schema_only(path)
            dt = find_pickup_datetime_col(schema)
            loc = find_pickup_location_col(schema)
            if dt:
                dt_col = dt
            if loc:
                loc_col = loc
        except Exception as e:
            logger.warning("Schema check failed for %s: %s", path, e)
    return dt_col, loc_col, None if (dt_col and loc_col) else "could not detect pickup datetime/location columns"


def main() -> None:
    parser = argparse.ArgumentParser(description="Taxi pivot pipeline: S3/local -> wide table Parquet")
    parser.add_argument("--input-dir", required=True, help="Local dir or s3://bucket/prefix")
    parser.add_argument("--output-dir", required=True, help="Local output directory")
    parser.add_argument("--min-rides", type=int, default=50, help="Drop rows with fewer rides (default 50)")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel file workers (default 1). Each process uses ~2–2.5 GB RAM; set based on available memory.",
    )
    parser.add_argument(
        "--partition-size",
        type=str,
        default=None,
        help="Batch size in rows or size string (e.g. 200MB). If unset, run partition optimization.",
    )
    parser.add_argument(
        "--skip-partition-optimization",
        action="store_true",
        help="Skip partition optimization; use default batch size 100_000",
    )
    parser.add_argument("--keep-intermediate", action="store_true", help="Keep intermediate Parquet files")
    parser.add_argument("--s3-output", type=str, default=None, help="S3 URI to upload final Parquet (e.g. s3://bucket/key)")
    parser.add_argument("--report", type=str, default="report.tex", help="Report output path (default report.tex)")
    parser.add_argument("--max-files", type=int, default=None, help="Process only first N files (for testing)")
    args = parser.parse_args()

    start_time = time.time()
    start_rss = _get_rss_bytes()

    # Discover files
    logger.info("Discovering Parquet files under %s", args.input_dir)
    files = discover_parquet_files(args.input_dir)
    # Only process paths that look like trip data (skip UserGuide, DataDictionary, etc.)
    files = [f for f in files if "tripdata" in f.lower()]
    if not files:
        logger.error("No tripdata Parquet files found under %s", args.input_dir)
        sys.exit(1)
    # Prefer yellow/green (standard tpep/lpep datetime columns) over FHV (can use epoch/different format)
    files.sort(key=lambda p: (0 if "yellow" in p.lower() or "green" in p.lower() else 1, p))
    if args.max_files is not None:
        files = files[: args.max_files]
        logger.info("Limiting to first %d files (--max-files)", len(files))
    logger.info("Discovered %d Parquet files", len(files))
    if files:
        logger.info("First file to process: %s", files[0])
    if not files:
        logger.error("No Parquet files found")
        sys.exit(1)

    # Schema check
    run_schema_check(files)

    # Partition size
    if args.skip_partition_optimization or args.partition_size:
        if args.partition_size:
            try:
                batch_size = parse_size(args.partition_size)
                # Approximate rows from bytes (assume ~500 bytes/row)
                batch_size = max(10_000, batch_size // 500)
            except ValueError:
                batch_size = int(args.partition_size)
        else:
            batch_size = 100_000
    else:
        logger.info("Running partition optimization on first file...")
        batch_size = find_optimal_partition_size(files[0], max_memory_usage=int(1.5 * 1024**3))
        logger.info("Optimal batch size (rows): %d", batch_size)

    # Build flat list of (file_path, output_dir, batch_size, min_rides) in month order
    by_month: Dict[Tuple[int, int], List[str]] = defaultdict(list)
    for p in files:
        m = infer_month_from_path(p)
        if m:
            by_month[m].append(p)
    months_sorted = sorted(by_month.keys())
    flat_tasks: List[Tuple[str, str, int, int]] = []
    for (year, month) in months_sorted:
        for fp in by_month[(year, month)]:
            flat_tasks.append((fp, args.output_dir, batch_size, args.min_rides))

    total_input_rows = 0
    total_month_mismatch = 0
    total_low_count_dropped = 0
    intermediate_paths: List[str] = []
    workers = max(1, args.workers)
    if workers > 1:
        logger.warning(
            "Multiple workers (%d) use ~2–2.5 GB RAM each; can cause OOM or SSH drops on small instances. "
            "If SSH disconnects, use --workers 1 and run in screen/nohup.",
            workers,
        )

    if workers <= 1:
        for task in tqdm(flat_tasks, desc="Files"):
            result = _process_one_file_task(task)
            if "error" in result:
                logger.warning("Failed %s: %s", result.get("file"), result["error"])
            total_input_rows += result.get("input_rows", 0)
            total_month_mismatch += result.get("month_mismatch", 0)
            total_low_count_dropped += result.get("low_count_dropped", 0)
            if "intermediate_path" in result:
                intermediate_paths.append(result["intermediate_path"])
    else:
        logger.info("Processing %d files with %d workers", len(flat_tasks), workers)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one_file_task, t): t for t in flat_tasks}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Files"):
                result = future.result()
                if "error" in result:
                    logger.warning("Failed %s: %s", result.get("file"), result["error"])
                total_input_rows += result.get("input_rows", 0)
                total_month_mismatch += result.get("month_mismatch", 0)
                total_low_count_dropped += result.get("low_count_dropped", 0)
                if "intermediate_path" in result:
                    intermediate_paths.append(result["intermediate_path"])

    # Month-mismatch report
    logger.info("Total month-mismatch rows: %d", total_month_mismatch)

    # Combine into wide table
    output_parquet = str((Path(args.output_dir) / "wide_table.parquet").resolve())
    inter_dir = str(Path(args.output_dir) / "intermediate")
    output_rows = combine_into_wide_table(inter_dir, output_parquet)
    logger.info("Wide table rows: %d", output_rows)

    # Upload to S3 if requested
    if args.s3_output:
        try:
            import subprocess
            subprocess.run(
                ["aws", "s3", "cp", output_parquet, args.s3_output],
                check=True,
                capture_output=True,
            )
            logger.info("Uploaded to %s", args.s3_output)
        except Exception as e:
            logger.warning("S3 upload failed: %s (ensure IAM role has s3:PutObject for the bucket/prefix)", e)

    # Report
    peak_rss = _get_rss_bytes()
    wall_time = time.time() - start_time
    bad_rows = total_month_mismatch + total_low_count_dropped

    report_data = {
        "input_row_count": total_input_rows,
        "output_row_count": output_rows,
        "bad_rows_ignored": bad_rows,
        "month_mismatch_rows": total_month_mismatch,
        "low_count_dropped": total_low_count_dropped,
        "memory_use_bytes": peak_rss,
        "memory_use_mb": round(peak_rss / (1024 * 1024), 2),
        "run_time_seconds": round(wall_time, 2),
        "run_time_minutes": round(wall_time / 60, 2),
    }

    report_path = Path(args.output_dir) / args.report
    if str(report_path).endswith(".tex"):
        tex = []
        tex.append("\\documentclass{article}")
        tex.append("\\begin{document}")
        tex.append("\\section{Pipeline Report}")
        tex.append("\\begin{itemize}")
        tex.append(f"\\item Input row count: {report_data['input_row_count']}")
        tex.append(f"\\item Output row count: {report_data['output_row_count']}")
        tex.append(f"\\item Bad rows ignored: {report_data['bad_rows_ignored']}")
        tex.append(f"\\item Memory use (MB): {report_data['memory_use_mb']}")
        tex.append(f"\\item Run time (seconds): {report_data['run_time_seconds']}")
        tex.append("\\end{itemize}")
        tex.append("\\end{document}")
        report_path.write_text("\n".join(tex), encoding="utf-8")
    else:
        report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")

    logger.info("Report written to %s", report_path)
    logger.info("Done. Input rows=%d, Output rows=%d, Bad rows=%d, Memory=%.2f MB, Time=%.2f s",
                total_input_rows, output_rows, bad_rows, report_data["memory_use_mb"], wall_time)

    if not args.keep_intermediate:
        for p in intermediate_paths:
            try:
                Path(p).unlink()
            except Exception:
                pass


if __name__ == "__main__":
    main()
