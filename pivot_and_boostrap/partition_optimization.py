"""
Partition size optimization: parse size strings and find optimal batch size
for reading Parquet from local or S3 within a memory budget.
"""

from __future__ import annotations

import re
import resource
from typing import List, Optional

import pyarrow.parquet as pq

from .pivot_utils import get_filesystem, is_s3_path


def parse_size(size_str: str) -> int:
    """
    Parse a size string like "200MB", "1.5GB", "500K" into bytes.
    """
    size_str = size_str.strip().upper()
    m = re.match(r"^([\d.]+)\s*([KMGTP]?B?)$", size_str)
    if not m:
        raise ValueError(f"Cannot parse size: {size_str!r}")
    num = float(m.group(1))
    u = (m.group(2) or "B").strip()
    if len(u) > 1 and u.endswith("B"):
        u = u[0]
    multipliers = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    mult = multipliers.get(u, 1)
    return int(num * mult)


def _get_rss_bytes() -> int:
    """Return current process RSS in bytes (Linux: ru_maxrss is in KB)."""
    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss * 1024 if rss else 0
    except Exception:
        return 0


def _open_parquet_file(parquet_path: str):
    """Open Parquet file (local or S3); returns (ParquetFile, context). Use with context manager or close."""
    if is_s3_path(parquet_path):
        fs = get_filesystem(parquet_path)
        if fs is not None:
            try:
                normalized = parquet_path.replace("s3://", "").lstrip("/")
                f = fs.open(normalized, "rb")
                return pq.ParquetFile(f), f
            except Exception:
                pass
        from pyarrow.fs import FileSystem
        fs2, path = FileSystem.from_uri(parquet_path)
        f = fs2.open_input_file(path)
        return pq.ParquetFile(f), f
    return pq.ParquetFile(parquet_path), None


def find_optimal_partition_size(
    parquet_path: str,
    candidate_sizes_bytes: Optional[List[int]] = None,
    max_memory_usage: Optional[int] = None,
    sample_batches: int = 3,
) -> int:
    """
    Find the largest batch size (in rows) that keeps memory under max_memory_usage
    when reading the Parquet file with iter_batches(batch_size=...).

    Args:
        parquet_path: Local path or s3:// URI to a Parquet file.
        candidate_sizes_bytes: List of batch sizes in bytes to try (used to derive row counts).
            If None, uses [50*2**20, 100*2**20, 200*2**20, 500*2**20] (50MB--500MB).
        max_memory_usage: Max memory in bytes (e.g. 1.5GB for t3.medium). If None, 1.5GB.
        sample_batches: Number of batches to sample per candidate for memory measurement.

    Returns:
        Recommended batch_size in rows (for iter_batches(batch_size=...)).
    """
    if max_memory_usage is None:
        max_memory_usage = int(1.5 * 1024**3)  # 1.5 GB
    if candidate_sizes_bytes is None:
        candidate_sizes_bytes = [
            50 * 2**20,
            100 * 2**20,
            200 * 2**20,
            500 * 2**20,
        ]

    # Get metadata (num_rows, bytes_per_row)
    pf, f = _open_parquet_file(parquet_path)
    try:
        num_rows = pf.metadata.num_rows
        total_bytes = pf.metadata.serialized_size or 0
        bytes_per_row = (total_bytes / num_rows) if num_rows else 500
    finally:
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    candidate_rows = [max(1, int(b / bytes_per_row)) for b in candidate_sizes_bytes]
    candidate_rows = sorted(set(candidate_rows))
    best_batch_size = candidate_rows[0] if candidate_rows else 100_000

    for batch_size_rows in candidate_rows:
        peak_rss = 0
        pf, f = _open_parquet_file(parquet_path)
        try:
            for i, batch in enumerate(pf.iter_batches(batch_size=batch_size_rows)):
                if i >= sample_batches:
                    break
                rss = _get_rss_bytes()
                if rss > peak_rss:
                    peak_rss = rss
                del batch
        except Exception:
            peak_rss = max_memory_usage + 1
        finally:
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass

        if peak_rss <= max_memory_usage and batch_size_rows >= best_batch_size:
            best_batch_size = batch_size_rows
        if peak_rss > max_memory_usage and batch_size_rows == candidate_rows[0]:
            best_batch_size = candidate_rows[0]
            break

    return best_batch_size
