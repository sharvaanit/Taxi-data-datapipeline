# Taxi Pivot Pipeline

Pipeline that processes NYC TLC taxi trip Parquet data: pivots trip-level records into (date × taxi_type × pickup_place × hour) counts, produces a single wide table, and stores it as Parquet (local and optionally S3).

**First-time setup and running:** See **[SETUP_AND_RUN.md](SETUP_AND_RUN.md)** for step-by-step instructions on configuring AWS CLI, locating the taxi Parquet prefix in S3, running on a small subset (`--max-files 1`), and filling in `performance.md` and the README S3 URI.

## Setup

- **Python:** 3.9+
- **Dependencies:** From repo root: `pip install -r requirements.txt`

```bash
pip install -r requirements.txt
```

Requirements: `pyarrow`, `pandas`, `s3fs`, `tqdm`, `pytest`.

## Usage

Run the full pipeline from the **project root** (parent of `pivot_and_bootstrap/`):

```bash
python3 -m pivot_and_bootstrap.pivot_all_files \
  --input-dir s3://dsc291-ucsd/<TAXI_PREFIX> \
  --output-dir ./output \
  --min-rides 50 \
  --workers 1 \
  --s3-output s3://dsc291-ucsd/<YOUR_OUTPUT_KEY>/wide_table.parquet \
  --report report.tex \
  --keep-intermediate
```

### CLI Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--input-dir` | Yes | — | Local directory or S3 URI (e.g. `s3://dsc291-ucsd/taxi/`) containing Parquet files |
| `--output-dir` | Yes | — | Local directory for intermediates and final wide table |
| `--min-rides` | No | 50 | Drop rows with total rides (across hours) below this value |
| `--workers` | No | 1 | Number of parallel workers per month (use 1–2 on small instances) |
| `--partition-size` | No | — | Batch size in rows or size string (e.g. `200MB`). If unset, partition optimization runs on first file |
| `--skip-partition-optimization` | No | False | Skip optimization; use default batch size (100k rows) |
| `--keep-intermediate` | No | False | Keep intermediate Parquet files under `output_dir/intermediate/` |
| `--s3-output` | No | — | S3 URI to upload the final wide table (e.g. `s3://bucket/key/wide_table.parquet`) |
| `--report` | No | report.tex | Output path for pipeline report (`.tex` or JSON) |
| `--max-files` | No | — | Process only first N files (for testing; e.g. `--max-files 1`) |

### S3 Input

- **Do not download** the full dataset; the pipeline reads directly from S3 via `s3fs`/PyArrow.
- Configure AWS CLI so the process can access S3: `aws configure` (use course API keys from the portal).
- Verify: `aws s3 ls s3://dsc291-ucsd/` then locate the taxi Parquet prefix (e.g. `s3://dsc291-ucsd/taxi/`).

### S3 Output (Single Parquet Table)

- **Course bucket is read-only.** Use **your own S3 bucket** for output (e.g. create `dsc291-team4-output` in the same AWS account and give your EC2 role `s3:PutObject` on that bucket).
- Use `--s3-output s3://<your-bucket>/<key>/wide_table.parquet` to upload the final wide table (e.g. `s3://dsc291-team4-output/team4/output/wide_table.parquet`).
- **S3 location of the single Parquet table:** After a run, the table will be at the URI you passed to `--s3-output`. Document this URI in your report and here after your first successful run.

**Access:** With AWS CLI configured, download with:
```bash
aws s3 cp s3://<your-bucket>/<key>/wide_table.parquet ./wide_table.parquet
```
(e.g. `aws s3 cp s3://dsc291-team4-output/team4/output/wide_table.parquet ./wide_table.parquet`)

## Project Layout

- `pivot_utils.py` — Column detection, path helpers, pivot, cleanup, S3/local discovery
- `partition_optimization.py` — Parse size strings, find optimal batch size for memory budget
- `pivot_all_files.py` — Main pipeline (discover → process by month → combine → report; optional S3 upload)
- `test_pivot_date_location_hour.py` — Tests for pivot utilities

## Tests

From project root:

```bash
python3 -m pytest pivot_and_bootstrap/test_pivot_date_location_hour.py -v
```

## Troubleshooting

- **"Unable to locate credentials"** — Run `aws configure` and paste course API keys (or set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`).
- **Out of memory** — Use `--partition-size 50MB` or `--skip-partition-optimization` with a small batch (e.g. `--partition-size 50000`). On t3.medium, use 1 worker and small batches.
- **No Parquet files found** — Check `--input-dir` (trailing slash optional). For S3, ensure the prefix exists and you have list permission.
- **Missing pickup columns** — Pipeline detects common names (`tpep_pickup_datetime`, `PULocationID`, etc.). If your schema differs, extend `find_pickup_datetime_col` / `find_pickup_location_col` in `pivot_utils.py`.
