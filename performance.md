# Performance Summary Report

Filled from the full pipeline run on r8i.4xlarge (8 vCPUs, 128 GB RAM).

## Command and run log

**Command:**
```bash
mkdir -p output
python3 -m pivot_and_bootstrap.pivot_all_files \
  --input-dir s3://dsc291-ucsd/taxi/Dataset/ \
  --output-dir ./output \
  --workers 8 \
  --keep-intermediate \
  --s3-output s3://dsc291-team4-output/team4/output/wide_table.parquet
```

**Console output:**
```
2026-02-03 03:30:15 [INFO] Discovering Parquet files under s3://dsc291-ucsd/taxi/Dataset/
2026-02-03 03:30:16 [INFO] Found credentials from IAM Role: dsc291-ec2-s3-role
2026-02-03 03:30:16 [INFO] Discovered 443 Parquet files
2026-02-03 03:30:16 [INFO] First file to process: s3://dsc291-ucsd/taxi/Dataset/2009/yellow_taxi/yellow_tripdata_2009-01.parquet
2026-02-03 03:30:16 [INFO] Running partition optimization on first file...
2026-02-03 03:30:25 [INFO] Optimal batch size (rows): 74300915395
2026-02-03 03:30:25 [WARNING] Multiple workers (8) use ~2–2.5 GB RAM each; can cause OOM or SSH drops on small instances. If SSH disconnects, use --workers 1 and run in screen/nohup.
2026-02-03 03:30:25 [INFO] Processing 443 files with 8 workers
Files: 100%|██████████████████████████████████████████████████████████████████████████████| 443/443 [12:48<00:00,  1.73s/it]
2026-02-03 03:43:16 [INFO] Total month-mismatch rows: 17064
2026-02-03 03:43:22 [INFO] Wide table rows: 2911496
2026-02-03 03:43:23 [INFO] Uploaded to s3://dsc291-team4-output/team4/output/wide_table.parquet
2026-02-03 03:43:23 [INFO] Report written to output/report.tex
2026-02-03 03:43:23 [INFO] Done. Input rows=3410052578, Output rows=2911496, Bad rows=8136950, Memory=4094.43 MB, Time=787.67 s
```

---

## Total runtime

- **End-to-end:** 787.67 seconds / 13.13 minutes

## Peak memory usage

- **Peak RSS:** 4094.43 MB (~4.0 GB)

## Row counts

| Metric | Value |
|--------|--------|
| **Total input rows** | 3,410,052,578 |
| **Discarded rows** | 8,136,950 |
| **Percentage skipped** | 0.24% |
| **Rows in intermediate pivoted table(s)** | 443 intermediate files (one per input file); row counts per file not aggregated in report |
| **Rows in final wide table** | 2,911,496 |

## Breakdown of discarded rows by reason

| Reason | Count |
|--------|--------|
| Month mismatch (row month ≠ file month) | 17,064 |
| Low count (< 50 rides) | 8,119,886 |
| Parse failures / invalid data | 0 |
| **Total** | 8,136,950 |

## Date consistency issues

- **Number of inconsistent rows:** 17,064
- **Number of files affected:** (not reported in log; can be derived from per-file stats if needed)

## Row breakdown by year and taxi type (wide table)

| Year | Taxi type | Row count |
|------|-----------|-----------|
| (Fill from wide table: e.g. `df.groupby([df['date'].dt.year, 'taxi_type']).size()`) | | |

(Generate by loading `wide_table.parquet` and grouping by year and `taxi_type`.)

## Schema summary

- **Number of output columns:** 27 (taxi_type, date, pickup_place, hour_0 … hour_23)
- **Column names:** `taxi_type`, `date`, `pickup_place`, `hour_0`, `hour_1`, …, `hour_23`

## S3 location of the single Parquet table

- **URI:** `s3://dsc291-team4-output/team4/output/wide_table.parquet`

## Key metrics summary

| Metric | Value |
|--------|--------|
| Instance | r8i.4xlarge (8 vCPUs, 128 GB RAM) |
| Workers | 8 |
| Files processed | 443 |
| Total runtime | 787.67 s (13.13 min) |
| Peak memory | 4094.43 MB |
| Input rows | 3,410,052,578 |
| Output rows | 2,911,496 |
| Bad rows | 8,136,950 |
| Month-mismatch rows | 17,064 |
