# Pipeline Summary — Implementation Checklist

A step-by-step breakdown of the 5 pipeline stages from the assignment.

---

## Step 1: Discover Parquet files

**Goal:** Get a sorted list of all `.parquet` files to process.

- **Input:** A path that is either:
  - A **local directory** (e.g. `./data/`), or
  - An **S3 URI** (e.g. `s3://bucket-name/prefix/`)
- **Action:** Recursively find all files ending in `.parquet`, then **sort** the list.
- **Output:** List of file paths (local or S3) to feed into Step 2.

**Implementation notes:** Use `discover_parquet_files(input_path)` from Part 2; use `get_filesystem()` so the same logic works for local and S3.

---

## Step 2: Check schemas and define a common schema

**Goal:** Ensure every file can be processed with the same column semantics.

- **Action:**
  1. Read schema (or a small sample) from each discovered Parquet file.
  2. **Check** if schemas are consistent (same column names/types across files).
  3. If **not** consistent: **define a common schema** that includes all required columns:
     - `pickup_datetime` (or equivalent; detect via `find_pickup_datetime_col`)
     - `pickup_location` (or equivalent; detect via `find_pickup_location_col`)
     - Any other columns needed for taxi_type, date, hour extraction.
  4. When reading each file, **normalize** into this common schema (rename/map columns, cast types) before any aggregation.

**Output:** A single “canonical” schema and a normalization step applied to every file read in Step 3.

---

## Step 3: Process by month (one month at a time)

**Goal:** For each month, read files → normalize → aggregate → pivot → drop low-count rows → write intermediate Parquet. Also count month-mismatch rows.

### 3a. Group files by month

- For each file path, call `infer_month_from_path(file_path)` → `(year, month)` (e.g. `2023`, `1`).
- **Group** all file paths by `(year, month)`.
- Process **one (year, month) group at a time** (e.g. all 2023-01 files, then 2023-02, …).

### 3b. For each file in that month

1. **Read** Parquet (local or S3).
2. **Normalize** to the common schema (from Step 2).
3. **Extract:** date (from pickup_datetime), taxi_type (e.g. from path via `infer_taxi_type_from_path`), pickup_place, hour (0–23).
4. **Aggregate:** count by `(date, taxi_type, pickup_place, hour)`.
5. **Pivot:** one row per `(taxi_type, date, pickup_place)` with columns `hour_0` … `hour_23` (use `pivot_counts_date_taxi_type_location`).
6. **Discard** rows with total rides (sum of hour_0…hour_23) **&lt; 50** (use `cleanup_low_count_rows(..., min_rides=50)`).
7. **Write** intermediate Parquet for this file (e.g. under `output_dir/intermediate/`).

### 3c. Month-mismatch reporting

- When normalizing/parsing, for each row compute the **month from the row’s pickup_datetime**.
- Compare to the **expected month** from the file path (`infer_month_from_path`).
- **Count** rows where row_month ≠ file_month (per file and/or per month).
- **Report:** total count of month-mismatch rows; optionally per-file and per-month breakdown (console, log, or small summary file).

---

## Step 4: Combine into a single wide table and store (including S3)

**Goal:** One final table: all data, indexed by `(taxi_type, date, pickup_place)`, with hour columns summed across all intermediates.

- **Input:** All intermediate Parquet files produced in Step 3.
- **Actions:**
  1. **Read** all intermediate Parquet files.
  2. **Aggregate** by `(taxi_type, date, pickup_place)`: **sum** the hour columns (`hour_0` … `hour_23`) across files.
  3. Result = **single wide table** with:
    - Index/keys: `taxi_type`, `date`, `pickup_place`
    - Columns: `hour_0` … `hour_23`
  4. **Store** this table as Parquet:
    - Locally (e.g. single file or partitioned `year=YYYY/month=MM/...`).
    - **Upload to S3** (same structure or single object).
  5. **Document** in README: S3 path(s) and any access instructions.

**Output:** One Parquet dataset (local + S3) representing “all available data” in wide form.

---

## Step 5: Generate the pipeline report

**Goal:** Emit a small report (e.g. `.tex` or other format per assignment) with these metrics:

| Metric | Meaning |
|--------|--------|
| **Input row count** | Total rows read from all input Parquet files (before pivot/aggregation). |
| **Output row count** | Number of rows in the final wide table (after combine and low-count drop). |
| **Bad rows ignored** | Rows dropped for any reason: parse failures, invalid data, month mismatch, **&lt; 50 rides**, etc. |
| **Memory use** | Peak RSS (or similar) during the pipeline run. |
| **Run time** | Wall-clock time for the full pipeline (discovery → process by month → combine → S3 → report). |

- **Output:** Write this report to a small file (e.g. `report.tex` or as specified in the assignment).

---

## Data flow (high level)

```
Input path (local or S3)
    → Step 1: discover_parquet_files() → list of .parquet paths
    → Step 2: schema check + common schema + normalize
    → Step 3: group by (year, month)
              → for each month: for each file:
                  read → normalize → aggregate (date, taxi_type, pickup_place, hour)
                  → pivot → drop rows with < 50 rides → write intermediate Parquet
                  → count month-mismatch rows
              → report month-mismatch (total + optional breakdown)
    → Step 4: read all intermediates → aggregate by (taxi_type, date, pickup_place), sum hours
              → single wide table → write Parquet → upload to S3
    → Step 5: collect stats (input rows, output rows, bad rows, memory, wall time)
              → write report (e.g. report.tex)
```

---

## Quick reference: key functions (from assignment)

| Step | Functions / concepts |
|------|----------------------|
| 1 | `discover_parquet_files(input_path)`, `get_filesystem()` |
| 2 | Schema inspection, `find_pickup_datetime_col`, `find_pickup_location_col`, common schema, normalize |
| 3 | `infer_month_from_path`, `infer_taxi_type_from_path`, `pivot_counts_date_taxi_type_location`, `cleanup_low_count_rows(min_rides=50)`, month-mismatch count |
| 4 | `combine_into_wide_table`, write Parquet, S3 upload, README S3 docs |
| 5 | Counts + peak RSS + wall time → report file |

Use this alongside `HOMEWORK_ASSISGNMENT1.md` and the Part 1–6 specs when implementing `pivot_utils.py`, `partition_optimization.py`, and `pivot_all_files.py`.
