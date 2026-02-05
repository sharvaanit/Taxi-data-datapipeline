# How to Configure AWS, Run the Pipeline, and Fill In the Report

Step-by-step instructions for: configuring AWS CLI, locating the taxi Parquet prefix, running on a small subset, and filling in `performance.md` and the README S3 URI.

- **Linux / macOS:** Use the main sections below; commands use `bash`-style line continuation (`\`).
- **Windows:** See **[Running on Windows](#running-on-windows)** for PowerShell/CMD and Windows-specific notes.

---

## 1. Configure AWS CLI

1. **Get credentials**
   - Open the course login page: [DSC291 login](https://ets-apps.ucsd.edu/individual/DSC291_WI26_G00) (or the URL your instructor gave).
   - At the bottom, click **Generate API Keys (for CLI/scripting)**.
   - Copy the values shown (Access Key ID, Secret Access Key, Session Token).

2. **Configure the CLI**
   In a terminal (Command Prompt, PowerShell, or Terminal on macOS/Linux), run:
   ```bash
   aws configure
   ```
   **Windows:** If `aws` is not found, install the [AWS CLI for Windows](https://aws.amazon.com/cli/) (MSI installer) or run `pip install awscli`, then open a new terminal.
   When prompted, paste:
   - **AWS Access Key ID** → from the portal
   - **AWS Secret Access Key** → from the portal
   - **AWS Session Token** → from the portal
   - **Default region** → `us-west-2` (or press Enter to skip)
   - **Default output** → press Enter to skip

   Alternatively, the portal may give you a block of environment variables or a single command to paste; if so, paste that into the terminal instead.

3. **Verify**
   ```bash
   aws sts get-caller-identity
   ```
   You should see JSON with your account ID and ARN.

   ```bash
   aws s3 ls
   ```
   You should see a list of buckets (including `dsc291-ucsd` if you have access).

---

## 2. Locate the Taxi Parquet Prefix in `s3://dsc291-ucsd/`

1. **List the course bucket**
   ```bash
   aws s3 ls s3://dsc291-ucsd/
   ```
   Note the prefixes (folder names), e.g. `taxi/`, `data/`, etc.

2. **Find where the Parquet files live**
   If you see something like `taxi/`, list it:
   ```bash
   aws s3 ls s3://dsc291-ucsd/taxi/
   ```
   If files are directly there and names look like `yellow_tripdata_2023-01.parquet`, your **input prefix** is:
   ```text
   s3://dsc291-ucsd/taxi/
   ```

   If there are more subfolders (e.g. by year), list one and keep going until you see `.parquet` files:
   - **Linux/macOS:** `aws s3 ls s3://dsc291-ucsd/taxi/ --recursive | head -20`
   - **Windows PowerShell:** `aws s3 ls s3://dsc291-ucsd/taxi/ --recursive | Select-Object -First 20`
   Use the **shortest prefix that contains all taxi Parquet files** as your `--input-dir`. Examples:
   - All files under `taxi/`: `s3://dsc291-ucsd/taxi/`
   - All under `data/nyc-tlc/`: `s3://dsc291-ucsd/data/nyc-tlc/`

3. **Write down**
   - **TAXI_PREFIX** = the prefix you found, e.g. `s3://dsc291-ucsd/taxi/`
   - **YOUR_KEY** = where you want the final wide table in S3, e.g. `team4/output` (full URI: `s3://dsc291-ucsd/team4/output/wide_table.parquet`)

---

## 3. Run on a Small Subset (One or Two Files)

From the **project root** (the folder that contains `pivot_and_bootstrap/` and `requirements.txt`).

**Windows:** Use **[Running on Windows](#running-on-windows)** for `mkdir`, one-line commands, and PowerShell multi-line syntax.

1. **Create an output directory**
   - **Linux/macOS:** `mkdir -p output`
   - **Windows:** `mkdir output`

2. **Run with one or two files**
   Replace `<TAXI_PREFIX>` and `<YOUR_KEY>` with the values from step 2.

   **Test with 1 file (fast):**
   ```bash
   python3 -m pivot_and_bootstrap.pivot_all_files \
     --input-dir s3://dsc291-ucsd/<TAXI_PREFIX> \
     --output-dir ./output \
     --skip-partition-optimization \
     --max-files 1 \
     --keep-intermediate \
     --s3-output s3://dsc291-ucsd/<YOUR_KEY>/wide_table.parquet
   ```

   **Test with 2 files (still small):**
   ```bash
   python3 -m pivot_and_bootstrap.pivot_all_files \
     --input-dir s3://dsc291-ucsd/<TAXI_PREFIX> \
     --output-dir ./output \
     --skip-partition-optimization \
     --max-files 2 \
     --keep-intermediate \
     --s3-output s3://dsc291-ucsd/<YOUR_KEY>/wide_table.parquet
   ```

   Example with concrete values:
   ```bash
   python3 -m pivot_and_bootstrap.pivot_all_files \
     --input-dir s3://dsc291-ucsd/taxi/ \
     --output-dir ./output \
     --skip-partition-optimization \
     --max-files 1 \
     --keep-intermediate \
     --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet
   ```

3. **Check the result**
   - `./output/wide_table.parquet` should exist.
   - `./output/report.tex` (or the path you set with `--report`) has input/output rows, memory, run time.
   - If you used `--s3-output`, check S3:
     ```bash
     aws s3 ls s3://dsc291-ucsd/<YOUR_KEY>/
     ```

---

## 4. Fill In `performance.md` and README S3 URI

### 4.1 `performance.md` (in project root)

Open `performance.md` and replace the placeholders with values from your run:

- **Total runtime** — From the log or from `report.tex` / JSON: “Run time (seconds)”.
- **Peak memory usage** — From the report: “Memory use (MB)”.
- **Total input rows** — “Input row count” from the report.
- **Discarded rows** — “Bad rows ignored” (and optionally “Percentage skipped” = discarded / input * 100).
- **Rows in intermediate / final** — From the report: “Output row count” is the final wide table; intermediate row counts can be summed from logs or from intermediate Parquet files if you used `--keep-intermediate`.
- **Breakdown of discarded rows** — Use “Month mismatch” and “Low count dropped” (and any parse failures if reported).
- **Date consistency** — “Month mismatch” total and, if you have it, number of files that had at least one mismatch.
- **Row breakdown by year and taxi type** — After the run, you can compute this from the wide table (e.g. in Python or with a small script) and paste the table into `performance.md`.
- **Schema summary** — Already described in the template (e.g. 27 columns: `taxi_type`, `date`, `pickup_place`, `hour_0` … `hour_23`).
- **S3 location** — Set to the exact URI you used for `--s3-output`, e.g. `s3://dsc291-ucsd/team4/output/wide_table.parquet`.

### 4.2 README S3 output URI

In `pivot_and_bootstrap/README.md`, find the section **“S3 location of the single Parquet table”** and replace the placeholder with your actual URI, for example:

```text
**S3 location of the single Parquet table:** `s3://dsc291-ucsd/team4/output/wide_table.parquet`
```

Also update the “Access” example if needed:

```bash
aws s3 cp s3://dsc291-ucsd/team4/output/wide_table.parquet ./wide_table.parquet
```

---

## 5. Full Run (All Files, No `--max-files`)

When the small subset run works and you’re ready to process all data:

**Important — avoid SSH disconnects:** Running with `--workers 4` (or any value &gt; 1) uses ~2–2.5 GB RAM per worker. On a 4–8 GB instance this can cause **memory spike → OOM or unresponsive box → SSH connection reset (ECONNRESET)**. Use **`--workers 1`** for stable runs on small instances, and run the job in **`nohup`** or **`screen`** so it keeps running if SSH drops:

```bash
# Option A: nohup (survives SSH disconnect; log in output/nohup.out)
nohup python3 -m pivot_and_bootstrap.pivot_all_files \
  --input-dir s3://dsc291-ucsd/<TAXI_PREFIX> \
  --output-dir ./output \
  --skip-partition-optimization \
  --workers 1 \
  --s3-output s3://dsc291-ucsd/<YOUR_KEY>/wide_table.parquet \
  --report report.tex \
  > output/nohup.out 2>&1 &

# Option B: screen (reconnect with: screen -r)
screen -S pivot
python3 -m pivot_and_bootstrap.pivot_all_files \
  --input-dir s3://dsc291-ucsd/<TAXI_PREFIX> \
  --output-dir ./output \
  --skip-partition-optimization \
  --workers 1 \
  --s3-output s3://dsc291-ucsd/<YOUR_KEY>/wide_table.parquet \
  --report report.tex
# Detach: Ctrl+A then D
```

Use **`--workers 1`** on t3.medium (4 GB) or similar. Only use `--workers 4` (or higher) on instances with **16+ GB RAM**. Omit `--max-files` so every discovered file is processed. You can add `--partition-size 50000` or `--skip-partition-optimization` to control memory. After the run, update `performance.md` and the README with the final numbers and S3 URI.

---

## Running on Windows

Use these variants when you are on **Windows** (Command Prompt or PowerShell).

### AWS CLI on Windows

- Install the AWS CLI: [AWS CLI for Windows](https://aws.amazon.com/cli/) (MSI) or run `pip install awscli` in a terminal.
- Open **PowerShell** or **Command Prompt** (search for “PowerShell” or “cmd” in the Start menu).
- Run `aws configure` and paste your Access Key ID, Secret Access Key, and Session Token as in section 1. Verification commands are the same: `aws sts get-caller-identity` and `aws s3 ls`.

### Locate the taxi prefix (Windows)

Same as section 2; only the “first 20 lines” command differs:

- **PowerShell:**  
  `aws s3 ls s3://dsc291-ucsd/taxi/ --recursive | Select-Object -First 20`
- **CMD:**  
  `aws s3 ls s3://dsc291-ucsd/taxi/ --recursive`  
  (no “first 20”; scroll if the list is long.)

### Create output directory (Windows)

In PowerShell or CMD, from the **project root** (the folder that contains `pivot_and_bootstrap` and `requirements.txt`):

```powershell
mkdir output
```

If `output` already exists, this is fine (no error in PowerShell; in CMD you may see “subdirectory already exists”).

### Run on a small subset (Windows)

From the project root, use **one** of the following.

**Option A — One line (works in CMD and PowerShell):**

Replace `<TAXI_PREFIX>` and `<YOUR_KEY>` with your values (e.g. `taxi/` and `team4/output`).

Test with 1 file:

```powershell
python -m pivot_and_bootstrap.pivot_all_files --input-dir s3://dsc291-ucsd/taxi/ --output-dir ./output --skip-partition-optimization --max-files 1 --keep-intermediate --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet
```

Test with 2 files:

```powershell
python -m pivot_and_bootstrap.pivot_all_files --input-dir s3://dsc291-ucsd/taxi/ --output-dir ./output --skip-partition-optimization --max-files 2 --keep-intermediate --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet
```

**Option B — Multi-line in PowerShell (backtick `` ` `` for line continuation):**

```powershell
python -m pivot_and_bootstrap.pivot_all_files `
  --input-dir s3://dsc291-ucsd/taxi/ `
  --output-dir ./output `
  --skip-partition-optimization `
  --max-files 1 `
  --keep-intermediate `
  --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet
```

**Notes:**

- Use `python` if that’s what works on your machine; if you have both `python` and `python3`, either is fine.
- Paths: `./output` is fine. For a different folder use e.g. `.\my_output` or `C:\path\to\output`.
- S3 URIs always use forward slashes: `s3://dsc291-ucsd/taxi/`.

### Check the result (Windows)

- In File Explorer, open your project folder and check for `output\wide_table.parquet` and `output\report.tex`.
- In PowerShell/CMD: `aws s3 ls s3://dsc291-ucsd/team4/output/` (replace with your `YOUR_KEY`).

### Full run — all files (Windows)

One-line (replace `taxi/` and `team4/output` with your prefix and key):

```powershell
python -m pivot_and_bootstrap.pivot_all_files --input-dir s3://dsc291-ucsd/taxi/ --output-dir ./output --min-rides 50 --workers 1 --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet --report report.tex
```

Multi-line in PowerShell:

```powershell
python -m pivot_and_bootstrap.pivot_all_files `
  --input-dir s3://dsc291-ucsd/taxi/ `
  --output-dir ./output `
  --min-rides 50 `
  --workers 1 `
  --s3-output s3://dsc291-ucsd/team4/output/wide_table.parquet `
  --report report.tex
```

### Filling in `performance.md` and README (Windows)

Same as section 4: open `performance.md` and `pivot_and_bootstrap\README.md` in an editor and fill in the values from your run and the S3 URI you used for `--s3-output`. Paths use backslashes in Explorer (`output\report.tex`) but in the docs you can still write the S3 URI with forward slashes.
