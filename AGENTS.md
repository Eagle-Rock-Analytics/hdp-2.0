# HDP 2.0 — Agent Guidelines

This is the v2 rewrite of the Historical Observations Platform, incorporating Phase 0–2 from [plan.md](../historical-obs-platform/plan.md):
- **Phase 0**: Append-aware refactor of clean/QAQC/merge
- **Phase 1**: Catch-up backfill to `s3://auto-hdp/hdp/ASOSAWOS/` with env-driven paths
- **Phase 2**: Automated AWS Batch + Step Functions + EventBridge pipeline

See the original repo's [README.md](https://github.com/Eagle-Rock-Analytics/historical-obs-platform/blob/main/README.md) for dataset overview.

---

## Environment

**Python 3.10**, managed via [uv](https://docs.astral.sh/uv/):
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install deps and set up pre-commit
make install-dev

# Or manually:
uv sync --extra dev
uv run pre-commit install
```

> **Note:** `cartopy` requires system libraries (GEOS, PROJ). On Ubuntu/Debian: `sudo apt install libgeos-dev libproj-dev`. On macOS: `brew install geos proj`.

Key packages: `xarray`, `zarr`, `pandas`, `boto3`, `s3fs`, `geopandas`, `dask`.
Code style: `black` (enforced). Run `make format` before committing, or rely on pre-commit hooks.

---

## Repository Structure

```
scripts/
├── paths.py              # Env-driven S3 constants — always import this, never hardcode
├── 1_pull_data/          # Download raw data from 27 networks
├── 2_clean_data/         # Standardize units and format per network
├── 3_qaqc_data/          # QA/QC pipeline (10+ modular checks)
├── 4_merge_data/         # Hourly standardization and final export
├── pcluster/             # SLURM batch scripts for catch-up (Phase 1)
└── tests/                # Data validation tests
infra/                    # CDK stacks for Phase 2 AWS infrastructure (to be created)
Dockerfile                # Single image for all pipeline entrypoints (to be created)
```

---

## Paths and Buckets

`scripts/paths.py` is **env-var driven** in hdp-2.0. All bucket names and prefixes are read from environment variables with fallback defaults:

| Env var | Default | Purpose |
|---|---|---|
| `HDP_BUCKET` | `wecc-historical-wx` | Staging/intermediate data |
| `HDP_PUBLISH_BUCKET` | `cadcat` | Generic publish bucket default in code; current ASOSAWOS source/validation work uses `auto-hdp` |
| `HDP_PUBLISH_PREFIX` | `hdp` | Prefix within publish bucket |

**Always import from `scripts/paths.py`** — never hardcode `s3://wecc-historical-wx` or bucket names anywhere in scripts.

Current ASOSAWOS bucket source for agent work: `s3://auto-hdp/hdp/ASOSAWOS/`

### AWS S3 Layout

| Stage | Prefix | Bucket |
|-------|--------|--------|
| Raw | `1_raw_wx/{NETWORK}/` | `wecc-historical-wx` (or `HDP_BUCKET`) |
| Clean | `2_clean_wx/{NETWORK}/` | `wecc-historical-wx` |
| QAQC | `3_qaqc_wx_v2/{NETWORK}/` | `wecc-historical-wx` |
| Merge (published) | `hdp/{NETWORK}/{STATION}.zarr` | Current ASOSAWOS source: `auto-hdp` at `s3://auto-hdp/hdp/ASOSAWOS/`; generic code path still uses `HDP_PUBLISH_BUCKET/HDP_PUBLISH_PREFIX` |
| Staging pull (Phase 2) | `1_raw_wx/{NETWORK}/` | `hdp-staging-pull` |
| Staging QAQC (Phase 2) | `3_qaqc_wx_v2/{NETWORK}/` | `hdp-staging-qaqc` |

---

## Running the Pipeline

### Single station (local dev)
```bash
# QAQC
python scripts/3_qaqc_data/QAQC_run_for_single_station.py --station="CW3E_HDC" [--rad_scheme="remove_zeros"] [--verbose=False]

# Merge (publishes to HDP_PUBLISH_BUCKET/HDP_PUBLISH_PREFIX)
python scripts/4_merge_data/MERGE_run_for_single_station.py --station="ASOSAWOS_69007093217" [--verbose=False]

# Override output bucket via env vars
HDP_PUBLISH_BUCKET=my-test-bucket HDP_PUBLISH_PREFIX=hdp-test \
  python scripts/4_merge_data/MERGE_run_for_single_station.py --station="CW3E_HDC"
```

### Full network (pcluster / SLURM — Phase 1 catch-up)
See [scripts/pcluster/README.md](scripts/pcluster/README.md) for full instructions.
```bash
cd scripts/pcluster
python generate_station_list.py --network="LOXWFO"
python generate_batch_script.py --network="LOXWFO" --process="qaqc"
sbatch run_qaqc_LOXWFO.sh
```

### Station list update (after batch run)
```bash
python scripts/3_qaqc_data/stnlist_update_qaqc.py <NETWORK>   # or run_stnlist_update_qaqc.sh for all
python scripts/4_merge_data/stnlist_update_merge.py <NETWORK>  # or run_stnlist_update_merge.sh for all
```

### Phase 2 Docker entrypoints (once containerized)
```bash
docker run hdp:latest run-pull --network=<X> --since=<DATE>
docker run hdp:latest run-qaqc --station=<ID>
docker run hdp:latest run-merge --station=<ID>
```

---

## Coding Conventions

**Argument parsing** — use `argparse` with `-s`/`--station` and `-v`/`--verbose` as standard flags.

**Logging** — use the shared logger setup; do not use `print()` in pipeline scripts:
```python
# QAQC scripts
from log_config import setup_logger
logger = setup_logger(log_file="./qaqc_logs/my_log.log", verbose=args.verbose)

# Merge scripts
from merge_log_config import setup_logger
```

**Paths** — always import from `scripts/paths.py`:
```python
from paths import BUCKET_NAME, CLEAN_WX, QAQC_WX, MERGE_WX, PUBLISH_BUCKET, PUBLISH_PREFIX
```

**Error tracking** — accumulate errors as `{"File": [...], "Time": [...], "Error": [...]}` dicts and log/upload at end of run.

**Data I/O** — use `xarray` Datasets for `.zarr` files; use `pandas` DataFrames for tabular/timeseries manipulation within QC checks.

**Unit conventions** — all cleaned data uses SI units: K (temperature), m/s (wind speed), mm (precip), Pa (pressure), m (elevation). See [scripts/2_clean_data/calc_clean.py](scripts/2_clean_data/calc_clean.py) for conversion functions.

---

## Phase 0 — Append-aware refactor (in-progress)

The dominant cost lever for Phase 2 automation is limiting reprocessing to only new timesteps. Phase 0 adds `--append` mode to clean/QAQC/merge so each station processes only its new time-slice. The same code serves catch-up (bulk slice) and biweekly automation (small slice).

- **Append flag**: `--append` skips existing timestamps; without it, behavior is identical to v1 (full overwrite).
- **Climatological checks** (`qaqc_climatological_outlier`, `qaqc_unusual_gaps`): still read the full station zarr to fit distributions, but only write flags for new rows.
- **Merge stage**: reads existing published zarr, concatenates new slice, writes back. Uses `xr.concat` along the time axis.

---

## Data Source Status — ISD Freeze and GHCNh Migration

### ISD is frozen (no 2026 data)
As of early October 2025, **both NOAA ISD access paths stopped being updated**:
- FTP (`ftp.ncdc.noaa.gov/pub/data/noaa/`): last update 2025-08-29; no `2026/` directory
- HTTPS (`https://www.ncei.noaa.gov/data/global-hourly/`): last update 2025-10-02; no `2026/` directory

Do not attempt to pull ASOSAWOS or OtherISD data from these sources for dates after ~October 2025.

### GHCNh is the ISD replacement
NOAA's **Global Historical Climatology Network — Hourly (GHCNh)** is the official
ISD successor. It is actively updated (data lag ~10 days) and covers **1718–2026**.
See `CONTEXT.md` for full technical details. Key facts for pipeline work:

**Access URL:**
```
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
```
Files are **Parquet** (not ISD fixed-width gzip). Read with `pd.read_parquet()`.

**Station ID conversion** (ISD → GHCNh):
```python
# ASOSAWOS_72630014733  →  USW00014733
wban = hdp_station_id.replace("ASOSAWOS_", "")[6:]  # last 5 digits
ghcnh_id = f"USW{int(wban):08d}"
```

**WECC coverage:** 94.9% of ISD stations are in GHCNh. All 63 missing are
decommissioned pre-2000 stations; no active ASOS/AWOS station is absent.

**New pull script needed:** `scripts/1_pull_data/GHCNh_pull.py` — does not exist
yet. The existing `ASOSAWOS_pullftp.py` / `OtherISD_pull.py` ISD FTP approach
cannot be extended to cover 2026+ data. A GHCNh pull must be written to fetch
Parquet files by station and year from the NCEI endpoint.

---

## Gotchas

- **`qaqc_concatenate_stations.py` is one-time only** — it deletes original input stations from S3 after merging co-located ASOSAWOS/MARITIME records. Re-running will fail unless originals are regenerated.
- **SLURM array job limit is 1000** — large networks (HADS, MADIS) are automatically split into `{NETWORK}_1`, `{NETWORK}_2`, etc. by `generate_station_list.py`.
- **QAQC logs** go to `scripts/3_qaqc_data/qaqc_logs/` locally, then are uploaded to S3 at end of run.
- **ASOSAWOS precip units** — the attribute must be explicitly set to `"mm"` after merge (known data artifact).
- **`STATIONS_CSV_PATH`** — the full station list lives at `s3://wecc-historical-wx/2_clean_wx/temp_clean_all_station_list.csv`.
- **`MERGE_pipeline.py` line ~757** — hardcoded merge bucket reference; must use `paths.py` constants.
- **Cross-account writes to `cadcat`** — requires bucket policy on the `cadcat` bucket granting the pipeline IAM role `s3:PutObject`. Confirm with Neil before running Phase 1.
- **Staging bucket TTL** — `hdp-staging-pull` and `hdp-staging-qaqc` have 30-day lifecycle rules. Don't rely on them for long-lived data.

---

## Phase 2 Infrastructure (infra/)

Once Phase 1 is complete, the `infra/` directory will contain CDK stacks (Python) for:

| Stack | Contents |
|---|---|
| `BucketsStack` | `hdp-staging-pull`, `hdp-staging-qaqc` with lifecycle rules; bucket policy on `cadcat` |
| `ComputeStack` | AWS Batch compute environment (EC2 Spot, `c7i-flex`, `c7g`, `m7i-flex`), job queue, 3 job definitions |
| `OrchestratorStack` | Step Functions state machine (pull → diff → clean → QAQC → merge → notify) |
| `ScheduleStack` | EventBridge monthly cron (`0 8 1 * ? *`), SNS alert topic, CloudWatch dashboard |

Trigger a manual run via Step Functions console before letting the cron own it. Verify SNS alert path with an intentional failure first.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
