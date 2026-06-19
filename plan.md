# Plan: Catch-up + Automate Historical Obs Platform (v2 — append-aware)

> **Living document** — last updated 2026-06-19. Reflects completed Phase 0 work,
> the ISD freeze discovery, and the resulting GHCNh pivot for ASOSAWOS/OtherISD.

---

## Current Status (as of 2026-06-19)

### Phase 0 — Append-aware refactor: 45% complete (5/11 tasks)

**Done:**
- `paths.py` fully env-var driven (`HDP_BUCKET`, `HDP_PUBLISH_BUCKET`, `HDP_PUBLISH_PREFIX`) ✓ `hdp-b1d.1`
- `ASOSAWOS_clean.py --append` mode: slices from per-station boundary, writes to `_append/` staging key ✓ `hdp-b1d.2`
- QAQC `--append` mode: reads from `_append/` staging key, writes QAQC'd slice to `QAQC_APPEND` key ✓ `hdp-b1d.3`
- Test bucket provisioned, `cadcat/hdp/ASOSAWOS` baseline copied ✓ `hdp-b1d.5`
- Per-station last-timestamp discovery script (`discover_last_timestamps_asosawos.py`) ✓ `hdp-b1d.6`
- Bug fixes: QAQC append zarr staging path ✓ `hdp-gmy`; clean append boundary aligned to hour start ✓ `hdp-h1x`

**In progress:**
- Merge `--append` mode (dedup-after-concat): core implementation done; pending e2e real-station verification ◐ `hdp-b1d.4`
- ASOSAWOS raw pull orchestrator (`pull_asosawos_from_last_timestamps.py`): implemented + tested; **blocked by ISD freeze** (see below) ◐ `hdp-b1d.7`

**Open (not started):**
- `hdp-b1d.8`: Run ASOSAWOS clean on new raw slice
- `hdp-b1d.9`: Run QAQC on new ASOSAWOS clean slice
- `hdp-b1d.10`: Run merge append into test bucket for all ASOSAWOS stations
- `hdp-b1d.11`: Validate ASOSAWOS timeseries continuity in test bucket

**Known bugs (open):**
- `hdp-8fr`: `merge_hourly_standardization` TypeError in float conversion — pre-existing, blocks e2e
- `hdp-1st`: ASOSAWOS pull granularity is year-level (not exact timestamp); deferred P2

---

## Critical Blocker: ISD Freeze → GHCNh Migration

### What happened
As of **early October 2025**, NOAA stopped updating both ISD access paths:
- FTP `ftp.ncdc.noaa.gov/pub/data/noaa/`: last update 2025-08-29; no `2026/` directory
- HTTPS `ncei.noaa.gov/data/global-hourly/`: last update 2025-10-02; no `2026/` directory

The existing `ASOSAWOS_pullftp.py` and `pull_asosawos_from_last_timestamps.py` pull from the ISD FTP. They can reach data through ~Oct 2025 but **cannot retrieve any 2026 data**. The Phase 1 catch-up goal (continuous timeseries through today) is blocked until a new pull path is in place.

### The replacement: GHCNh
NOAA's **Global Historical Climatology Network — Hourly (GHCNh)** is the official ISD
successor (`https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly`).
It is actively updated (~10 day lag) and covers 1718–2026. Full technical details in
`CONTEXT.md`. Key facts for the pipeline pivot:

- **Data available:** 1718–June 2026 (continuously updated)
- **WECC station coverage:** 94.9% of active ISD stations (all 63 missing are decommissioned pre-2020)
- **Format:** Parquet per station per year — cleaner than ISD fixed-width gzip
- **Primary endpoint (all years):**
  ```
  https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
  ```
- **Station ID mapping:** `ASOSAWOS_72630014733` → `USW00014733` (WBAN, `USW` prefix, 8 digits zero-padded)
- **Key variables:** `temperature`, `dew_point_temperature`, `wind_speed`, `wind_direction`, `precipitation`, `sea_level_pressure`, `relative_humidity` (+ QC codes per variable)

### Scope of required new work
The GHCNh pivot requires three new scripts in `scripts/1_pull_data/` and `scripts/2_clean_data/`:

| Script | Status | Notes |
|---|---|---|
| `scripts/1_pull_data/GHCNh_pull.py` | **Not yet written** | Fetch Parquet by station+year from NCEI endpoint |
| `scripts/2_clean_data/GHCNh_clean.py` | **Not yet written** | Standardize GHCNh Parquet → HDP NetCDF; handles unit conversions, variable renaming |
| `scripts/3_qaqc_data/` | Likely reusable | QAQC reads HDP NetCDF format; should work if clean output matches existing schema |
| `scripts/4_merge_data/` | Likely reusable | Merge reads QAQC zarr; should work unchanged |

OtherISD is in the same position as ASOSAWOS (also pulled from ISD FTP) and will need the same GHCNh pull+clean treatment. Scope: after ASOSAWOS POC validates.

---

## TL;DR (revised)
Three-phase plan, revised for GHCNh migration:
- **Phase 0 — Append-aware refactor (~30h original, ~45% done)**: Add `--append` mode to clean/QAQC/merge. Core implementation complete; merge e2e and full-run validation remain.
- **Phase 1 — GHCNh integration + catch-up (~30h revised)**: Write `GHCNh_pull.py` and `GHCNh_clean.py`, then run the append-aware pipeline from per-station last timestamps to present. This replaces the original ISD-FTP-based P1.2.
- **Phase 2 — Automation (~22h unchanged)**: AWS Batch + Step Functions + EventBridge, biweekly cadence. GHCNh pull replaces ISD pull in the containerized pipeline.

Decisions locked in: cross-account write to `cadcat/hdp`, biweekly cadence, append refactor in-scope, page on any failure, path layout `s3://cadcat/hdp/{NETWORK}/{STATION}.zarr` unchanged, climatology recompute = **Option A — refit from full record on every biweekly run**.

---

## Key constraints (discovered)
- **ISD is frozen** — no data past ~Oct 2025 from either FTP or HTTPS endpoint.
- **GHCNh is the ISD replacement** — Parquet format, NCEI Ceph object store, full historical record.
- **Pull is incremental** for all 8 network families (`update_pull.py`); GHCNh pull must follow the same per-station boundary logic.
- **Clean / QAQC / Merge overwrite full station zarrs (`mode="w"`)** — append refactor in Phase 0 resolves this.
- **`paths.py` is now env-var driven** — done in `hdp-b1d.1`.
- **15,064 stations** total; pcluster auto-splits networks above SLURM's 1000-job array limit.
- **45-day data lag** is intentional in `update_pull.py`.

## Phase 0 — Append-aware refactor (in-progress)

### P0.1 — Path/output parameterization ✓ DONE (`hdp-b1d.1`)
`paths.py` reads `BUCKET_NAME`, `RAW_WX`, `CLEAN_WX`, `QAQC_WX`, `MERGE_WX`,
`PUBLISH_BUCKET`, `PUBLISH_PREFIX` from env vars with current values as defaults.
Hardcoded bucket literals audited and replaced.

### P0.2 — Clean `--append` mode ✓ DONE (`hdp-b1d.2`, `hdp-h1x`)
`ASOSAWOS_clean.py --append` slices from per-station boundary (aligned to hour
start), writes to `s3://{HDP_BUCKET}/2_clean_wx/{NETWORK}/_append/{STATION}.nc`.

### P0.3 — QAQC `--append` mode ✓ DONE (`hdp-b1d.3`, `hdp-gmy`)
QAQC reads from `QAQC_APPEND` staging key, writes QAQC'd slice to append path.

### P0.4 — Merge `--append` mode ◐ IN PROGRESS (`hdp-b1d.4`)
Core implementation done (dedup-after-concat, baseline fallback, skip eraqc CSV
overwrites in append mode). Pending: e2e real-station 30-day overlap verification
after `hdp-8fr` (TypeError bug) is resolved.

### P0.5 — Per-station discovery + pull orchestrator ◐ IN PROGRESS (`hdp-b1d.6`, `hdp-b1d.7`)
- `discover_last_timestamps_asosawos.py` ✓ done
- `pull_asosawos_from_last_timestamps.py` ✓ implemented + tested; **blocked by ISD freeze**

### P0.6 — Fix `merge_hourly_standardization` TypeError ○ OPEN (`hdp-8fr`)
Pre-existing bug: `sequence item 0: expected str instance, float found`. Blocks
e2e append pipeline testing.

### P0.7 — Remaining append e2e (open)
After `hdp-8fr` resolved and merge append verified:
- `hdp-b1d.8`: Run ASOSAWOS clean on new raw slice
- `hdp-b1d.9`: Run QAQC on new ASOSAWOS clean slice
- `hdp-b1d.10`: Run merge append into test bucket for all ASOSAWOS stations
- `hdp-b1d.11`: Validate ASOSAWOS timeseries continuity in test bucket

---

## Phase 1 — GHCNh Integration + Catch-up (revised, ~30h)

> **Revised from original.** The original P1.2 (ISD FTP pull 2022→2026) is no
> longer feasible — ISD is frozen past Oct 2025. ASOSAWOS and OtherISD must
> migrate to GHCNh. Non-ISD networks (HADS, CIMIS, CW3E, MADIS, etc.) are
> unaffected.

### P1.1 — Write `GHCNh_pull.py` (~8h) ○ NOT STARTED
New script: `scripts/1_pull_data/GHCNh_pull.py`. Responsibilities:
- Accept `--station <ASOSAWOS_ID>` (or list), `--start-year`, `--end-year`
- Convert HDP station ID → GHCNh ID: `ASOSAWOS_72630014733` → `USW00014733`
- Fetch per-station per-year Parquet files from NCEI:
  ```
  https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
  ```
- Handle 404s gracefully (station absent for a year = no data, not an error)
- Write raw Parquet to `s3://{HDP_BUCKET}/1_raw_wx/ASOSAWOS/{STATION}/{YEAR}.parquet`
  (or `.gz` equivalent if downstream expects the ISD `.gz` naming; TBD in design)
- Integrate with `pull_asosawos_from_last_timestamps.py` — replace the FTP call
  with a GHCNh Parquet fetch for the relevant year range
- Logging + error accumulation pattern matching existing scripts

**Design decision needed:** does raw storage stay as-is (ISD `.gz`) or switch to
Parquet? Parquet is the natural GHCNh format; keeping it avoids a re-encode step.
The clean stage will need to adapt either way.

### P1.2 — Write `GHCNh_clean.py` (~10h) ○ NOT STARTED
New script: `scripts/2_clean_data/GHCNh_clean.py`. Responsibilities:
- Read GHCNh Parquet (329 columns, `temperature`, `dew_point_temperature`, etc.)
- Map GHCNh column names → HDP variable names (see `CONTEXT.md` for full table)
- Apply unit conversions to SI: °C → K (temperature, dew point), m/s wind already
  in m/s, mm precip already in mm, hPa → Pa (pressure)
- Reconstruct hourly timestamp from `DATE` ISO string (or Year/Month/Day/Hour)
- Handle sub-hourly obs: select the `:56` observation per hour as the canonical
  hourly value (matching ASOS METAR convention), or use the last obs per hour
- Apply QC flag filtering consistent with existing ASOSAWOS clean logic
- Output: same `.nc` format as `ASOSAWOS_clean.py` so downstream QAQC/merge are
  unchanged
- Support `--append` flag for slice-only processing

**Verification:** output of `GHCNh_clean.py` for an overlap period (e.g. 2024)
should match `ASOSAWOS_clean.py` output from ISD for the same station+year.

### P1.3 — Wire GHCNh into append pipeline (~4h) ○ NOT STARTED
- Update `pull_asosawos_from_last_timestamps.py` to call `GHCNh_pull.py` for
  stations where last-timestamp is Oct 2025 or later (or always, if the decision
  is to cut over cleanly)
- Add `--source ghcnh|isd` flag or auto-detect based on year (ISD for ≤2025,
  GHCNh for 2026+) — design TBD
- OtherISD: same treatment; `OtherISD_pull.py` and `OtherISD_clean.py` need
  GHCNh equivalents or an adapter. Defer to after ASOSAWOS POC validates.

### P1.4 — Catch-up pull: ASOSAWOS via GHCNh (~4h wallclock) ○ NOT STARTED
- Run GHCNh pull for all active ASOSAWOS stations from each station's
  last-timestamp through today-45d
- Verify raw Parquet lands in S3; spot-check data against NCEI web explorer
- Track failures per `stnlist_update_pull.py` pattern

### P1.5 — Clean/QAQC/Merge catch-up on pcluster (~10h work, multi-day wallclock) ○ NOT STARTED
- Run `GHCNh_clean.py --append` per station
- Run QAQC `--append` per station (existing code, unchanged)
- Run Merge `--append` per station (existing code, pending `hdp-b1d.4` completion)
- Reuse pcluster flow:
  ```
  generate_station_list.py --network=ASOSAWOS
  generate_batch_script.py --network=ASOSAWOS --process=qaqc
  sbatch run_qaqc_ASOSAWOS.sh
  ```
- Set merge output to `s3://cadcat/hdp/` via `HDP_PUBLISH_BUCKET`/`HDP_PUBLISH_PREFIX`

### P1.6 — Validation + publish (~4h) ○ NOT STARTED
- For 5+ stations: plot pre/post timeseries at the Oct 2025 ISD→GHCNh boundary;
  assert no discontinuity in temperature, wind, pressure
- Run `scripts/tests/` validators against `s3://cadcat/hdp/` data
- Verify `data-access/` examples still work

### P1.7 — OtherISD (deferred, post-ASOSAWOS) ○ NOT STARTED
Same pivot as ASOSAWOS. After ASOSAWOS validates, apply `GHCNh_pull.py` and
`GHCNh_clean.py` to the OtherISD station list. GHCNh covers non-ASOS/AWOS ISD
stations via the same `USW`/`USC` ID namespace.

## Phase 2 — Automation (estimated ~22h, unchanged in architecture)

> Phase 2 architecture is unchanged. The only delta from the original plan: the
> pull stage in the Step Functions state machine calls `GHCNh_pull.py` instead
> of `ASOSAWOS_pullftp.py`/`OtherISD_pull.py` for ISD-derived networks.

### P2.1 — Containerize the pipeline (~8h)
- Build a single `Dockerfile` from `environment/` (uv base image).
- Three entrypoints (one image, different commands):
  - `run-pull --network=<X> --since=<DATE>` ← calls `GHCNh_pull.py` for ASOSAWOS/OtherISD
  - `run-qaqc --station=<ID>`
  - `run-merge --station=<ID>`
- Push to ECR.

### P2.2 — Staging buckets + IAM (~3h)
- `s3://hdp-staging-pull/{NETWORK}/` (mirrors `1_raw_wx`); 30-day lifecycle
- `s3://hdp-staging-qaqc/{NETWORK}/` (mirrors `3_qaqc_wx_v2`); 30-day lifecycle
- IAM role: read staging-pull, write staging-qaqc, write `cadcat/hdp`

### P2.3 — Batch compute environment (~3h)
- EC2 Spot, instance types `c7i-flex.large`, `c7g.large`, `m7i-flex.large`
- `MAXVCPUS=512`, `SPOT_PRICE_CAPACITY_OPTIMIZED`
- Job definitions: `hdp-pull-job` (1vCPU/2GB), `hdp-qaqc-job` (1vCPU/4GB, 2h timeout), `hdp-merge-job` (1vCPU/2GB, 1h timeout)

### P2.4 — Step Functions orchestration (~8h)
State machine stages:
1. **Pull-fanout**: Map over network families; each calls `GHCNh_pull.py` (ASOSAWOS/OtherISD) or existing pull (other networks). Output → `hdp-staging-pull`.
2. **Diff-stations**: Lambda lists modified prefixes, builds station array.
3. **Clean-fanout**: `GHCNh_clean.py --append` (ASOSAWOS/OtherISD) or existing cleaner.
4. **QAQC-fanout**: `MaxConcurrency=500`, one job per station → `hdp-staging-qaqc`.
5. **Merge-fanout**: One job per station → `s3://cadcat/hdp/`.
6. **Stationlist-update**: Single job runs `stnlist_update_*` for all networks.
7. **Notify**: SNS on success/failure.

### P2.5 — EventBridge schedule + observability (~3h)
- EventBridge: `cron(0 8 1 * ? *)` (1st of month, 08:00 UTC) → Step Functions
- CloudWatch dashboard: Batch success rate, Step Functions duration, `cadcat/hdp` object count delta
- SNS alerts on Step Functions failure or >5% station failure rate

## Cost Estimates (revised)

> ⚠️ Order-of-magnitude. GHCNh pull costs are HTTP GET from NCEI (free); no FTP
> egress costs. Parquet parsing is faster than ISD fixed-width gzip — expect
> clean stage wall time to decrease slightly.

### Phase 1 — One-time catch-up (revised)
| Item | Calc | Cost |
|---|---|---|
| GHCNh pull (HTTP, no compute cost) | — | **~$0** |
| Clean compute (1 r6i.2xlarge × ~3 days) | 8 vCPU × 72h × $0.10/vCPU-hr spot | **~$60** |
| QAQC compute | 15,064 × 30 min × $0.04/hr | **~$300** |
| Merge compute | 15,064 × 10 min × $0.04/hr | **~$100** |
| pcluster head node (1 month) | t3.medium | **~$30** |
| EBS / FSx for pcluster shared storage | 500 GB × 1 mo gp3 | **~$40** |
| S3 storage delta | ~100 GB-mo | **~$3** |
| **Phase 1 total** | | **≈ $530** |

### Phase 2 — Recurring monthly automation
| Item | Calc | Cost / month |
|---|---|---|
| GHCNh pull jobs (HTTP, Batch for orchestration) | 8 jobs × ~15 min × $0.04 | **~$0.10** |
| Clean jobs (Batch Spot) | ~10 jobs × ~1 hr × $0.04 | **~$0.50** |
| QAQC jobs (~10% of stations touched) | 1,500 × 30 min × $0.04 | **~$30** |
| Merge jobs | 1,500 × 10 min × $0.04 | **~$10** |
| Step Functions, Lambda, staging storage, CloudWatch | | **~$3** |
| **Phase 2 total per run** | | **≈ $45/month** |

## Relevant files

### Completed
- [scripts/paths.py](scripts/paths.py) — env-var driven ✓
- [scripts/1_pull_data/pull_asosawos_from_last_timestamps.py](scripts/1_pull_data/pull_asosawos_from_last_timestamps.py) — per-station pull orchestrator ✓ (needs GHCNh backend)
- [scripts/1_pull_data/ASOSAWOS_pullftp.py](scripts/1_pull_data/ASOSAWOS_pullftp.py) — ISD FTP pull (frozen at Oct 2025; keep for historical reference)
- [scripts/2_clean_data/ASOSAWOS_clean.py](scripts/2_clean_data/ASOSAWOS_clean.py) — `--append` mode ✓
- [scripts/3_qaqc_data/QAQC_run_for_single_station.py](scripts/3_qaqc_data/QAQC_run_for_single_station.py) — `--append` mode ✓
- [scripts/4_merge_data/MERGE_run_for_single_station.py](scripts/4_merge_data/MERGE_run_for_single_station.py) — `--append` mode ◐ (in progress)

### To be created
- `scripts/1_pull_data/GHCNh_pull.py` — **not yet written**; fetches Parquet from NCEI by station+year
- `scripts/2_clean_data/GHCNh_clean.py` — **not yet written**; GHCNh Parquet → HDP NetCDF

### Reference
- [CONTEXT.md](CONTEXT.md) — GHCNh technical reference (endpoints, format, variable map, station ID mapping, WECC coverage)
- [AGENTS.md](AGENTS.md) — operational gotchas including ISD freeze and GHCNh access summary
- [scripts/pcluster/](scripts/pcluster/) — SLURM batch scripts for Phase 1 catch-up
- [scripts/4_merge_data/MERGE_pipeline.py](scripts/4_merge_data/MERGE_pipeline.py) — hardcoded bucket reference resolved via `paths.py`

## Verification

1. **GHCNh ↔ ISD overlap check**: For a station with data in both (any year ≤2025), `GHCNh_clean.py` output should match `ASOSAWOS_clean.py` output within sensor precision.
2. **Append boundary**: Run append for a station with a known 30-day overlap; assert no duplicate time coordinates in final zarr.
3. **Phase 1 ASOSAWOS**: `aws s3 ls s3://cadcat/hdp/ASOSAWOS/ | wc -l` matches expected active station count.
4. **Timeseries continuity**: For 5+ stations, plot the Oct 2025 ISD→GHCNh handoff in the merged zarr; assert no jump or gap.
5. **Phase 2 container**: `docker run hdp:latest run-qaqc --station=CW3E_HDC` produces same zarr as a local run.
6. **Phase 2 state machine**: Manually trigger; assert all states green and station counts match.
7. **Phase 2 schedule**: Trigger via EventBridge once before letting cron own it; verify SNS alert with intentional failure.

## Decisions / scope
- **In scope**: GHCNh pull + clean scripts; append pipeline e2e; pcluster catch-up; Phase 2 Batch + Step Functions + CDK.
- **Excluded**: Changing QC algorithms; adding new networks; exact-timestamp pull granularity (deferred `hdp-1st`).
- **Cadence**: Monthly, locked by 45-day pull lag.
- **Climatology**: Option A — refit from full record on every monthly run. Revisit if cost exceeds budget after 2 production runs.
- **OtherISD**: Same GHCNh pivot as ASOSAWOS; deferred until ASOSAWOS POC validates.

## Further considerations
1. **GHCNh raw storage format**: Keep as Parquet in S3 (natural format), or re-encode to ISD `.gz` for pipeline compatibility? Recommend Parquet-native — simpler and faster.
2. **Station ID inventory maintenance**: GHCNh station list should be cached locally (or refreshed monthly) rather than hitting NCEI on every pull run. Consider a `scripts/1_pull_data/refresh_ghcnh_station_list.py` helper.
3. **Oct 2025 gap stitching**: Some stations may have ISD data through Aug 2025 (FTP) and GHCNh data from Jan 2024 onward (with overlap). The append dedup logic handles this correctly; document the expected overlap in the validation step.
4. **cadcat bucket policy**: Neil owns cross-account write permission. Required before Phase 1 merge outputs can land in `cadcat/hdp`.
