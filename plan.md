# Plan: Catch-up + Automate Historical Obs Platform (v2 — append-aware)

> **Living document** — last updated 2026-06-23. Phase 0 complete. Phase 1 ASOSAWOS
> catch-up complete in test bucket (`auto-hdp/hdp/ASOSAWOS/`, 429 deduplicated zarrs).
> Next gate: P1.6 validation + publish to `cadcat/hdp/`.

---

## Current Status (as of 2026-06-23)

### Phase 0 — Append-aware refactor: COMPLETE ✓
### Phase 1 (ASOSAWOS) — Catch-up to test bucket: COMPLETE ✓

**Done:**
- `paths.py` fully env-var driven ✓ `hdp-b1d.1`
- `ASOSAWOS_clean.py --append` mode ✓ `hdp-b1d.2`, `hdp-h1x`
- QAQC `--append` mode ✓ `hdp-b1d.3`, `hdp-gmy`
- Test bucket provisioned (`auto-hdp/hdp/ASOSAWOS/`) ✓ `hdp-b1d.5`
- Per-station last-timestamp discovery (`discover_last_timestamps_asosawos.py`) ✓ `hdp-b1d.6`
- GHCNh pull (`GHCNh_pull.py`): Parquet fetch from NCEI by station+year, S3 upload, skip-existing, retry ✓ `hdp-b1d.12`
- GHCNh clean (`GHCNh_clean.py --append`): Parquet → HDP NetCDF, 18 vars, unit conversions ✓ `hdp-b1d.13`
- GHCNh wired into pull orchestrator (`--backend ghcnh` default) ✓ `hdp-b1d.14`
- Raw pull validated: SFO (USW00014733) 2026: 1,117 KB, 3,829 hourly obs; orchestrator dry-run 25-station clean ✓ `hdp-b1d.7`
- Clean append validated: `ASOSAWOS_72020200118` 38,196 obs, 18 vars, 2022-09 → 2026-06 ✓ `hdp-b1d.8`
- QAQC append validated: same station, 38,196 obs, sfcWind_dir flagged 38.96% ✓ `hdp-b1d.9`
- `merge_hourly_standardization` TypeError fixed ✓ `hdp-8fr`
- Merge `--append` mode: e2e verified; 2005-01-03 → 2026-06-09, 187,847 timesteps, 0 gaps > 1h ✓ `hdp-b1d.4`
- **P1.4**: GHCNh pull complete — all 455 stations, 2022–2026 Parquet in `1_raw_wx/ASOSAWOS/` ✓
- **P1.5a**: Clean + QAQC append complete — 446/455 stations (9 deactivated pre-2022, expected) ✓
- **P1.5b**: Merge append complete — 429 deduplicated zarrs in `auto-hdp/hdp/ASOSAWOS/` ✓
  - Pre/post validation: pre-2022 tas NaN% matches baseline (~0.1–47% per station quality)
  - `ASOSAWOS_72074924255` (Whidbey Island NAS) manually updated: baseline_only → full (1980–2026)
  - 20 WBAN-collision duplicates removed (all were `append_only` with a `full` counterpart under a different USAF code)
  - Manifest saved to `temp/asosawos_merge_manifest.csv`

**Open:**
- `hdp-b1d.11`: Validate ASOSAWOS timeseries continuity + publish to `cadcat/hdp/` (P1.6)

**Deferred:**
- `hdp-1st`: Exact per-station pull timestamp boundaries (currently year-granular); P2

**Bugs fixed this sprint (2026-06-19 → 2026-06-23):**
- `GHCNh_clean.py`: BytesIO wrapper for S3 streaming body; correct `_append/` path convention
- `GHCNh_clean.py`: station-specific temp file (`temp_ghcnh_{station_id}.nc`) to prevent parallel NFS collision
- `QAQC_pipeline.py`: `qaqc_source` string variable excluded from QAQC processing (follows `qaqc_process` pattern)
- `qaqc_unusual_large_jumps.py`: `freq='M'` → `'ME'` (pandas 2.x deprecation)
- `qaqc_wholestation.py`: `not df.isnull()` → `~df.isnull()` (ambiguous Series truth value)
- `merge_hourly_standardization.py`: filter float NaN before joining QC flags
- `MERGE_pipeline.py`: clear stale zarr chunk encoding before `to_zarr` in append mode
- `MERGE_pipeline.py`: **self-overwrite NaN bug** — `ds.compute()` before `fs.rm()` in `write_zarr_to_s3`; lazy concat referenced the same zarr being deleted, silently producing 100% NaN for pre-2022 data

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
- **Phase 0 — Append-aware refactor (COMPLETE ✓)**: `--append` mode validated e2e across clean/QAQC/merge. GHCNh scripts written and validated. pcluster batch scripts ready for catch-up. Only remaining: full-station merge batch run (`hdp-b1d.10`), blocked on QAQC catch-up.
- **Phase 1 — GHCNh integration + catch-up (~30h revised)**: `GHCNh_pull.py`, `GHCNh_clean.py`, and orchestrator wiring are done. Remaining: pcluster catch-up run, publish to `cadcat/hdp/`, OtherISD.
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

### P0.4 — Merge `--append` mode ✓ DONE (`hdp-b1d.4`)
Core implementation done (dedup-after-concat, baseline fallback, skip eraqc CSV
overwrites in append mode). Bug fixed: stale zarr encoding from `xr.open_zarr`
caused `ValueError` on `to_zarr`; clear all variable/coordinate encodings before
write. Verified on `ASOSAWOS_72020200118`: 2005-01-03 → 2026-06-09, 187,847
timesteps, 0 gaps > 1h, 337 obs around Oct 2025 ISD→GHCNh boundary.

### P0.5 — Per-station discovery + pull orchestrator ◐ IN PROGRESS (`hdp-b1d.6`, `hdp-b1d.7`)
- `discover_last_timestamps_asosawos.py` ✓ done
- `pull_asosawos_from_last_timestamps.py` ✓ implemented + tested; **blocked by ISD freeze**

### P0.6 — Fix `merge_hourly_standardization` TypeError ✓ DONE (`hdp-8fr`)
Fixed: filter float NaN before joining QC flags; NaN-only returns `'nan'`.

### P0.7 — Remaining append e2e ✓ DONE
- `hdp-b1d.8` ✓: GHCNh clean append validated on `ASOSAWOS_72020200118`
- `hdp-b1d.9` ✓: QAQC append validated on same station
- `hdp-b1d.4` ✓: Merge append validated e2e (see P0.4)
- `hdp-b1d.10` ◐: pcluster batch script ready; pending QAQC catch-up for all 455 stations
- `hdp-b1d.11`: Validate ASOSAWOS timeseries continuity in test bucket (P2, post hdp-b1d.10)

---

## Phase 1 — GHCNh Integration + Catch-up (revised, ~30h)

> **Revised from original.** The original P1.2 (ISD FTP pull 2022→2026) is no
> longer feasible — ISD is frozen past Oct 2025. ASOSAWOS and OtherISD must
> migrate to GHCNh. Non-ISD networks (HADS, CIMIS, CW3E, MADIS, etc.) are
> unaffected.

### P1.1 — Write `GHCNh_pull.py` ✓ DONE (`hdp-b1d.12`)
`scripts/1_pull_data/GHCNh_pull.py`. Fetches per-station per-year Parquet from NCEI
Ceph endpoint. Handles 404 (station absent for year) gracefully, 3-retry on 503/504,
skip-existing via `head_object`. Output: `1_raw_wx/ASOSAWOS/{STATION}/GHCNh_{GHCNH_ID}_{YEAR}.parquet`.
Design decision: **store as Parquet** (not re-encoded to ISD `.gz`). 17 unit tests.

### P1.2 — Write `GHCNh_clean.py` ✓ DONE (`hdp-b1d.13`)
`scripts/2_clean_data/GHCNh_clean.py`. Reads GHCNh Parquet → HDP NetCDF (18 vars).
Unit conversions applied. Sub-hourly handling: last obs per floor-hour. Output
schema identical to `ASOSAWOS_clean.py`. Supports `--append`. 22 unit tests.

### P1.3 — Wire GHCNh into append pipeline ✓ DONE (`hdp-b1d.14`)
`pull_asosawos_from_last_timestamps.py` now uses `--backend ghcnh` (default).
ISD FTP preserved as `--backend isd`. OtherISD deferred to post-ASOSAWOS validation.

### P1.4 — Catch-up pull: ASOSAWOS via GHCNh ✓ DONE
- All 455 stations pulled, 2022–2026 Parquet in `s3://wecc-historical-wx/1_raw_wx/ASOSAWOS/`

### P1.5 — Clean/QAQC/Merge catch-up on pcluster ✓ DONE
- Clean + QAQC: 446/455 stations (9 deactivated = expected failures)
- Merge (pcluster job 2704): 429 deduplicated zarrs written to `s3://auto-hdp/hdp/ASOSAWOS/`
- **WBAN deduplication**: Post-merge, found 20 `append_only` zarrs were WBAN collisions
  (multiple USAF codes sharing the same physical station/WBAN). All 20 had a `full`-record
  counterpart already in the bucket. `ASOSAWOS_72074924255` (Whidbey Island NAS, WBAN=24255)
  was manually completed (clean → QAQC → merge) before removing its 2 duplicates.
  All 20 duplicates deleted. Final count: **429 zarrs** (`full`: 409, `baseline_only`: 18,
  2 log subdirs excluded).
- Manifest: `temp/asosawos_merge_manifest.csv`
- Manifest — investigation of collisions: `temp/asosawos_append_only_investigation.csv`

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
- [scripts/4_merge_data/MERGE_run_for_single_station.py](scripts/4_merge_data/MERGE_run_for_single_station.py) — `--append` mode ✓
- [scripts/pcluster/run_merge_append_template.sh](scripts/pcluster/run_merge_append_template.sh) — pcluster merge append template ✓
- [scripts/pcluster/run_merge_append_ASOSAWOS.sh](scripts/pcluster/run_merge_append_ASOSAWOS.sh) — 455-task array job, ready to sbatch ✓
- [scripts/pcluster/stations_input/ASOSAWOS-input.dat](scripts/pcluster/stations_input/ASOSAWOS-input.dat) — 455 station list ✓

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
