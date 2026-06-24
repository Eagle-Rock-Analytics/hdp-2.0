# HDP 2.0 — Phase 0 & Phase 1 Retrospective

> **Status:** Phase 0 (append-aware refactor) ✓ COMPLETE · Phase 1 ASOSAWOS catch-up ✓ COMPLETE
> **Date:** 2026-06-23
> **Author:** Engineering team (Eagle Rock Analytics)

---

## Table of Contents

1. [Background & Motivation](#1-background--motivation)
2. [What We Built](#2-what-we-built)
3. [Key Decisions](#3-key-decisions)
4. [Roadblocks Encountered](#4-roadblocks-encountered)
5. [Bugs Found and Fixed](#5-bugs-found-and-fixed)
6. [Nice-to-Haves for Future Work](#6-nice-to-haves-for-future-work)
7. [Phase 2 Outline](#7-phase-2-outline)

---

## 1. Background & Motivation

### What the platform does
The Historical Data Platform (HDP) collects, cleans, QA/QCs, and publishes hourly
weather observations from ~15,000 stations across the western US, spanning 27
distinct sensor networks. Processed output lands in `s3://cadcat/hdp/{NETWORK}/{STATION}.zarr`
and powers downstream climate analytics workflows.

### Why a v2 was needed
The v1 pipeline (`historical-obs-platform`) was designed as a one-time batch
processing system. Every run re-processed each station's full historical record
from scratch. This worked for the initial data ingestion, but became a problem when
we wanted to:

- **Run regularly** (monthly updates) — reprocessing 15,000 stations from scratch each
  time is prohibitively expensive (~$530 vs ~$45/month for incremental updates).
- **Handle upstream data freezes** — when NOAA ISD stopped being updated in October 2025
  (see [Section 4](#4-roadblocks-encountered)), the pull scripts needed to be
  replaced wholesale.

The v2 goal: a pipeline that can run in **append mode** (process only new timesteps
per station), supports monthly automation via AWS Batch + Step Functions, and uses
GHCNh as the upstream data source for ASOSAWOS and OtherISD stations.

---

## 2. What We Built

### Phase 0 — Append-aware refactor

The core architectural change: every pipeline stage gained an `--append` flag that
restricts processing to new timesteps only.

#### `paths.py` — env-var driven (ticket `hdp-b1d.1`)
Previously, all S3 bucket names were hardcoded literals scattered across pipeline
scripts. `scripts/paths.py` was rewritten so every bucket name and prefix is read
from an environment variable with the current value as the default:

| Env var | Default | Purpose |
|---|---|---|
| `HDP_BUCKET` | `wecc-historical-wx` | Staging / intermediate data |
| `HDP_PUBLISH_BUCKET` | `cadcat` | Published output |
| `HDP_PUBLISH_PREFIX` | `hdp` | Prefix within publish bucket |

This enables testing against a sandboxed bucket (`auto-hdp`) without touching
production data, and will support the Phase 2 staging buckets.

#### Clean append mode (`hdp-b1d.2`, `hdp-h1x`)
`ASOSAWOS_clean.py --append` reads the per-station last timestamp, fetches only
the new raw slice, applies the same standardization (unit conversions, variable
renaming, sub-hourly deduplication), and writes output to a staging key:
`2_clean_wx/{NETWORK}/_append/{STATION}.nc`. The existing full-history clean file
is left untouched, so a parallel full re-run remains safe.

#### QAQC append mode (`hdp-b1d.3`, `hdp-gmy`)
`QAQC_run_for_single_station.py --append` reads from the `_append/` staging key,
applies all 10+ QA/QC checks to only the new slice, and writes the flagged output
to the QAQC staging key. **Climatological checks** (`qaqc_climatological_outlier`,
`qaqc_unusual_gaps`) still read the full station zarr to fit distributions but
only write flags for the new rows.

#### Merge append mode (`hdp-b1d.4`)
The merge stage is the most complex because it must integrate the new slice into
an existing zarr:

1. Load the new QAQC'd slice from the staging key.
2. Open the existing baseline zarr from the publish bucket.
3. `xr.concat([baseline, new_slice], dim="time")`.
4. Deduplicate on time: `drop_duplicates(dim="time", keep="last")`. The `keep="last"`
   policy preserves the new-slice row on any overlap, so upstream source revisions
   are absorbed.
5. Sort by time, write back to the publish bucket.

This strategy (called **dedup-after-concat**) was chosen over filter-before-concat
because it handles ambiguous timestamp boundaries without risking data loss.

#### Per-station discovery and pull orchestration (`hdp-b1d.6`, `hdp-b1d.7`)
`discover_last_timestamps_asosawos.py` opens each station zarr, reads the last time
coordinate, and writes a per-station boundary CSV. The pull orchestrator
`pull_asosawos_from_last_timestamps.py` uses these boundaries to fetch only new
data, then routes through the clean → QAQC → merge append pipeline.

---

### Phase 1 — GHCNh Integration and ASOSAWOS Catch-up

See [Section 4](#4-roadblocks-encountered) for why GHCNh became necessary.

#### `GHCNh_pull.py` (`hdp-b1d.12`)
Fetches Parquet files from the NCEI Ceph object store by station and year:
```
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
```
Design decisions:
- **Skip-existing** via `head_object` — idempotent re-runs are safe.
- **3-retry** with exponential backoff on HTTP 503/504.
- **Graceful 404 handling** — some stations have no data for certain years (station
  not yet active, or decommissioned). These are logged but do not fail the job.
- **Store as Parquet in S3** (not re-encoded to ISD `.gz`) — avoids a round-trip
  format conversion and keeps the raw data in its native format for inspection.
- Ships with 17 unit tests.

#### `GHCNh_clean.py` (`hdp-b1d.13`)
Converts GHCNh Parquet → HDP NetCDF (18 variables). Key behaviors:
- Sub-hourly observations (METAR cadence, typically at :56 past the hour) are
  deduplicated by keeping the **last observation per floor-hour**.
- GHCNh temperature is in °C; HDP standard is Kelvin — conversion applied.
- Output schema is identical to `ASOSAWOS_clean.py` so all downstream stages are
  unaware of the upstream source change.
- Station ID mapping: `ASOSAWOS_72630014733` → `USW00014733` (strip `ASOSAWOS_`,
  take last 5 digits as WBAN, prefix with `USW` zero-padded to 8 digits).
- Ships with 22 unit tests.

#### GHCNh wired into pull orchestrator (`hdp-b1d.14`)
`pull_asosawos_from_last_timestamps.py` gained `--backend {ghcnh,isd}` (default:
`ghcnh`). The ISD FTP backend is preserved for historical reference and any stations
that may still be accessible there.

#### ASOSAWOS full catch-up (P1.4 → P1.5b)
All 455 ASOSAWOS stations pulled (2022–2026 Parquet to `wecc-historical-wx`), then
processed through clean + QAQC + merge on the pcluster SLURM cluster:

- **446/455 stations** completed clean + QAQC (9 were deactivated pre-2022, expected failures).
- **429 deduplicated zarrs** written to `s3://auto-hdp/hdp/ASOSAWOS/`:
  - 409 `full` record zarrs (pre-2022 baseline + 2022–2026 appended).
  - 18 `baseline_only` zarrs (station deactivated; no new data, baseline preserved).
- **WBAN deduplication**: 20 `append_only` zarrs were WBAN collision duplicates (see
  [Section 4](#4-roadblocks-encountered)). All 20 were deleted; `ASOSAWOS_72074924255`
  (Whidbey Island NAS, WBAN=24255) was manually promoted to full coverage before removal.
- Manifests saved to `temp/asosawos_merge_manifest.csv` and
  `temp/asosawos_append_only_investigation.csv`.

---

## 3. Key Decisions

### D1 — GHCNh over ISD (and Parquet-native storage)
**Decision:** Migrate ASOSAWOS and OtherISD pulls to GHCNh. Store raw data as
Parquet in S3 rather than re-encoding to ISD `.gz`.
**Rationale:** ISD is frozen (Section 4). GHCNh is the NOAA-designated replacement
with active maintenance and a full historical record back to 1718. Parquet is the
native GHCNh format — storing it natively avoids a lossy round-trip and simplifies
inspection. The output schema of `GHCNh_clean.py` is identical to `ASOSAWOS_clean.py`,
so no downstream changes were needed.
**Trade-off considered:** Re-encoding to ISD `.gz` would have allowed reuse of
`ASOSAWOS_clean.py` unchanged, but would have added format conversion complexity
and obscured provenance.

### D2 — dedup-after-concat (not filter-before-concat)
**Decision:** In merge append mode, concatenate the full new slice with the existing
zarr and then deduplicate, rather than filtering the new slice to exclude existing timestamps.
**Rationale:** Per-station last timestamps are floor-hour aligned, but real data
boundaries are fuzzy (METAR obs at :56, ISD/GHCNh transition creating a ~1-year
overlap). Filter-before-concat risks dropping valid new observations in the overlap
window. dedup-after-concat with `keep="last"` safely absorbs the overlap and ensures
upstream source revisions are applied.

### D3 — Test bucket (`auto-hdp`) before production (`cadcat`)
**Decision:** All Phase 1 processing targets `s3://auto-hdp/hdp/ASOSAWOS/` until
P1.6 validation passes. Production `s3://cadcat/hdp/` is only written after explicit
sign-off.
**Rationale:** The merge append bug (Section 5, "self-overwrite NaN") silently
produced 100% NaN for pre-2022 data on the first pcluster run. Running in a test
bucket first meant no production data was harmed. This pattern — test bucket →
validate → publish — should be standard for all future catch-up runs.

### D4 — Climatological refit on every monthly run (Option A)
**Decision:** Re-fit climatological distributions from the full station record on
every monthly automation run, rather than caching distributions and applying a delta.
**Rationale:** Simplicity. The station record grows by ~720 hours/month — the
marginal computational cost of refitting is small relative to the I/O cost of the
merge stage. Re-evaluate if cost exceeds budget after 2 production runs.
**Alternative considered (Option B):** Cache fitted distributions in metadata zarr
attributes; only refit annually. Rejected: adds state management complexity and
requires a migration path for existing zarrs.

### D5 — Biweekly pull cadence with 45-day lag
**Decision:** Monthly automation runs with a 45-day lookback lag, consistent with
the existing `update_pull.py` design.
**Rationale:** Source networks finalize observations on a 30–45 day delay. A shorter
lag risks ingesting preliminary data that gets revised. The 45-day lag is intentional
and should not be shortened without validating source data finalization timelines.

### D6 — WBAN as the station deduplication key
**Decision:** When multiple USAF codes map to the same WBAN (co-located sensors),
keep the zarr with the longest pre-2022 historical record and delete the others.
**Rationale:** GHCNh merges co-located sensors under one WBAN-based ID, so the
pipeline can only produce one zarr per WBAN after GHCNh migration. The station with
pre-2022 history contains the most valuable long-record data.

### D7 — pcluster SLURM for Phase 1 catch-up (not Batch)
**Decision:** Use AWS ParallelCluster (SLURM) for the one-time Phase 1 catch-up,
not the Phase 2 AWS Batch infrastructure.
**Rationale:** pcluster was already set up and the team had operational experience
with it. Standing up Batch/Step Functions purely for Phase 1 would have added 8+
hours of infrastructure work for a one-time run. Phase 2 will use Batch going forward.

---

## 4. Roadblocks Encountered

### R1 — ISD data freeze (October 2025) [BLOCKING]
**What happened:** NOAA stopped updating both ISD access paths in October 2025 —
no data is available after that date from either the FTP (`ftp.ncdc.noaa.gov`) or
HTTPS (`ncei.noaa.gov/data/global-hourly/`) endpoints. This was discovered when
Phase 1 pull jobs returned empty results for 2026.
**Root cause:** Attributed to NOAA staffing and budget reductions; not a temporary
outage.
**Resolution:** Full migration to GHCNh (`GHCNh_pull.py`, `GHCNh_clean.py`). Added
~2 weeks of development time.
**Lesson:** Monitor upstream data sources proactively; add a freshness check to
the pull validation test suite so silent data freeze is caught early.

### R2 — WBAN collision duplicates
**What happened:** After the pcluster merge run, 20 zarrs in the test bucket were
classified as `append_only` (containing only 2022+ data with no pre-2022 baseline).
Investigation revealed these were WBAN collisions: multiple USAF codes sharing the
same 5-digit WBAN. Since GHCNh IDs are WBAN-based, all USAF variants of a station
map to the same GHCNh file, and the merge stage created a separate zarr for each
USAF code.
**Resolution:** Identified 20 collision pairs. All 20 `append_only` zarrs had a
corresponding `full`-record zarr under a different USAF code. Deleted the duplicates.
`ASOSAWOS_72074924255` (Whidbey Island NAS, WBAN=24255) was an edge case where
both USAF codes were `append_only` — the station had to be manually rebuilt
(clean → QAQC → merge from its original ISD baseline) before deduplication.
**Lesson:** The station deduplication logic should be automated before OtherISD
migration and Phase 2, where the same collision pattern will recur across a larger
station set.

### R3 — `merge_hourly_standardization` TypeError on NaN
**What happened:** During merge of certain stations, `merge_hourly_standardization`
raised a `TypeError` when joining QC flags. The bug: a float `NaN` value was being
passed into a string join operation without type coercion.
**Resolution:** Filter float NaN values before joining QC flags; NaN-only columns
return `'nan'` string. Fixed in `hdp-8fr`.

### R4 — pandas 2.x deprecation: `freq='M'` → `'ME'`
**What happened:** `qaqc_unusual_large_jumps.py` used `freq='M'` in a
`pd.Grouper`, which raises a `FutureWarning` (and will eventually become an error)
in pandas 2.x. The correct spelling is `'ME'` (month-end).
**Resolution:** One-line fix. Added to pre-commit checks.

### R5 — Ambiguous Series truth value in QAQC
**What happened:** `qaqc_wholestation.py` used `not df.isnull()` on a pandas
Series, which raises `ValueError: The truth value of a Series is ambiguous`.
**Resolution:** Changed to `~df.isnull()` (element-wise NOT). Standard pandas
gotcha.

### R6 — S3 streaming body needs BytesIO wrapper
**What happened:** `GHCNh_clean.py` attempted to pass an S3 streaming response
body directly to `pd.read_parquet()`. Parquet readers require seekable file-like
objects; S3 streaming bodies are not seekable.
**Resolution:** Wrapped the streaming body in `io.BytesIO()` before passing to
`read_parquet`. Added to coding conventions for all future S3-read patterns.

### R7 — pcluster NFS station-ID collision in temp files
**What happened:** `GHCNh_clean.py` was writing a single shared temp file
(`temp_ghcnh.nc`) when processing on the NFS-shared pcluster filesystem. Multiple
parallel SLURM tasks overwrote each other's temp files, producing corrupt NetCDF
output.
**Resolution:** Changed temp file name to include the station ID:
`temp_ghcnh_{station_id}.nc`. All SLURM array tasks now write to unique paths.

### R8 — Stale zarr chunk encoding on re-open
**What happened:** `xr.open_zarr` attaches encoding metadata (chunk sizes, compressors)
to variables. When the opened dataset is later passed to `to_zarr`, the stale encoding
conflicts with the new write parameters and raises a `ValueError`.
**Resolution:** Clear all variable and coordinate encodings explicitly before calling
`to_zarr` in append mode. Pattern:
```python
for var in ds.data_vars:
    ds[var].encoding.clear()
for coord in ds.coords:
    ds[coord].encoding.clear()
```

---

## 5. Bugs Found and Fixed

| Bug | File | Severity | Commit | Description |
|-----|------|----------|--------|-------------|
| **Self-overwrite NaN** | `MERGE_pipeline.py` | 🔴 Critical | `4ea8bc7` | Lazy `xr.concat` referenced the zarr being deleted — `ds.compute()` must be called before `fs.rm()` in `write_zarr_to_s3`. Produced 100% NaN for all pre-2022 data. |
| `merge_hourly_standardization` TypeError | `merge_hourly_standardization.py` | 🟠 High | `hdp-8fr` | Float NaN not coerced before string join of QC flags. |
| `QAQC_pipeline.py` `qaqc_source` processing | `QAQC_pipeline.py` | 🟡 Medium | — | String variable `qaqc_source` was being passed through QAQC checks; excluded following `qaqc_process` pattern. |
| `freq='M'` deprecation | `qaqc_unusual_large_jumps.py` | 🟡 Medium | — | pandas 2.x requires `'ME'` for month-end frequency. |
| `not df.isnull()` on Series | `qaqc_wholestation.py` | 🟡 Medium | — | Ambiguous Series truth value; changed to `~df.isnull()`. |
| GHCNh S3 BytesIO | `GHCNh_clean.py` | 🟡 Medium | — | Non-seekable S3 streaming body passed to `read_parquet`; wrapped in `BytesIO`. |
| pcluster NFS temp collision | `GHCNh_clean.py` | 🟡 Medium | — | Single shared temp file overwritten by parallel tasks; station-specific filename used. |
| Stale zarr encoding | `MERGE_pipeline.py` | 🟡 Medium | — | `xr.open_zarr` encoding metadata must be cleared before `to_zarr` in append mode. |

> **The self-overwrite NaN bug is the most important one to be aware of.** It is
> silent — no error is raised, the zarr writes successfully, but all pre-existing
> data becomes NaN. The fix (`ds.compute()` before `fs.rm()`) is in place and must
> not be reverted.

---

## 6. Nice-to-Haves for Future Work

### N1 — OtherISD migration to GHCNh
OtherISD stations (non-ASOS/AWOS ISD stations) use the same FTP pull path that is
now frozen. They need the same `GHCNh_pull.py` + `GHCNh_clean.py` treatment as
ASOSAWOS. The station ID mapping is slightly different (`USC` prefix instead of `USW`
for non-WBAN stations), but the approach is identical.
**Scope estimate:** 1–2 days. Deferred until ASOSAWOS P1.6 validation passes.

### N2 — Automated WBAN deduplication
The WBAN collision problem (R2) will recur when OtherISD is migrated and on any
future station-list refresh. The current process is manual. A deduplication script
that identifies USAF-code clusters sharing a WBAN, picks the canonical station
(longest historical record), and logs the deletions would prevent hours of manual
investigation.

### N3 — Exact-timestamp pull boundaries (issue `hdp-1st`)
The current pull orchestrator uses year-granular boundaries (pull all years ≥ year
of last timestamp). This means each incremental run re-fetches up to 12 months of
Parquet files. For large station counts, this generates unnecessary S3 GET requests
and network egress. The fix: store the exact per-station last timestamp (ISO 8601)
alongside the pull boundary CSV and pass it as a `--since` argument to
`GHCNh_pull.py`.
**Scope estimate:** 0.5 days.

### N4 — GHCNh station inventory cache
`GHCNh_pull.py` does not maintain a local cache of the GHCNh station list. Every
pull run relies on the NCEI endpoint being available. A monthly refresh of
`ghcnh-station-list.txt` (serialized to `data/ghcnh_station_list.csv`) would:
- Enable offline validation of station ID mappings.
- Support proactive detection of newly decommissioned stations.
- Reduce dependency on NCEI Ceph availability during pull jobs.

### N5 — Staging bucket env-var gap (issue `hdp-05b`)
`scripts/paths.py` exposes `HDP_BUCKET` and `HDP_PUBLISH_BUCKET` as env vars, but
the QAQC and clean stages always write intermediate files to the hardcoded bucket
`wecc-historical-wx`, not to `HDP_BUCKET`. This means testing with a fully isolated
staging bucket requires manual path overrides. The fix: propagate `HDP_BUCKET` into
all intermediate write paths in `QAQC_pipeline.py` and `ASOSAWOS_clean.py`.

### N6 — Timeseries continuity validation notebook
P1.6 (publish to `cadcat`) requires a validation pass: plot 5+ stations at the
Oct 2025 ISD→GHCNh boundary to confirm no discontinuity in temperature, wind, and
pressure. This notebook should be committed to `notebooks/` and run as part of
every future catch-up cycle. Currently, it does not exist.

### N7 — Non-ASOSAWOS network catch-up
The other 26 networks (HADS, CIMIS, CW3E, MADIS, SCAN/SNOTEL, MARITIME, etc.)
have not received a catch-up run under the append refactor. Most use non-ISD data
sources and are not affected by the ISD freeze, but their zarrs may be 3–12 months
stale depending on when v1 last ran. Scope: network-by-network assessment and batch
catch-up run (pcluster or Phase 2 Batch).

### N8 — Data confidence score refresh
`notebooks/data_confidence_calculation.ipynb` computes a per-station quality score
from QAQC flag statistics. This has not been recomputed since the GHCNh-sourced
QAQC flags were added. A scheduled refresh in the Phase 2 pipeline (post-merge,
per-network) would keep the confidence scores current.

### N9 — `cadcat` cross-account write policy
Phase 1 publish to `s3://cadcat/hdp/` requires a bucket policy granting the pipeline
IAM role `s3:PutObject` cross-account. This has not been set up — Neil needs to
approve and apply it before P1.6 can complete. Document the required policy as part
of the Phase 2 CDK `BucketsStack`.

---

## 7. Phase 2 Outline

See [phase2-architecture-guide.md](phase2-architecture-guide.md) for the full
implementation guide.

Phase 2 replaces the manual pcluster catch-up workflow with a fully automated,
event-driven pipeline on AWS managed services:

| Component | Service |
|---|---|
| Compute | AWS Batch (EC2 Spot) |
| Orchestration | AWS Step Functions |
| Schedule | Amazon EventBridge (monthly cron) |
| Alerting | Amazon SNS + CloudWatch |
| Infrastructure | AWS CDK (Python) |

**Monthly run cost estimate:** ~$45/month (vs $530 for the one-time catch-up).

**Trigger:** `cron(0 8 1 * ? *)` — 1st of each month at 08:00 UTC.

The Step Functions state machine covers: pull fanout → diff stations → clean fanout →
QAQC fanout → merge fanout → station list update → SNS notification.
