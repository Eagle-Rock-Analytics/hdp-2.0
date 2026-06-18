# HDP 2.0 — Domain Glossary

## append mode
A pipeline execution mode (`--append` flag) in which a stage processes only the new time-slice for a station — timestamps strictly after the station's last output timestamp — rather than reprocessing the full historical record. The merge stage implements this as: run pipeline on new slice → `xr.concat` with existing zarr → dedup on time coordinate → write back. Contrast: **full-overwrite mode** (existing behavior, `mode="w"` with no slice filtering).

## baseline zarr
The existing merged station zarr in the publish bucket that serves as the starting point for an append operation. For the ASOSAWOS POC, the baseline is copied from `s3://cadcat/hdp/ASOSAWOS/` into the test bucket before any append work begins.

## test bucket
A sandboxed S3 bucket (e.g. `hdp-test-asosawos`) holding a copy of the production ASOSAWOS baseline zarrs. All Phase 1 append operations target this bucket, not `cadcat`. Protects production data during development.

## per-station last timestamp
The last time coordinate in a station's baseline zarr, read at the start of each append run. Used as `start_date` for the raw data pull for that station. Varies per station — active stations have recent timestamps; inactive stations may be years behind. Discovered by opening each zarr and reading `ds.time.values[-1]`.

## dedup-after-concat
The chosen conflict resolution strategy for the merge append: after `xr.concat(existing, new_slice)`, call `drop_duplicates` on the time dimension. Ensures no data loss when the new slice overlaps with the baseline (common due to the 45-day pull lag). Contrast: *filter-before-concat* (discarded — risks missing data at ambiguous boundaries).

## touched station
An ASOSAWOS station that has new raw files in S3 since its per-station last timestamp. Only touched stations are run through clean/QAQC/merge in an append run. Determined by comparing raw file `last_modified` dates against per-station last timestamps.

## 45-day lag
Intentional delay in `update_pull.py` — raw data is only pulled up to `today - 45 days` to ensure data finalization from source networks. Means per-station last timestamps and pull boundaries will never reach the present day.

## clean new-slice
The `.nc` output produced by `ASOSAWOS_clean.py --append` for a single station. Contains only the rows strictly after the boundary timestamp `T` (either `--start-date` or the per-station last timestamp from the baseline zarr). Written to the **staging clean key** rather than the full-history clean key, so the full-history `.nc` is preserved unmodified.

## staging clean key
The S3 path where a clean new-slice is written during an append run:
`s3://{HDP_BUCKET}/2_clean_wx/{NETWORK}/_append/{STATION}.nc`
The `_append` subprefix (constant `CLEAN_APPEND` in `scripts/paths.py`) is a cross-stage contract: the QAQC append stage must read from this key, not from the full-history clean key. Kept separate to allow full-history re-runs without stomping in-flight append slices.
