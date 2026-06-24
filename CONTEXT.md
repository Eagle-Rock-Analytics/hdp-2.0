# HDP 2.0 — Domain Glossary

## append mode
A pipeline execution mode (`--append` flag) in which a stage processes only the new time-slice for a station — from the start of the hour containing the station's last output timestamp onward — rather than reprocessing the full historical record. The merge stage implements this as: run pipeline on new slice → `xr.concat` with existing zarr → dedup on time coordinate (`keep="last"`) → write back. Contrast: **full-overwrite mode** (existing behavior, `mode="w"` with no slice filtering).

## baseline zarr
The existing merged station zarr in the publish bucket that serves as the starting point for an append operation. For the ASOSAWOS POC, the baseline is copied from `s3://cadcat/hdp/ASOSAWOS/` into the test bucket before any append work begins.

## test bucket
A sandboxed S3 bucket (e.g. `hdp-test-asosawos`) holding a copy of the production ASOSAWOS baseline zarrs. All Phase 1 append operations target this bucket, not `cadcat`. Protects production data during development.

## private publish target
A non-public S3 bucket or prefix that holds the authoritative merged dataset for
automation runs while HDP output remains private. It serves as both the append
baseline read location and the merge write target. It is durable storage, not an
ephemeral staging area.

## per-station last timestamp
The last time coordinate in a station's baseline zarr, read at the start of each append run. Used as `start_date` for the raw data pull for that station. Varies per station — active stations have recent timestamps; inactive stations may be years behind. Discovered by opening each zarr and reading `ds.time.values[-1]`.

## dedup-after-concat
The chosen conflict resolution strategy for the merge append: after `xr.concat(existing, new_slice)`, call `drop_duplicates` on the time dimension with `keep="last"` and sort by time. This keeps the new-slice row on overlaps so append runs absorb upstream source revisions while preserving all non-overlapping baseline rows. Contrast: *filter-before-concat* (discarded — risks missing data at ambiguous boundaries).

## touched station
An ASOSAWOS station that has new raw files in S3 since its per-station last timestamp. Only touched stations are run through clean/QAQC/merge in an append run. Determined by comparing raw file `last_modified` dates against per-station last timestamps.

## 45-day lag
Intentional delay in `update_pull.py` — raw data is only pulled up to `today - 45 days` to ensure data finalization from source networks. Means per-station last timestamps and pull boundaries will never reach the present day.

## clean new-slice
The `.nc` output produced by `ASOSAWOS_clean.py --append` for a single station. Contains rows from the start of the hour containing boundary timestamp `T` onward (where `T` is either `--start-date` or the per-station last timestamp from the baseline zarr). Written to the **staging clean key** rather than the full-history clean key, so the full-history `.nc` is preserved unmodified.

## staging clean key
The S3 path where a clean new-slice is written during an append run:
`s3://{HDP_BUCKET}/2_clean_wx/{NETWORK}/_append/{STATION}.nc`
The `_append` subprefix (constant `CLEAN_APPEND` in `scripts/paths.py`) is a cross-stage contract: the QAQC append stage must read from this key, not from the full-history clean key. Kept separate to allow full-history re-runs without stomping in-flight append slices.

## no-op run
A scheduled automation run that completes without processing any stations because
the pull and diff stages found zero touched stations. This is considered a
successful outcome when source freshness remains within the expected lag window.

---

# GHCNh — ISD Successor Dataset

## ISD freeze (October 2025)
As of early October 2025, NOAA stopped updating both the ISD FTP
(`ftp.ncdc.noaa.gov/pub/data/noaa/`) and the global-hourly HTTPS archive
(`https://www.ncei.noaa.gov/data/global-hourly/`). The FTP `2025/` directory was
last modified **2025-08-29**; the HTTPS `access/2025/` directory was last modified
**2025-10-02**. No `2026/` directory exists on either endpoint. This is the cause
of HDP's data cutoff at early October 2025. The freeze is attributed to NOAA
staffing/budget reductions.

## GHCNh (Global Historical Climatology Network — Hourly)
NOAA's official **replacement for ISD**, announced and documented at
`https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly`.
GHCNh harmonises the same underlying source observations as ISD into a cleaned,
station-centric format and is actively maintained and updated. It is the correct
long-term upstream source for ASOSAWOS and OtherISD pipeline data.

**Historical depth:** global dataset spans **1718–2026**. For a representative
WECC ASOS station (SFO, USW00023234), continuous hourly coverage runs from
**1932 through June 2026** (data refreshed every ~10 days).

## GHCNh access endpoints
GHCNh is served from two locations with different year coverage:

| Endpoint | Years available | Notes |
|---|---|---|
| NCEI Ceph object store (primary) | 1718–2026 | Full historical record; Ceph/RadosGW, not AWS |
| AWS Open Data S3 (`noaa-ghcnh-pds`) | 2024–2026 only | Mirror of recent years only |

**Primary URL pattern (all years):**
```
https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
```

**AWS S3 URL pattern (2024+ only):**
```
s3://noaa-ghcnh-pds/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
https://noaa-ghcnh-pds.s3.amazonaws.com/hourly/access/by-year/{YEAR}/parquet/GHCNh_{STATION}_{YEAR}.parquet
```

**Bucket backing the NCEI web explorer:** `datapub-global-historical-climatology-network` (Ceph). The S3-compatible
API is accessible via the NCEI hostname but does not support anonymous bucket
listing the same way AWS does; use the direct HTTPS file URLs instead.

## GHCNh file format
Files are **Parquet** (not the fixed-width gzip format used by ISD). Each
station-year file contains **329 columns**: one value column and five metadata
columns (Measurement_Code, Quality_Code, Report_Type, Source_Code,
Source_Station_ID) per variable. Key variables and their GHCNh column names:

| HDP variable | GHCNh column | Units |
|---|---|---|
| Air temperature | `temperature` | °C |
| Dew point | `dew_point_temperature` | °C |
| Wind speed | `wind_speed` | m/s |
| Wind direction | `wind_direction` | degrees |
| Precipitation | `precipitation` | mm |
| Sea-level pressure | `sea_level_pressure` | hPa |
| Relative humidity | `relative_humidity` | % |
| Station pressure | `station_level_pressure` | hPa |
| Snow depth | `snow_depth` | m |
| Visibility | `visibility` | m |

Timestamp column is `DATE` (ISO 8601 string). Year/Month/Day/Hour/Minute are
also provided as separate integer columns. Observations are sub-hourly (METAR
cadence, typically at :56 past the hour plus specials).

## GHCNh station ID mapping (ISD → GHCNh)
ISD station IDs (`726300-14733`) map to GHCNh IDs as follows:

```
ISD format:    USAF(6) + "-" + WBAN(5)   e.g. 726300-14733
GHCNh format:  "USW" + WBAN zero-padded to 8 digits  e.g. USW00014733
HDP format:    "ASOSAWOS_" + USAF(6) + WBAN(5)       e.g. ASOSAWOS_72630014733
```

Extraction in Python:
```python
def isd_to_ghcnh(isd_id: str) -> str:
    """Convert ISD '726300-14733' to GHCNh 'USW00014733'."""
    wban = isd_id.split("-")[1]          # '14733'
    return f"USW{int(wban):08d}"          # 'USW00014733'

def hdp_to_ghcnh(hdp_station_id: str) -> str:
    """Convert HDP 'ASOSAWOS_72630014733' to GHCNh 'USW00014733'."""
    stripped = hdp_station_id.replace("ASOSAWOS_", "")  # '72630014733'
    wban = stripped[6:]                                   # '14733'
    return f"USW{int(wban):08d}"                          # 'USW00014733'
```

**Note:** this mapping is exact for WBAN-registered stations. The ISD USAF prefix
is not encoded in the GHCNh ID; stations that share a WBAN but have different USAF
codes (co-located sensors) merge into one GHCNh record.

## GHCNh WECC station coverage
Cross-referenced against the ISD station history (`isd-history.csv`) filtered to
WECC states (CA, OR, WA, NV, AZ, NM, UT, CO, ID, MT, WY, AK, HI) as of
June 2026:

- **Total WECC ISD stations with valid WBAN:** 1,231
- **Present in GHCNh:** 1,168 **(94.9%)**
- **Missing from GHCNh:** 63
  - Closed before 2000: 56
  - Closed 2000–2009: 3
  - Closed 2010–2019: 3
  - Closed 2020+: 1 (Death Valley NP, WBAN 53186, USAF 999999 — NPS station
    not registered in ASOS/ISD proper; not in HDP ASOSAWOS network)

**Practical conclusion:** all currently-active WECC ASOS/AWOS stations are present
in GHCNh. The 63 missing stations are all decommissioned, predominantly pre-2000
military airfields (El Toro MCAS, Yuma Proving Ground, Hunter-Liggett) and remote
Alaskan posts. None are in the active ASOSAWOS pipeline.

## GHCNh metadata / inventory files
All documentation files are served from the same Ceph object store:

| File | URL | Description |
|---|---|---|
| Station list | `.../hourly/doc/ghcnh-station-list.txt` | Fixed-width: ID, lat, lon, elev, state, name, WBAN |
| Inventory | `.../hourly/doc/ghcnh-inventory.txt` | Per-station per-year monthly hour counts (93 MB, 1M+ rows) |
| Column reference | `.../hourly/doc/ghcnh-columns.pdf` | All 329 columns with definitions and units |
| Country codes | `.../hourly/doc/ghcn-countries.txt` | Country code crosswalk |

Base URL prefix: `https://www.ncei.noaa.gov/oa/global-historical-climatology-network/`

Web explorer (human-readable directory browser):
`https://www.ncei.noaa.gov/oa/global-historical-climatology-network/index.html#hourly/`
