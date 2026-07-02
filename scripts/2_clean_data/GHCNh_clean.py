"""GHCNh_clean.py

Clean GHCNh (Global Historical Climatology Network — Hourly) Parquet files
into the HDP NetCDF schema produced by ASOSAWOS_clean.py.

The output is identical in structure to ASOSAWOS_clean.py so that the QAQC
and merge stages are fully format-agnostic.

Input:
  s3://{HDP_BUCKET}/1_raw_wx/ASOSAWOS/{STATION_ID}/GHCNh_{GHCNH_ID}_{YEAR}.parquet

Output (full run):
  s3://{HDP_BUCKET}/2_clean_wx/ASOSAWOS/{STATION_ID}.nc

Output (append run):
  s3://{HDP_BUCKET}/2_clean_wx_append/ASOSAWOS/{STATION_ID}.nc

GHCNh column → HDP variable mapping
-------------------------------------
  temperature              → tas      (°C → K)
  dew_point_temperature    → tdps     (°C → K)
  wind_speed               → sfcWind  (m/s, no conversion)
  wind_direction           → sfcWind_dir (degrees, no conversion)
  precipitation            → pr       (mm, no conversion)
  sea_level_pressure       → psl      (hPa → Pa, fallback if ps absent)
  station_level_pressure   → ps       (hPa → Pa, preferred)
  relative_humidity        → hurs     (%, no conversion)

Quality codes follow GHCNh convention: 0=passed, 1=passed(only source),
2=passed with doubt, 3=failed.  Preserved as-is in *_qc variables.

Sub-hourly obs handling:
  GHCNh stores METAR-cadence observations (~:56 past the hour).  For the
  hourly time series, the last observation within each calendar hour is
  selected (typically the :55/:56 synoptic METAR).

Usage
-----
  # Single station, full run
  python GHCNh_clean.py --station ASOSAWOS_72630014733

  # Append mode (only new data since baseline zarr last timestamp)
  python GHCNh_clean.py --station ASOSAWOS_72630014733 --append

  # Explicit append boundary (override baseline zarr lookup)
  python GHCNh_clean.py --station ASOSAWOS_72630014733 --append --start-date 2025-10-01

  # Multiple stations from a CSV
  python GHCNh_clean.py --stations-csv /path/to/stations.csv
"""

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
from io import StringIO

import boto3
import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import calc_clean
from paths import (
    BUCKET_NAME,
    CLEAN_APPEND,
    CLEAN_WX,
    MERGE_WX,
    RAW_WX,
    SOURCE_BUCKET,
)

s3 = boto3.resource("s3")
s3_cl = boto3.client("s3")

os.makedirs("temp", exist_ok=True)

# ---------------------------------------------------------------------------
# GHCNh column definitions
# ---------------------------------------------------------------------------

# Maps GHCNh Parquet column name → (HDP variable name, conversion function or None)
# Order matters: ps before psl so station pressure takes precedence.
_VARIABLE_MAP: list[tuple[str, str, object | None]] = [
    ("station_level_pressure", "ps", calc_clean._unit_pres_hpa_to_pa),
    ("sea_level_pressure", "psl", calc_clean._unit_pres_hpa_to_pa),
    ("temperature", "tas", calc_clean._unit_degC_to_K),
    ("dew_point_temperature", "tdps", calc_clean._unit_degC_to_K),
    ("precipitation", "pr", None),
    ("relative_humidity", "hurs", None),
    ("wind_speed", "sfcWind", None),
    ("wind_direction", "sfcWind_dir", None),
]

# GHCNh missing/fill value for numeric columns
_GHCNH_FILL = 9999.9


# ---------------------------------------------------------------------------
# Helpers: station ID
# ---------------------------------------------------------------------------


def hdp_to_ghcnh_id(station_id: str) -> str:
    """Convert ``ASOSAWOS_72630014733`` → ``USW00014733``."""
    stripped = station_id.replace("ASOSAWOS_", "")
    wban = stripped[6:]
    return f"USW{int(wban):08d}"


# ---------------------------------------------------------------------------
# Helpers: S3 I/O
# ---------------------------------------------------------------------------


def _list_raw_parquet_keys(bucket: str, station_id: str) -> list[str]:
    """Return S3 keys for all GHCNh Parquet files for *station_id*."""
    prefix = f"{RAW_WX}/ASOSAWOS/{station_id}/"
    keys: list[str] = []
    paginator = s3_cl.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return sorted(keys)


def _year_from_parquet_key(key: str) -> int | None:
    """Extract year from S3 key ``…/GHCNh_USW00014733_2024.parquet`` → 2024."""
    try:
        return int(key.split("_")[-1].replace(".parquet", ""))
    except (ValueError, IndexError):
        return None


def _keys_from_year(keys: list[str], start_year: int) -> list[str]:
    """Return only keys whose year >= *start_year*."""
    return [k for k in keys if (_year_from_parquet_key(k) or 0) >= start_year]


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Download and read a Parquet file from S3 into a DataFrame."""
    import io

    obj = s3_cl.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


# ---------------------------------------------------------------------------
# Helpers: append boundary
# ---------------------------------------------------------------------------


def _baseline_last_time(station_id: str) -> datetime | None:
    """Return the last time coordinate from the published baseline zarr.

    Returns ``None`` if the zarr does not exist or has no time data, which
    causes the caller to fall back to a full-record clean.
    """
    zarr_url = f"s3://{SOURCE_BUCKET}/{MERGE_WX}/ASOSAWOS/{station_id}.zarr"
    try:
        ds = xr.open_zarr(zarr_url)
        last = pd.Timestamp(ds.time.values[-1]).to_pydatetime()
        ds.close()
        return last
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core: parse a single GHCNh DataFrame into HDP variable dict
# ---------------------------------------------------------------------------


def _parse_ghcnh_df(df: pd.DataFrame) -> dict:
    """Convert one station-year GHCNh DataFrame into HDP variable lists.

    Returns a dict with the same keys as the ``data`` accumulator in
    ``clean_ghcnh``.  Missing values are represented as ``np.nan``.
    """
    rows: dict[str, list] = {
        "time": [],
        "lat": [],
        "lon": [],
        "elevation": [],
        "qaqc_source": [],
    }
    # Pre-populate variable and qc lists
    for _, hdp_var, _ in _VARIABLE_MAP:
        rows[hdp_var] = []
        rows[f"{hdp_var}_qc"] = []

    # Parse timestamps — GHCNh uses ISO8601 UTC strings in the DATE column
    df = df.copy()
    df["_dt"] = pd.to_datetime(df["DATE"], utc=True, errors="coerce")
    df = df.dropna(subset=["_dt"])

    # Group by floor-hour, take the last observation in each hour bucket
    # (the regular METAR is typically at :55-:56, which is the last obs)
    df["_hour"] = df["_dt"].dt.floor("h")
    df = df.sort_values("_dt").groupby("_hour", sort=False).last().reset_index()
    df = df.rename(columns={"_hour": "_time_hourly"})

    for _, row in df.iterrows():
        rows["time"].append(row["_time_hourly"].to_pydatetime().replace(tzinfo=None))
        rows["lat"].append(_safe_float(row.get("LATITUDE")))
        rows["lon"].append(_safe_float(row.get("LONGITUDE")))
        rows["elevation"].append(_safe_float(row.get("ELEVATION")))
        rows["qaqc_source"].append("GHCNh")

        for ghcnh_col, hdp_var, _conv in _VARIABLE_MAP:
            val = _safe_float(row.get(ghcnh_col))
            qc_col = f"{ghcnh_col}_Quality_Code"
            qc_val = (
                str(row.get(qc_col, "")) if pd.notna(row.get(qc_col, np.nan)) else ""
            )
            rows[hdp_var].append(val)
            rows[f"{hdp_var}_qc"].append(qc_val)

    return rows


def _safe_float(val) -> float:
    """Convert value to float, returning NaN for missing fill values."""
    try:
        f = float(val)
        # GHCNh uses 9999/9999.9 as missing fill
        if f >= 9000:
            return np.nan
        return f
    except (TypeError, ValueError):
        return np.nan


def _merge_row_dicts(acc: dict, new: dict) -> dict:
    """Extend accumulator lists with values from new dict."""
    for key, vals in new.items():
        acc.setdefault(key, []).extend(vals)
    return acc


# ---------------------------------------------------------------------------
# Core: build xarray Dataset from HDP variable dict
# ---------------------------------------------------------------------------


def _build_dataset(
    data: dict,
    station_id: str,
    station_name: str,
    timestamp: str,
) -> xr.Dataset | None:
    """Convert the HDP variable dict to an xarray Dataset.

    Applies unit conversions, sets variable attributes, sets coordinates,
    and drops all-NaN variables.  Returns ``None`` if ``data`` is empty.
    """
    df = pd.DataFrame(data)
    if df.empty:
        return None

    # Set time as the primary index
    df = df.sort_values("time").drop_duplicates(subset=["time"])
    df = df.set_index("time")

    ds = df.to_xarray()

    # ---- unit conversions (operate on xr.DataArray) ----
    for ghcnh_col, hdp_var, conv in _VARIABLE_MAP:
        if hdp_var in ds and conv is not None:
            ds[hdp_var] = conv(ds[hdp_var])

    # ---- global attributes ----
    ds = ds.assign_attrs(
        title="ASOS/AWOS cleaned (GHCNh source)",
        institution="Eagle Rock Analytics / Cal Adapt",
        source="NOAA Global Historical Climatology Network — Hourly (GHCNh)",
        history=f"GHCNh_clean.py run on {timestamp} UTC",
        comment="Intermediate data product sourced from GHCNh Parquet files.",
        license="",
        citation="NOAA NCEI GHCNh https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly",
        disclaimer=(
            "This document was prepared as a result of work sponsored by the "
            "California Energy Commission (PIR-19-006). It does not necessarily "
            "represent the views of the Energy Commission."
        ),
    )

    # ---- station metadata ----
    ds.attrs["station_name"] = station_name

    # ---- coordinates ----
    ds = ds.assign_coords(station=station_id)
    ds = ds.expand_dims("station")

    if "lat" in ds:
        ds = ds.set_coords("lat")
    if "lon" in ds:
        ds = ds.set_coords("lon")

    # ---- coordinate attributes ----
    ds["time"].attrs.update(long_name="time", standard_name="time", comment="In UTC.")
    ds["station"].attrs.update(
        long_name="station_id",
        comment="Unique ID: network prefix + original station ID.",
    )
    if "lat" in ds:
        ds["lat"].attrs.update(
            long_name="latitude", standard_name="latitude", units="degrees_north"
        )
    if "lon" in ds:
        ds["lon"].attrs.update(
            long_name="longitude", standard_name="longitude", units="degrees_east"
        )
    if "elevation" in ds:
        ds["elevation"].attrs.update(
            standard_name="height_above_mean_sea_level",
            long_name="station_elevation",
            units="meter",
            positive="up",
        )

    # ---- variable attributes ----
    _set_variable_attrs(ds)

    # ---- drop all-NaN variables ----
    keys_to_drop = []
    for key in list(ds.data_vars):
        try:
            if key != "elevation" and np.all(np.isnan(ds[key].values.astype(float))):
                keys_to_drop.append(key)
        except (TypeError, ValueError):
            pass
    if keys_to_drop:
        ds = ds.drop_vars(keys_to_drop)

    # ---- reorder variables ----
    desired_order = ["ps", "tas", "tdps", "pr", "hurs", "sfcWind", "sfcWind_dir"]
    desired_order = [v for v in desired_order if v in ds]
    rest = [v for v in ds.data_vars if v not in desired_order]
    ds = ds[desired_order + rest]

    return ds


def _set_variable_attrs(ds: xr.Dataset) -> None:
    """Set CF-style attributes for HDP variables in place."""
    _qc_note = "GHCNh Quality Code: 0=passed, 1=passed(only source), 2=passed with doubt, 3=failed."

    if "tas" in ds:
        ds["tas"].attrs.update(
            long_name="air_temperature",
            standard_name="air_temperature",
            units="degree_Kelvin",
            ancillary_variables="tas_qc",
            comment="Converted from Celsius. Source: GHCNh temperature column.",
        )
    if "tas_qc" in ds:
        ds["tas_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "tdps" in ds:
        ds["tdps"].attrs.update(
            long_name="dew_point_temperature",
            standard_name="dew_point_temperature",
            units="degree_Kelvin",
            ancillary_variables="tdps_qc",
            comment="Converted from Celsius. Source: GHCNh dew_point_temperature column.",
        )
    if "tdps_qc" in ds:
        ds["tdps_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "ps" in ds:
        ds["ps"].attrs.update(
            long_name="station_air_pressure",
            standard_name="air_pressure",
            units="Pa",
            ancillary_variables="ps_qc",
            comment="Converted from hPa to Pa. Source: GHCNh station_level_pressure column.",
        )
    if "ps_qc" in ds:
        ds["ps_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "psl" in ds:
        ds["psl"].attrs.update(
            long_name="sea_level_air_pressure",
            standard_name="air_pressure_at_sea_level",
            units="Pa",
            ancillary_variables="psl_qc",
            comment="Converted from hPa to Pa. Source: GHCNh sea_level_pressure column.",
        )
    if "psl_qc" in ds:
        ds["psl_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "pr" in ds:
        ds["pr"].attrs.update(
            long_name="precipitation_accumulation",
            units="mm/?",
            ancillary_variables="pr_qc",
            comment="Source: GHCNh precipitation column. Duration not provided by GHCNh.",
        )
    if "pr_qc" in ds:
        ds["pr_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "hurs" in ds:
        ds["hurs"].attrs.update(
            long_name="relative_humidity",
            standard_name="relative_humidity",
            units="%",
            ancillary_variables="hurs_qc",
            comment="Source: GHCNh relative_humidity column.",
        )
    if "hurs_qc" in ds:
        ds["hurs_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "sfcWind" in ds:
        ds["sfcWind"].attrs.update(
            long_name="wind_speed",
            standard_name="wind_speed",
            units="m s-1",
            ancillary_variables="sfcWind_qc",
            comment="Source: GHCNh wind_speed column. Already in m/s.",
        )
    if "sfcWind_qc" in ds:
        ds["sfcWind_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "sfcWind_dir" in ds:
        ds["sfcWind_dir"].attrs.update(
            long_name="wind_direction",
            standard_name="wind_from_direction",
            units="degrees_clockwise_from_north",
            ancillary_variables="sfcWind_dir_qc",
            comment="Source: GHCNh wind_direction column.",
        )
    if "sfcWind_dir_qc" in ds:
        ds["sfcWind_dir_qc"].attrs.update(flag_values="0 1 2 3", flag_meanings=_qc_note)

    if "qaqc_source" in ds:
        ds["qaqc_source"].attrs.update(
            long_name="data_source",
            comment="Always 'GHCNh' for files processed by GHCNh_clean.py.",
        )


# ---------------------------------------------------------------------------
# Main clean function
# ---------------------------------------------------------------------------


def clean_ghcnh(
    station_ids: list[str],
    rawdir_prefix: str = RAW_WX,
    cleandir: str = f"{CLEAN_WX}/ASOSAWOS/",
    append: bool = False,
    start_date: datetime | None = None,
    bucket: str = BUCKET_NAME,
) -> None:
    """Clean GHCNh Parquet data for a list of HDP station IDs.

    Parameters
    ----------
    station_ids : list[str]
        HDP ASOSAWOS station IDs, e.g. ``["ASOSAWOS_72630014733"]``.
    rawdir_prefix : str
        S3 prefix for raw GHCNh Parquet files.
    cleandir : str
        S3 prefix for clean output .nc files (no leading/trailing slash).
    append : bool
        If True, process only data after the baseline zarr last timestamp.
    start_date : datetime | None
        Explicit append boundary. Overrides per-station baseline zarr lookup.
    bucket : str
        S3 bucket for both input and output.
    """
    errors: dict[str, list] = {"File": [], "Time": [], "Error": []}
    end_api = datetime.now().strftime("%Y%m%d%H%M")
    timestamp = datetime.now(tz=timezone.utc).strftime("%m-%d-%Y, %H:%M:%S")

    for station_id in station_ids:
        print(f"Parsing: {station_id}")
        try:
            keys = _list_raw_parquet_keys(bucket, station_id)
            if not keys:
                print(f"  No raw Parquet files found for {station_id}, skipping.")
                continue

            # Append mode: resolve boundary timestamp and restrict to relevant years
            T: datetime | None = None
            if append:
                T = (
                    start_date
                    if start_date is not None
                    else _baseline_last_time(station_id)
                )
                if T is not None:
                    keys = _keys_from_year(keys, T.year)
                    if not keys:
                        print(
                            f"  No new Parquet files for {station_id} after "
                            f"{T.year}, skipping."
                        )
                        continue

            # Load and concatenate all Parquet files
            dfs: list[pd.DataFrame] = []
            for key in keys:
                try:
                    df_year = _read_parquet_from_s3(bucket, key)
                    dfs.append(df_year)
                except Exception as exc:
                    print(f"  Error reading {key}: {exc}")
                    errors["File"].append(key)
                    errors["Time"].append(end_api)
                    errors["Error"].append(str(exc))

            if not dfs:
                print(f"  No data loaded for {station_id}, skipping.")
                continue

            df_all = pd.concat(dfs, ignore_index=True)

            # Parse and accumulate HDP variable rows
            data = _parse_ghcnh_df(df_all)

            # Append mode: filter rows to only those after boundary T
            if append and T is not None:
                T_floor = pd.Timestamp(T).floor("h").to_pydatetime()
                idx = [i for i, t in enumerate(data["time"]) if t >= T_floor]
                if not idx:
                    print(
                        f"  No new observations for {station_id} after "
                        f"{T:%Y-%m-%dT%H:%M}, skipping."
                    )
                    continue
                data = {k: [v[i] for i in idx] for k, v in data.items()}

            if not data.get("time"):
                print(f"  Empty data for {station_id} after filtering, skipping.")
                continue

            # Infer station name from Parquet NAME column (first non-null)
            station_name: str = station_id
            for df in dfs:
                if "NAME" in df.columns:
                    names = df["NAME"].dropna()
                    if not names.empty:
                        station_name = str(names.iloc[0])
                        break

            # Build xarray Dataset
            ds = _build_dataset(data, station_id, station_name, timestamp)
            if ds is None:
                print(f"  No data to save for {station_id}.")
                continue

            # Write to S3
            filename = f"{station_id}.nc"
            if append:
                filepath = f"{cleandir}{CLEAN_APPEND}/{filename}"
            else:
                filepath = f"{cleandir}{filename}"

            ds.to_netcdf(path=f"temp/temp_ghcnh_{station_id}.nc", engine="netcdf4")
            s3.Bucket(bucket).upload_file(f"temp/temp_ghcnh_{station_id}.nc", filepath)
            os.remove(f"temp/temp_ghcnh_{station_id}.nc")
            print(f"  Saved {filename} ({ds.dims}) -> s3://{bucket}/{filepath}")
            ds.close()

        except Exception as exc:
            traceback.print_exc()
            errors["File"].append(station_id)
            errors["Time"].append(end_api)
            errors["Error"].append(str(exc))

    # Write error log
    errors_df = pd.DataFrame(errors)
    csv_buf = StringIO()
    errors_df.to_csv(csv_buf, index=False)
    s3_cl.put_object(
        Bucket=bucket,
        Body=csv_buf.getvalue(),
        Key=f"{cleandir.rstrip('/')}/errors_ghcnh_clean_{end_api}.csv",
    )
    if not errors_df.empty:
        print(f"  {len(errors_df)} error(s) — see errors_ghcnh_clean_{end_api}.csv")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Clean GHCNh Parquet files into HDP NetCDF format.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    station_group = parser.add_mutually_exclusive_group(required=True)
    station_group.add_argument(
        "-s",
        "--station",
        nargs="+",
        metavar="STATION_ID",
        help="One or more HDP ASOSAWOS station IDs.",
    )
    station_group.add_argument(
        "--stations-csv",
        metavar="CSV_PATH",
        help="Path to a CSV with a 'station_id' column.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append mode: process only data after baseline zarr last timestamp.",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Explicit append boundary (overrides baseline zarr lookup). "
        "Only used with --append.",
    )
    parser.add_argument(
        "--bucket",
        default=BUCKET_NAME,
        help="S3 bucket. Overrides HDP_BUCKET env var.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.station:
        station_ids = args.station
    else:
        df = pd.read_csv(args.stations_csv)
        if "station_id" not in df.columns:
            raise ValueError(
                f"CSV must have a 'station_id' column. Found: {list(df.columns)}"
            )
        station_ids = df["station_id"].dropna().astype(str).tolist()

    if not station_ids:
        print("No stations to process. Exiting.")
        return

    start_date: datetime | None = None
    if args.append and args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")

    clean_ghcnh(
        station_ids=station_ids,
        append=args.append,
        start_date=start_date,
        bucket=args.bucket,
    )


if __name__ == "__main__":
    main()
