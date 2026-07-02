"""Plot all-station ASOSAWOS station-vs-time heatmaps for QA/QC validation.

This script creates a 2D station-time heatmap where:
- x-axis: time bins (monthly by default)
- y-axis: station ids
- color: selected metric (value mean, completeness, or flagged ratio)

Stations are ordered with active stations at the top and inactive stations at the
bottom. Within each group, stations are sorted by overall data completeness.

Example
-------
python scripts/tests/plot_asosawos_station_time_heatmap.py \
  --bucket auto-hdp \
  --prefix hdp \
  --network ASOSAWOS \
  --variable tas \
  --metric completeness \
  --output-dir temp/asosawos_validation_figures
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import boto3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from paths import PUBLISH_BUCKET, PUBLISH_PREFIX

FLAG_EMPTY_VALUES = {"", "nan", "None", "no_flag"}


@dataclass
class StationMetric:
    station_id: str
    series: pd.Series
    last_valid_time: pd.Timestamp | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create all-station ASOSAWOS station-time heatmaps for QA/QC review."
    )
    parser.add_argument(
        "--bucket", default=PUBLISH_BUCKET, help="S3 bucket with merged zarrs"
    )
    parser.add_argument(
        "--prefix",
        default=PUBLISH_PREFIX,
        help="S3 prefix containing network folders (e.g., hdp)",
    )
    parser.add_argument("--network", default="ASOSAWOS", help="Network folder to scan")
    parser.add_argument(
        "--variable",
        default="tas",
        help="Primary variable to summarize for each station",
    )
    parser.add_argument(
        "--metric",
        default="completeness",
        choices=["completeness", "flagged_ratio", "mean"],
        help="Metric used for heatmap color",
    )
    parser.add_argument(
        "--resample-frequency",
        default="MS",
        help="Pandas resample frequency for x-axis bins (default MS = monthly start)",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=45,
        help="Stations with latest valid timestamp newer than this are active",
    )
    parser.add_argument(
        "--output-dir",
        default="temp/asosawos_validation_figures",
        help="Directory to write figure and summary CSVs",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=0,
        help="Optional cap for fast preview; 0 means all stations",
    )
    return parser.parse_args()


def list_station_ids(bucket: str, prefix: str, network: str) -> list[str]:
    s3_client = boto3.client("s3")
    base_prefix = f"{prefix.strip('/')}/{network}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    station_ids: set[str] = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            suffix = key[len(base_prefix) :]
            store_name = suffix.split("/", 1)[0]
            if store_name.endswith(".zarr"):
                station_ids.add(store_name[: -len(".zarr")])

    return sorted(station_ids)


def station_zarr_url(bucket: str, prefix: str, network: str, station_id: str) -> str:
    return f"s3://{bucket}/{prefix.strip('/')}/{network}/{station_id}.zarr"


def _time_series_from_var(ds: xr.Dataset, variable: str) -> pd.Series:
    """Return a 1D UTC-indexed series for a station variable.

    HDP merged zarrs are typically (station, time). This helper squeezes out the
    station dimension and guarantees a DatetimeIndex for time resampling.
    """

    da = ds[variable]
    if "station" in da.dims:
        da = da.isel(station=0)
    series = da.to_series().sort_index()
    series.index = pd.to_datetime(series.index, utc=True)
    return series


def _compute_metric_series(
    ds: xr.Dataset,
    variable: str,
    metric: str,
    resample_frequency: str,
) -> pd.Series:
    value_series = _time_series_from_var(ds, variable)

    if metric == "mean":
        return value_series.resample(resample_frequency).mean()

    present = value_series.notna().astype(float)

    if metric == "completeness":
        return present.resample(resample_frequency).mean()

    flag_col = f"{variable}_eraqc"
    if flag_col not in ds.data_vars:
        return pd.Series(dtype=float)

    flag_series = _time_series_from_var(ds, flag_col).astype(str).fillna("no_flag")
    flagged = (~flag_series.isin(FLAG_EMPTY_VALUES) & value_series.notna()).astype(
        float
    )

    flagged_counts = flagged.resample(resample_frequency).sum()
    present_counts = present.resample(resample_frequency).sum().replace(0, np.nan)
    return flagged_counts / present_counts


def load_station_metric(
    bucket: str,
    prefix: str,
    network: str,
    station_id: str,
    variable: str,
    metric: str,
    resample_frequency: str,
) -> StationMetric | None:
    zarr_url = station_zarr_url(bucket, prefix, network, station_id)
    ds = None
    try:
        ds = xr.open_zarr(zarr_url)
        if variable not in ds.data_vars or "time" not in ds:
            return None

        metric_series = _compute_metric_series(ds, variable, metric, resample_frequency)
        metric_series.name = station_id

        value_series = _time_series_from_var(ds, variable)
        valid_index = value_series.dropna().index
        last_valid_time = pd.Timestamp(valid_index.max()) if len(valid_index) else None

        return StationMetric(
            station_id=station_id,
            series=metric_series,
            last_valid_time=last_valid_time,
        )
    except Exception:
        return None
    finally:
        if ds is not None:
            ds.close()


def build_matrix(artifacts: list[StationMetric]) -> pd.DataFrame:
    return pd.DataFrame(
        {item.station_id: item.series for item in artifacts}
    ).transpose()


def _metric_color_limits(
    matrix: pd.DataFrame, metric: str
) -> tuple[float | None, float | None, str]:
    if metric in {"completeness", "flagged_ratio"}:
        cmap = "viridis" if metric == "completeness" else "magma"
        return 0.0, 1.0, cmap

    values = matrix.values[np.isfinite(matrix.values)]
    if values.size == 0:
        return None, None, "coolwarm"
    vmin = float(np.nanpercentile(values, 2))
    vmax = float(np.nanpercentile(values, 98))
    if math.isclose(vmin, vmax):
        vmin = None
        vmax = None
    return vmin, vmax, "coolwarm"


def plot_heatmap(
    matrix: pd.DataFrame,
    ordered_stations: list[str],
    active_count: int,
    metric: str,
    variable: str,
    output_path: Path,
) -> None:
    if matrix.empty:
        return

    ordered_matrix = matrix.loc[ordered_stations]
    data = ordered_matrix.values.astype(float)
    masked = np.ma.masked_invalid(data)
    vmin, vmax, cmap = _metric_color_limits(ordered_matrix, metric)

    fig_height = max(8, min(32, 0.22 * len(ordered_stations) + 2.5))
    fig, ax = plt.subplots(figsize=(18, fig_height))
    image = ax.imshow(
        masked, aspect="auto", interpolation="nearest", cmap=cmap, vmin=vmin, vmax=vmax
    )

    tick_step = max(1, len(ordered_stations) // 60)
    y_ticks = np.arange(0, len(ordered_stations), tick_step)
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([ordered_stations[idx] for idx in y_ticks], fontsize=7)

    columns = ordered_matrix.columns
    x_tick_step = max(1, len(columns) // 16)
    x_ticks = np.arange(0, len(columns), x_tick_step)
    x_labels = [pd.Timestamp(columns[idx]).strftime("%Y-%m") for idx in x_ticks]
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")

    ax.set_xlabel("Time")
    ax.set_ylabel("Station ID")
    ax.set_title(
        f"{variable} {metric} heatmap by station and time\n"
        f"Top: active stations, Bottom: inactive stations"
    )

    if 0 < active_count < len(ordered_stations):
        ax.axhline(active_count - 0.5, color="white", linestyle="--", linewidth=1.2)
        ax.text(
            0,
            active_count - 0.8,
            "active / inactive split",
            color="white",
            fontsize=8,
            va="bottom",
            ha="left",
        )

    colorbar = fig.colorbar(image, ax=ax, shrink=0.9)
    colorbar.set_label(f"{metric} ({variable})")

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    station_ids = list_station_ids(args.bucket, args.prefix, args.network)
    if args.max_stations > 0:
        station_ids = station_ids[: args.max_stations]

    print(
        f"Found {len(station_ids)} stations under s3://{args.bucket}/{args.prefix}/{args.network}/"
    )
    artifacts: list[StationMetric] = []
    skipped: list[dict[str, str]] = []

    for index, station_id in enumerate(station_ids, start=1):
        if index % 25 == 0 or index == len(station_ids):
            print(f"Processing station {index}/{len(station_ids)}")

        artifact = load_station_metric(
            bucket=args.bucket,
            prefix=args.prefix,
            network=args.network,
            station_id=station_id,
            variable=args.variable,
            metric=args.metric,
            resample_frequency=args.resample_frequency,
        )
        if artifact is None:
            skipped.append(
                {
                    "station_id": station_id,
                    "reason": "missing variable/time or read error",
                }
            )
            continue
        artifacts.append(artifact)

    if not artifacts:
        raise RuntimeError("No station data could be loaded for plotting")

    matrix = build_matrix(artifacts)
    now_utc = pd.Timestamp(datetime.now(tz=UTC))
    freshness_cutoff = now_utc - pd.Timedelta(days=args.freshness_days)

    summary_rows: list[dict[str, object]] = []
    for item in artifacts:
        non_null_fraction = float(matrix.loc[item.station_id].notna().mean())
        is_active = (
            item.last_valid_time is not None
            and item.last_valid_time >= freshness_cutoff
        )
        summary_rows.append(
            {
                "station_id": item.station_id,
                "last_valid_time": (
                    item.last_valid_time.isoformat()
                    if item.last_valid_time is not None
                    else ""
                ),
                "is_active": bool(is_active),
                "completeness_score": non_null_fraction,
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    active_df = summary_df[summary_df["is_active"]].sort_values(
        ["completeness_score", "station_id"], ascending=[False, True]
    )
    inactive_df = summary_df[~summary_df["is_active"]].sort_values(
        ["completeness_score", "station_id"], ascending=[False, True]
    )
    ordered_df = pd.concat([active_df, inactive_df], ignore_index=True)
    ordered_stations = ordered_df["station_id"].tolist()

    figure_path = (
        output_dir / f"asosawos_{args.variable}_{args.metric}_station_time_heatmap.png"
    )
    plot_heatmap(
        matrix=matrix,
        ordered_stations=ordered_stations,
        active_count=len(active_df),
        metric=args.metric,
        variable=args.variable,
        output_path=figure_path,
    )

    ordered_df.to_csv(
        output_dir / f"asosawos_{args.variable}_{args.metric}_station_order.csv",
        index=False,
    )
    matrix.loc[ordered_stations].to_csv(
        output_dir / f"asosawos_{args.variable}_{args.metric}_matrix.csv"
    )
    pd.DataFrame(skipped).to_csv(
        output_dir / f"asosawos_{args.variable}_{args.metric}_skipped_stations.csv",
        index=False,
    )

    print(f"Wrote figure: {figure_path}")
    print(
        "Wrote summaries: "
        f"{output_dir / f'asosawos_{args.variable}_{args.metric}_station_order.csv'}, "
        f"{output_dir / f'asosawos_{args.variable}_{args.metric}_matrix.csv'}"
    )


if __name__ == "__main__":
    main()
