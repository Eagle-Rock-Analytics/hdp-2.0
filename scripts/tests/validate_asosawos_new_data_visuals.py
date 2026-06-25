"""Create visual validations for ASOSAWOS append-era merged data.

This script is the Phase 1 validation surface for the ASOSAWOS append catch-up.
It reads the local Phase 1 manifests under ``temp/`` plus merged station zarrs from
the configured publish bucket, then writes a compact validation package:

- monthly data-quality trends for selected stations
- boundary continuity plots around each station's append boundary
- variable coverage deltas before/after the append boundary
- station-level summary checks for duplicates, freshness, and boundary gaps

Usage
-----
python scripts/tests/validate_asosawos_new_data_visuals.py

Example targeting a private bucket:
HDP_PUBLISH_BUCKET=auto-hdp HDP_PUBLISH_PREFIX=hdp \
  python scripts/tests/validate_asosawos_new_data_visuals.py \
    --output-dir temp/asosawos_validation_auto_hdp
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from paths import PUBLISH_BUCKET, PUBLISH_PREFIX

NETWORK = "ASOSAWOS"
QUALITY_VARS = [
    "tas",
    "tas_derived",
    "tdps",
    "tdps_derived",
    "ps",
    "psl",
    "ps_altimeter",
    "ps_derived",
    "hurs",
    "hurs_derived",
    "sfcWind",
    "sfcWind_dir",
    "pr_1h",
    "pr_24h",
    "pr_localday",
]
CONTINUITY_FAMILIES = {
    "Temperature": ["tas", "tas_derived"],
    "Wind Speed": ["sfcWind"],
    "Pressure": ["ps", "psl", "ps_altimeter", "ps_derived"],
}
FLAG_EMPTY_VALUES = {"", "nan", "None", "no_flag"}


@dataclass
class StationArtifacts:
    station_id: str
    boundary_time: pd.Timestamp | None
    record_type: str
    monthly_quality: pd.DataFrame
    coverage_delta: pd.Series
    summary_row: dict[str, object]
    continuity_data: dict[str, pd.DataFrame]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Phase 1 ASOSAWOS continuity and data-quality visuals."
    )
    parser.add_argument(
        "--bucket",
        default=PUBLISH_BUCKET,
        help="Publish bucket containing merged station zarrs.",
    )
    parser.add_argument(
        "--prefix",
        default=PUBLISH_PREFIX,
        help="Publish prefix within the bucket.",
    )
    parser.add_argument(
        "--manifest-csv",
        default="temp/asosawos_merge_manifest.csv",
        help="Local manifest CSV emitted after the Phase 1 merge catch-up.",
    )
    parser.add_argument(
        "--boundary-csv",
        default="temp/asosawos_last_timestamps.csv",
        help="Local CSV containing per-station append boundary timestamps.",
    )
    parser.add_argument(
        "--output-dir",
        default="temp/asosawos_validation_figures",
        help="Directory for figures and summary tables.",
    )
    parser.add_argument(
        "--station-list",
        default="",
        help="Comma-separated station ids to analyze. Overrides automatic selection.",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=10,
        help="Maximum number of representative stations to analyze.",
    )
    parser.add_argument(
        "--boundary-window-days",
        type=int,
        default=14,
        help="Days before/after the boundary to show in continuity plots.",
    )
    parser.add_argument(
        "--quality-window-days",
        type=int,
        default=30,
        help="Days before/after the boundary to compare coverage rates.",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=45,
        help="Freshness threshold for active stations.",
    )
    return parser.parse_args()


def load_inputs(
    manifest_csv: str, boundary_csv: str
) -> tuple[pd.DataFrame, pd.DataFrame, datetime]:
    manifest = pd.read_csv(manifest_csv)
    boundaries = pd.read_csv(boundary_csv)

    manifest["start"] = pd.to_datetime(manifest["start"], utc=True, errors="coerce")
    manifest["end"] = pd.to_datetime(manifest["end"], utc=True, errors="coerce")
    boundaries["last_timestamp"] = pd.to_datetime(
        boundaries["last_timestamp"], utc=True, errors="coerce"
    )

    manifest = manifest.rename(columns={"station": "station_id"})
    merged = manifest.merge(boundaries, on="station_id", how="left")

    now_utc = datetime.now(tz=UTC)
    return merged, boundaries, now_utc


def _evenly_spaced_ids(values: list[str], count: int) -> list[str]:
    if count <= 0 or not values:
        return []
    if len(values) <= count:
        return values
    indices = np.linspace(0, len(values) - 1, count, dtype=int)
    return [values[index] for index in indices]


def select_representative_stations(
    manifest: pd.DataFrame,
    max_stations: int,
    freshness_days: int,
    explicit_station_list: str,
    now_utc: datetime,
) -> list[str]:
    if explicit_station_list.strip():
        return [
            item.strip() for item in explicit_station_list.split(",") if item.strip()
        ]

    freshness_cutoff = pd.Timestamp(now_utc - timedelta(days=freshness_days))
    boundary_ready = manifest[
        manifest["last_timestamp"].notna()
        & manifest["has_pre_2022"].astype(bool)
        & (manifest["n_2022_plus"] > 0)
    ].copy()

    active = boundary_ready[
        (boundary_ready["record_type"] == "full")
        & (boundary_ready["end"] >= freshness_cutoff)
    ].sort_values(["start", "station_id"])
    stale = boundary_ready[
        (boundary_ready["record_type"] == "full")
        & (boundary_ready["end"] < freshness_cutoff)
    ].sort_values(["end", "station_id"])
    baseline_only = manifest[manifest["record_type"] == "baseline_only"].sort_values(
        ["end", "station_id"]
    )

    active_target = min(max_stations, max(6, math.ceil(max_stations * 0.7)))
    stale_target = min(
        max_stations - min(len(active), active_target), max(2, max_stations // 4)
    )

    selected = []
    selected.extend(_evenly_spaced_ids(active["station_id"].tolist(), active_target))
    selected.extend(_evenly_spaced_ids(stale["station_id"].tolist(), stale_target))

    if len(selected) < max_stations:
        fillers = (
            baseline_only["station_id"].tolist()
            + manifest[~manifest["station_id"].isin(selected)]["station_id"].tolist()
        )
        for station_id in fillers:
            if station_id not in selected:
                selected.append(station_id)
            if len(selected) >= max_stations:
                break

    return selected[:max_stations]


def station_zarr_url(bucket: str, prefix: str, station_id: str) -> str:
    return f"s3://{bucket}/{prefix.strip('/')}/{NETWORK}/{station_id}.zarr"


def read_station_dataset(bucket: str, prefix: str, station_id: str) -> xr.Dataset:
    ds = xr.open_zarr(station_zarr_url(bucket, prefix, station_id))
    if "time" not in ds:
        raise ValueError(f"{station_id}: merged zarr is missing the time coordinate")
    return ds.sortby("time")


def _first_existing(candidates: list[str], available: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def _quality_series_for_var(
    df: pd.DataFrame, variable: str
) -> tuple[pd.Series, pd.Series]:
    present = df[variable].notna().astype(int)
    flag_col = f"{variable}_eraqc"
    if flag_col not in df.columns:
        flagged = pd.Series(0, index=df.index, dtype=int)
        return present, flagged

    flag_values = df[flag_col].astype(str).fillna("no_flag")
    flagged = (~flag_values.isin(FLAG_EMPTY_VALUES) & df[variable].notna()).astype(int)
    return present, flagged


def build_monthly_quality(df: pd.DataFrame, available_vars: list[str]) -> pd.DataFrame:
    if not available_vars:
        return pd.DataFrame(
            columns=[
                "time",
                "completeness_ratio",
                "flagged_ratio",
                "available_count",
                "flagged_count",
            ]
        )

    availability = pd.DataFrame(index=df.index)
    flags = pd.DataFrame(index=df.index)
    for variable in available_vars:
        availability[variable], flags[variable] = _quality_series_for_var(df, variable)

    available_count = availability.sum(axis=1)
    flagged_count = flags.sum(axis=1)
    daily_quality = pd.DataFrame(
        {
            "completeness_ratio": available_count / len(available_vars),
            "flagged_ratio": flagged_count / available_count.replace(0, np.nan),
            "available_count": available_count,
            "flagged_count": flagged_count,
        },
        index=df.index,
    )

    monthly = daily_quality.resample("MS").mean(numeric_only=True)
    monthly.index.name = "time"
    return monthly.reset_index()


def coverage_delta(
    df: pd.DataFrame,
    boundary_time: pd.Timestamp | None,
    window_days: int,
    available_vars: list[str],
) -> pd.Series:
    if boundary_time is None or not available_vars:
        return pd.Series(dtype=float)

    pre_start = boundary_time - pd.Timedelta(days=window_days)
    post_end = boundary_time + pd.Timedelta(days=window_days)
    pre_df = df[(df.index >= pre_start) & (df.index < boundary_time)]
    post_df = df[(df.index >= boundary_time) & (df.index <= post_end)]

    if pre_df.empty or post_df.empty:
        return pd.Series(dtype=float)

    pre_cov = pre_df[available_vars].notna().mean(axis=0)
    post_cov = post_df[available_vars].notna().mean(axis=0)
    return (post_cov - pre_cov).sort_index()


def continuity_windows(
    df: pd.DataFrame,
    boundary_time: pd.Timestamp | None,
    window_days: int,
) -> dict[str, pd.DataFrame]:
    if boundary_time is None:
        return {}

    start = boundary_time - pd.Timedelta(days=window_days)
    end = boundary_time + pd.Timedelta(days=window_days)
    clipped = df[(df.index >= start) & (df.index <= end)]
    if clipped.empty:
        return {}

    continuity: dict[str, pd.DataFrame] = {}
    available_columns = list(df.columns)
    for label, family in CONTINUITY_FAMILIES.items():
        variable = _first_existing(family, available_columns)
        if variable is None:
            continue
        subset_cols = [variable]
        flag_col = f"{variable}_eraqc"
        if flag_col in clipped.columns:
            subset_cols.append(flag_col)
        continuity[label] = clipped[subset_cols].copy()
    return continuity


def summarize_station(
    station_row: pd.Series,
    ds: xr.Dataset,
    now_utc: datetime,
    freshness_days: int,
    quality_window_days: int,
    boundary_window_days: int,
) -> StationArtifacts:
    boundary_time = pd.Timestamp(station_row.get("last_timestamp"))
    if pd.isna(boundary_time):
        boundary_time = None

    quality_vars = [var for var in QUALITY_VARS if var in ds.data_vars]
    df = ds[
        quality_vars + [var for var in ds.data_vars if var.endswith("_eraqc")]
    ].to_dataframe()
    df = df.sort_index()
    df.index = pd.to_datetime(df.index, utc=True)

    duplicate_count = int(df.index.duplicated().sum())
    dedup_df = df[~df.index.duplicated(keep="last")]

    boundary_gap_hours = np.nan
    pre_boundary_time = pd.NaT
    post_boundary_time = pd.NaT
    if boundary_time is not None:
        pre_times = dedup_df.index[dedup_df.index <= boundary_time]
        post_times = dedup_df.index[dedup_df.index > boundary_time]
        if len(pre_times) > 0:
            pre_boundary_time = pre_times.max()
        if len(post_times) > 0:
            post_boundary_time = post_times.min()
        if pd.notna(pre_boundary_time) and pd.notna(post_boundary_time):
            boundary_gap_hours = (
                post_boundary_time - pre_boundary_time
            ) / pd.Timedelta(hours=1)

    monthly_quality = build_monthly_quality(dedup_df, quality_vars)
    cov_delta = coverage_delta(
        dedup_df,
        boundary_time=boundary_time,
        window_days=quality_window_days,
        available_vars=quality_vars,
    )
    continuity = continuity_windows(
        dedup_df,
        boundary_time=boundary_time,
        window_days=boundary_window_days,
    )

    last_time = pd.Timestamp(dedup_df.index.max()) if not dedup_df.empty else pd.NaT
    freshness_days_value = (
        (pd.Timestamp(now_utc) - last_time) / pd.Timedelta(days=1)
        if pd.notna(last_time)
        else np.nan
    )
    is_active = (
        pd.Timestamp(station_row["end"])
        >= pd.Timestamp(now_utc - timedelta(days=freshness_days))
        if pd.notna(station_row["end"])
        else False
    )

    coverage_match = bool(
        cov_delta.empty or ((cov_delta.abs() <= 0.25) | cov_delta.isna()).all()
    )
    no_boundary_gap = bool(np.isnan(boundary_gap_hours) or boundary_gap_hours <= 1.0)

    summary_row = {
        "station_id": station_row["station_id"],
        "record_type": station_row["record_type"],
        "boundary_time": boundary_time.isoformat() if boundary_time is not None else "",
        "start": (
            pd.Timestamp(station_row["start"]).isoformat()
            if pd.notna(station_row["start"])
            else ""
        ),
        "end": (
            pd.Timestamp(station_row["end"]).isoformat()
            if pd.notna(station_row["end"])
            else ""
        ),
        "n_total": station_row.get("n_total", np.nan),
        "n_2022_plus": station_row.get("n_2022_plus", np.nan),
        "available_quality_vars": ",".join(quality_vars),
        "duplicate_time_count": duplicate_count,
        "boundary_gap_hours": boundary_gap_hours,
        "pre_boundary_time": (
            pre_boundary_time.isoformat() if pd.notna(pre_boundary_time) else ""
        ),
        "post_boundary_time": (
            post_boundary_time.isoformat() if pd.notna(post_boundary_time) else ""
        ),
        "last_merged_time": last_time.isoformat() if pd.notna(last_time) else "",
        "freshness_days": freshness_days_value,
        "is_active_station": is_active,
        "passes_freshness_check": (not is_active)
        or freshness_days_value <= freshness_days,
        "passes_duplicate_check": duplicate_count == 0,
        "passes_boundary_gap_check": no_boundary_gap,
        "passes_coverage_check": coverage_match,
    }

    return StationArtifacts(
        station_id=station_row["station_id"],
        boundary_time=boundary_time,
        record_type=station_row["record_type"],
        monthly_quality=monthly_quality,
        coverage_delta=cov_delta,
        summary_row=summary_row,
        continuity_data=continuity,
    )


def plot_quality_over_time(
    monthly_quality: pd.DataFrame,
    output_path: Path,
    boundary_bounds: tuple[pd.Timestamp, pd.Timestamp] | None,
) -> None:
    if monthly_quality.empty:
        return

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    grouped = monthly_quality.groupby("time")
    summary = grouped.agg(
        completeness_mean=("completeness_ratio", "mean"),
        completeness_low=(
            "completeness_ratio",
            lambda values: np.nanpercentile(values, 10),
        ),
        completeness_high=(
            "completeness_ratio",
            lambda values: np.nanpercentile(values, 90),
        ),
        flagged_mean=("flagged_ratio", "mean"),
        flagged_low=(
            "flagged_ratio",
            lambda values: (
                np.nanpercentile(values.dropna(), 10)
                if values.dropna().size
                else np.nan
            ),
        ),
        flagged_high=(
            "flagged_ratio",
            lambda values: (
                np.nanpercentile(values.dropna(), 90)
                if values.dropna().size
                else np.nan
            ),
        ),
    ).reset_index()

    axes[0].plot(
        summary["time"], summary["completeness_mean"], color="#005f73", linewidth=2
    )
    axes[0].fill_between(
        summary["time"],
        summary["completeness_low"],
        summary["completeness_high"],
        color="#94d2bd",
        alpha=0.45,
    )
    axes[0].set_ylabel("Completeness ratio")
    axes[0].set_title("Monthly data completeness across selected ASOSAWOS stations")
    axes[0].set_ylim(0, 1.05)
    axes[0].grid(alpha=0.25)

    axes[1].plot(summary["time"], summary["flagged_mean"], color="#bb3e03", linewidth=2)
    axes[1].fill_between(
        summary["time"],
        summary["flagged_low"],
        summary["flagged_high"],
        color="#ee9b00",
        alpha=0.35,
    )
    axes[1].set_ylabel("Flagged ratio")
    axes[1].set_title("Monthly QA/QC flag rate across selected ASOSAWOS stations")
    axes[1].set_ylim(bottom=0)
    axes[1].grid(alpha=0.25)

    if boundary_bounds is not None:
        start, end = boundary_bounds
        for axis in axes:
            axis.axvspan(start, end, color="#ae2012", alpha=0.12)

    axes[1].xaxis.set_major_locator(mdates.YearLocator())
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_station_timeline(summary_df: pd.DataFrame, output_path: Path) -> None:
    if summary_df.empty:
        return

    plot_df = summary_df.copy().sort_values(["record_type", "start", "station_id"])
    plot_df["start"] = pd.to_datetime(plot_df["start"], utc=True, errors="coerce")
    plot_df["end"] = pd.to_datetime(plot_df["end"], utc=True, errors="coerce")
    plot_df["boundary_time"] = pd.to_datetime(
        plot_df["boundary_time"], utc=True, errors="coerce"
    )

    fig, ax = plt.subplots(figsize=(14, max(6, len(plot_df) * 0.5)))
    color_map = {
        "full": "#0a9396",
        "baseline_only": "#9b2226",
        "append_only": "#ca6702",
    }
    for row_index, (_, row) in enumerate(plot_df.iterrows()):
        ax.hlines(
            y=row_index,
            xmin=row["start"],
            xmax=row["end"],
            color=color_map.get(row["record_type"], "#5c677d"),
            linewidth=3,
        )
        if pd.notna(row["boundary_time"]):
            ax.scatter(row["boundary_time"], row_index, color="#001219", s=24, zorder=3)

    ax.set_yticks(range(len(plot_df)))
    ax.set_yticklabels(plot_df["station_id"])
    ax.set_title("Selected stations: record span and append boundary")
    ax.set_xlabel("Time")
    ax.grid(alpha=0.2)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_coverage_delta_heatmap(
    coverage_frame: pd.DataFrame,
    output_path: Path,
) -> None:
    if coverage_frame.empty:
        return

    ordered = coverage_frame.sort_index().sort_index(axis=1)
    fig, ax = plt.subplots(
        figsize=(max(8, ordered.shape[1] * 0.9), max(5, ordered.shape[0] * 0.55))
    )
    image = ax.imshow(ordered.values, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")
    ax.set_xticks(range(ordered.shape[1]))
    ax.set_xticklabels(ordered.columns, rotation=45, ha="right")
    ax.set_yticks(range(ordered.shape[0]))
    ax.set_yticklabels(ordered.index)
    ax.set_title("Coverage delta: post-boundary minus pre-boundary window")
    colorbar = fig.colorbar(image, ax=ax, shrink=0.85)
    colorbar.set_label("Coverage delta")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_flag_markers(axis: plt.Axes, series_df: pd.DataFrame, value_col: str) -> None:
    flag_col = f"{value_col}_eraqc"
    if flag_col not in series_df.columns:
        return
    flags = series_df[flag_col].astype(str).fillna("no_flag")
    flagged_points = series_df[
        (~flags.isin(FLAG_EMPTY_VALUES)) & series_df[value_col].notna()
    ]
    if flagged_points.empty:
        return
    axis.scatter(
        flagged_points.index,
        flagged_points[value_col],
        color="#bb3e03",
        s=12,
        alpha=0.85,
        label="flagged obs",
    )


def plot_station_continuity(
    artifact: StationArtifacts,
    output_path: Path,
) -> None:
    if not artifact.continuity_data:
        return

    columns = list(artifact.continuity_data.keys())
    fig, axes = plt.subplots(
        1, len(columns), figsize=(5.4 * len(columns), 4.2), sharex=False
    )
    if len(columns) == 1:
        axes = [axes]

    for axis, label in zip(axes, columns, strict=False):
        series_df = artifact.continuity_data[label]
        value_col = next(col for col in series_df.columns if not col.endswith("_eraqc"))
        axis.plot(series_df.index, series_df[value_col], color="#005f73", linewidth=1.5)
        _plot_flag_markers(axis, series_df, value_col)
        if artifact.boundary_time is not None:
            axis.axvline(
                artifact.boundary_time, color="#ae2012", linestyle="--", linewidth=1.1
            )
        axis.set_title(f"{artifact.station_id}\n{label}: {value_col}")
        axis.grid(alpha=0.25)
        axis.xaxis.set_major_locator(
            mdates.DayLocator(interval=max(1, len(series_df) // 250 or 1))
        )
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        axis.tick_params(axis="x", rotation=45)

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_markdown_summary(
    summary_df: pd.DataFrame,
    selected_stations: list[str],
    output_path: Path,
    bucket: str,
    prefix: str,
) -> None:
    lines = [
        "# ASOSAWOS new-data validation summary",
        "",
        f"Target bucket: `s3://{bucket}/{prefix.strip('/')}/{NETWORK}/`",
        "",
        "## Selected stations",
        "",
    ]
    lines.extend([f"- {station_id}" for station_id in selected_stations])
    lines.extend(
        [
            "",
            "## Check counts",
            "",
            f"- Duplicate check passed: {int(summary_df['passes_duplicate_check'].sum())}/{len(summary_df)}",
            f"- Boundary gap check passed: {int(summary_df['passes_boundary_gap_check'].sum())}/{len(summary_df)}",
            f"- Coverage check passed: {int(summary_df['passes_coverage_check'].sum())}/{len(summary_df)}",
            f"- Freshness check passed: {int(summary_df['passes_freshness_check'].sum())}/{len(summary_df)}",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    station_plot_dir = output_dir / "stations"
    station_plot_dir.mkdir(parents=True, exist_ok=True)

    manifest, _, now_utc = load_inputs(args.manifest_csv, args.boundary_csv)
    selected_stations = select_representative_stations(
        manifest=manifest,
        max_stations=args.max_stations,
        freshness_days=args.freshness_days,
        explicit_station_list=args.station_list,
        now_utc=now_utc,
    )

    print(f"Selected {len(selected_stations)} stations for validation")
    artifacts: list[StationArtifacts] = []
    skipped_rows: list[dict[str, str]] = []

    for station_id in selected_stations:
        station_rows = manifest[manifest["station_id"] == station_id]
        if station_rows.empty:
            skipped_rows.append(
                {"station_id": station_id, "reason": "station missing from manifest"}
            )
            continue

        station_row = station_rows.iloc[0]
        try:
            print(f"Reading {station_id} from publish bucket...")
            ds = read_station_dataset(args.bucket, args.prefix, station_id)
            artifact = summarize_station(
                station_row=station_row,
                ds=ds,
                now_utc=now_utc,
                freshness_days=args.freshness_days,
                quality_window_days=args.quality_window_days,
                boundary_window_days=args.boundary_window_days,
            )
            artifacts.append(artifact)
            ds.close()
        except Exception as exc:
            skipped_rows.append(
                {"station_id": station_id, "reason": f"{type(exc).__name__}: {exc}"}
            )

    summary_df = pd.DataFrame(
        [artifact.summary_row for artifact in artifacts]
    ).sort_values(["record_type", "station_id"])
    summary_df.to_csv(output_dir / "station_validation_summary.csv", index=False)

    skipped_df = pd.DataFrame(skipped_rows)
    skipped_df.to_csv(output_dir / "skipped_stations.csv", index=False)

    monthly_quality = (
        pd.concat(
            [
                artifact.monthly_quality.assign(station_id=artifact.station_id)
                for artifact in artifacts
            ],
            ignore_index=True,
        )
        if artifacts
        else pd.DataFrame()
    )
    monthly_quality.to_csv(output_dir / "monthly_quality_summary.csv", index=False)

    coverage_frame = pd.DataFrame(
        {
            artifact.station_id: artifact.coverage_delta
            for artifact in artifacts
            if not artifact.coverage_delta.empty
        }
    ).transpose()
    coverage_frame.index.name = "station_id"
    coverage_frame.to_csv(output_dir / "coverage_delta_summary.csv")

    plot_station_timeline(summary_df, output_dir / "selected_station_timeline.png")

    boundary_times = [
        artifact.boundary_time
        for artifact in artifacts
        if artifact.boundary_time is not None
    ]
    boundary_bounds = None
    if boundary_times:
        boundary_bounds = (min(boundary_times), max(boundary_times))
    plot_quality_over_time(
        monthly_quality=monthly_quality,
        output_path=output_dir / "monthly_quality_over_time.png",
        boundary_bounds=boundary_bounds,
    )
    plot_coverage_delta_heatmap(
        coverage_frame=coverage_frame,
        output_path=output_dir / "coverage_delta_heatmap.png",
    )

    for artifact in artifacts:
        plot_station_continuity(
            artifact=artifact,
            output_path=station_plot_dir
            / f"{artifact.station_id}_boundary_continuity.png",
        )

    write_markdown_summary(
        summary_df=summary_df,
        selected_stations=selected_stations,
        output_path=output_dir / "README.md",
        bucket=args.bucket,
        prefix=args.prefix,
    )

    print(f"Wrote validation package to {output_dir}")
    if not skipped_df.empty:
        print(
            f"Skipped {len(skipped_df)} stations; see {output_dir / 'skipped_stations.csv'}"
        )


if __name__ == "__main__":
    main()
