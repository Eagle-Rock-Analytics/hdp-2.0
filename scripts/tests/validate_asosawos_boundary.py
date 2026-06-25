"""Validate and visualize ASOSAWOS append continuity in a target publish bucket.

This script is the Phase 1 validation surface for the ASOSAWOS append catch-up.
It reads the Phase 1 merge manifest plus per-station baseline boundary inventory,
opens merged station zarrs from the requested bucket/prefix, and writes:

- a station summary CSV with continuity and freshness diagnostics
- representative station selection CSV
- quality-over-time plots for appended data
- boundary continuity small multiples for temperature, wind, and pressure
- pre/post boundary coverage and flag-rate comparison heatmaps
- a station timeline plot for context

Usage examples
--------------
Validate a representative 10-station sample in the default ASOSAWOS source target:

    uv run python3 scripts/tests/validate_asosawos_boundary.py

Validate a specific test bucket and write figures to a custom directory:

        uv run python3 scripts/tests/validate_asosawos_boundary.py \
      --output-dir temp/asosawos_validation_auto_hdp

Run a narrow smoke check on a specific station:

        uv run python3 scripts/tests/validate_asosawos_boundary.py \
      --stations ASOSAWOS_72020200118 \
      --output-dir temp/asosawos_validation_smoke
"""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr


def load_paths_module() -> object:
    script_root = Path(__file__).resolve().parents[1]
    module_path = script_root / "paths.py"
    spec = importlib.util.spec_from_file_location("hdp_paths", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load paths module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PATHS = load_paths_module()
STATIONS_CSV_PATH = PATHS.STATIONS_CSV_PATH

NETWORK = "ASOSAWOS"
DEFAULT_MANIFEST = "temp/asosawos_merge_manifest.csv"
DEFAULT_BOUNDARIES = "temp/asosawos_last_timestamps.csv"
DEFAULT_OUTPUT_DIR = "temp/asosawos_validation"
DEFAULT_SOURCE_TRANSITION = "2025-10-02"
DEFAULT_BUCKET = "auto-hdp"
DEFAULT_PREFIX = "hdp"

PRIMARY_VARIABLES = {
    "temperature": ["tas", "tas_derived"],
    "wind": ["sfcWind"],
    "pressure": ["ps", "ps_derived", "psl", "ps_altimeter"],
}

RECORD_TYPE_COLORS = {
    "full": "#1f77b4",
    "baseline_only": "#7f7f7f",
    "append_only": "#ff7f0e",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate ASOSAWOS append continuity and generate figures."
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="Bucket containing merged station zarrs for ASOSAWOS validation.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Prefix within the validation bucket.",
    )
    parser.add_argument(
        "--network",
        default=NETWORK,
        help="Network name under the publish prefix.",
    )
    parser.add_argument(
        "--manifest-csv",
        default=DEFAULT_MANIFEST,
        help="Phase 1 merge manifest CSV.",
    )
    parser.add_argument(
        "--boundaries-csv",
        default=DEFAULT_BOUNDARIES,
        help="Per-station baseline boundary inventory CSV.",
    )
    parser.add_argument(
        "--stations-csv",
        default=STATIONS_CSV_PATH,
        help="Authoritative station metadata CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where figures and summary CSVs are written.",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=10,
        help="Maximum number of representative stations to visualize.",
    )
    parser.add_argument(
        "--stations",
        default="",
        help="Comma-separated station list. Overrides representative selection.",
    )
    parser.add_argument(
        "--boundary-window-days",
        type=int,
        default=10,
        help="Days before and after the append boundary to plot.",
    )
    parser.add_argument(
        "--comparison-window-days",
        type=int,
        default=30,
        help="Days before and after the boundary for pre/post comparisons.",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=45,
        help="Freshness threshold for recent data checks.",
    )
    parser.add_argument(
        "--source-transition-date",
        default=DEFAULT_SOURCE_TRANSITION,
        help="Optional vertical marker for the global source transition date.",
    )
    return parser.parse_args()


def resolve_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    lower_map = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        match = lower_map.get(candidate.lower())
        if match is not None:
            return match
    raise ValueError(f"Could not resolve {label} from columns: {sorted(df.columns)}")


def optional_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        match = lower_map.get(candidate.lower())
        if match is not None:
            return match
    return None


def load_manifest(manifest_csv: str) -> pd.DataFrame:
    manifest = pd.read_csv(manifest_csv)
    station_col = resolve_column(manifest, ["station", "station_id"], "station id")
    manifest = manifest.rename(columns={station_col: "station"}).copy()
    manifest["start"] = pd.to_datetime(manifest["start"], errors="coerce")
    manifest["end"] = pd.to_datetime(manifest["end"], errors="coerce")
    return manifest


def load_boundaries(boundaries_csv: str) -> pd.DataFrame:
    boundaries = pd.read_csv(boundaries_csv)
    station_col = resolve_column(
        boundaries, ["station", "station_id"], "boundary station id"
    )
    time_col = resolve_column(
        boundaries,
        ["last_timestamp", "boundary_time", "timestamp"],
        "boundary timestamp",
    )
    boundaries = boundaries.rename(
        columns={station_col: "station", time_col: "boundary_time"}
    ).copy()
    boundaries["boundary_time"] = pd.to_datetime(
        boundaries["boundary_time"], errors="coerce"
    )
    return boundaries[["station", "boundary_time"]]


def load_station_metadata(stations_csv: str, network: str) -> pd.DataFrame:
    stations = pd.read_csv(stations_csv)
    network_col = optional_column(stations, ["network"])
    if network_col is not None:
        stations = stations[stations[network_col] == network].copy()

    station_col = resolve_column(
        stations, ["era-id", "ERA-ID", "station", "station_id"], "station id"
    )
    lat_col = optional_column(stations, ["lat", "latitude", "LATITUDE"])
    lon_col = optional_column(stations, ["lon", "longitude", "LONGITUDE"])

    rename_map = {station_col: "station"}
    if lat_col is not None:
        rename_map[lat_col] = "latitude"
    if lon_col is not None:
        rename_map[lon_col] = "longitude"

    keep_cols = list(rename_map)
    metadata = stations[keep_cols].rename(columns=rename_map).copy()
    for coord in ["latitude", "longitude"]:
        if coord in metadata.columns:
            metadata[coord] = pd.to_numeric(metadata[coord], errors="coerce")
    return metadata.drop_duplicates(subset=["station"])


def build_inventory(
    manifest: pd.DataFrame,
    boundaries: pd.DataFrame,
    metadata: pd.DataFrame,
    freshness_days: int,
) -> pd.DataFrame:
    inventory = manifest.merge(boundaries, on="station", how="left")
    inventory = inventory.merge(metadata, on="station", how="left")
    freshness_cutoff = pd.Timestamp.now("UTC").tz_localize(None) - pd.Timedelta(
        days=freshness_days
    )
    inventory["is_recent"] = inventory["end"] >= freshness_cutoff
    inventory["has_post_boundary_data"] = inventory["end"] > (
        inventory["boundary_time"] + pd.Timedelta(hours=1)
    )
    inventory["status_group"] = np.where(inventory["is_recent"], "active", "inactive")
    return inventory


def pick_evenly_spaced(df: pd.DataFrame, count: int) -> list[str]:
    if df.empty or count <= 0:
        return []

    sort_columns = [
        column for column in ["longitude", "latitude", "end"] if column in df
    ]
    if sort_columns:
        ordered = df.sort_values(sort_columns + ["station"])
    else:
        ordered = df.sort_values("station")

    indices = np.linspace(
        0, len(ordered) - 1, num=min(count, len(ordered)), dtype=int
    ).tolist()
    return ordered.iloc[indices]["station"].astype(str).tolist()


def select_representative_stations(
    inventory: pd.DataFrame, max_stations: int
) -> list[str]:
    active_full = inventory[
        (inventory["record_type"] == "full") & inventory["has_post_boundary_data"]
    ].copy()
    inactive = inventory[
        (~inventory["has_post_boundary_data"])
        | (inventory["record_type"] == "baseline_only")
    ].copy()

    active_target = min(
        len(active_full), max(1, max_stations - min(2, max_stations // 3))
    )
    selected = pick_evenly_spaced(active_full, active_target)

    remaining_slots = max_stations - len(selected)
    selected.extend(pick_evenly_spaced(inactive, remaining_slots))

    if len(selected) < max_stations:
        already = set(selected)
        fallback = inventory.sort_values(["end", "station"], ascending=[False, True])
        for station in fallback["station"].astype(str):
            if station not in already:
                selected.append(station)
                already.add(station)
            if len(selected) >= max_stations:
                break

    return selected[:max_stations]


def build_zarr_url(bucket: str, prefix: str, network: str, station: str) -> str:
    prefix_clean = prefix.strip("/")
    return f"s3://{bucket}/{prefix_clean}/{network}/{station}.zarr"


def choose_primary_variables(columns: list[str]) -> dict[str, str | None]:
    chosen: dict[str, str | None] = {}
    for group, candidates in PRIMARY_VARIABLES.items():
        chosen[group] = next(
            (candidate for candidate in candidates if candidate in columns), None
        )
    return chosen


def load_station_frame(zarr_url: str) -> tuple[pd.DataFrame, dict[str, str | None]]:
    ds = xr.open_zarr(zarr_url)
    try:
        primary = choose_primary_variables(list(ds.data_vars))
        keep_columns: list[str] = []
        for variable in primary.values():
            if variable is None:
                continue
            keep_columns.append(variable)
            flag_column = f"{variable}_eraqc"
            if flag_column in ds.data_vars:
                keep_columns.append(flag_column)

        if not keep_columns:
            raise ValueError(
                f"No expected meteorological variables found in {zarr_url}"
            )

        frame = ds[keep_columns].to_dataframe().reset_index()
        frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
        frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        return frame, primary
    finally:
        ds.close()


def flagged_mask(values: pd.Series) -> pd.Series:
    normalized = values.fillna("").astype(str).str.strip().str.lower()
    return ~normalized.isin(["", "nan", "none", "no_flag"])


def expected_hours(start: pd.Timestamp, end: pd.Timestamp) -> int:
    if pd.isna(start) or pd.isna(end) or end < start:
        return 0
    return int(((end - start) / pd.Timedelta(hours=1))) + 1


def window_metrics(
    frame: pd.DataFrame,
    value_column: str,
    flag_column: str | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[float, float]:
    window = frame[(frame["time"] >= start) & (frame["time"] <= end)].copy()
    hours = expected_hours(start, end)
    if hours <= 0 or window.empty or value_column not in window:
        return float("nan"), float("nan")

    non_null = window[value_column].notna()
    coverage = float(non_null.sum()) / float(hours)

    if flag_column is None or flag_column not in window or non_null.sum() == 0:
        return coverage, float("nan")

    flagged = flagged_mask(window[flag_column]) & non_null
    flag_rate = float(flagged.sum()) / float(non_null.sum())
    return coverage, flag_rate


def monthly_metrics(
    frame: pd.DataFrame,
    value_column: str,
    flag_column: str | None,
    start_time: pd.Timestamp,
) -> tuple[pd.Series, pd.Series]:
    monthly = frame[frame["time"] >= start_time].copy()
    if monthly.empty:
        empty = pd.Series(dtype=float)
        return empty, empty

    monthly = monthly.set_index("time").sort_index()
    coverage = monthly[value_column].notna().resample("MS").mean().astype(float)

    if flag_column is None or flag_column not in monthly:
        flagged = pd.Series(index=coverage.index, dtype=float)
        return coverage, flagged

    available = monthly[value_column].notna()
    flagged = flagged_mask(monthly[flag_column]) & available
    flagged = flagged.resample("MS").mean().astype(float)
    return coverage, flagged


def boundary_metrics(
    frame: pd.DataFrame,
    boundary_time: pd.Timestamp,
    has_post_boundary_data: bool,
) -> dict[str, float | bool | str]:
    times = pd.DatetimeIndex(frame["time"]).sort_values()
    unique_times = times[~times.duplicated()]
    duplicate_count = int(times.duplicated().sum())

    if len(unique_times) > 1:
        diffs = pd.Series(unique_times[1:] - unique_times[:-1])
        max_gap_hours = diffs.max() / pd.Timedelta(hours=1)
    else:
        max_gap_hours = float("nan")

    boundary_hour = pd.Timestamp(boundary_time).floor("h")
    boundary_present = boundary_hour in unique_times

    previous_time = pd.NaT
    next_time = pd.NaT
    if boundary_present:
        boundary_index = unique_times.get_loc(boundary_hour)
        if boundary_index > 0:
            previous_time = unique_times[boundary_index - 1]
        if boundary_index < len(unique_times) - 1:
            next_time = unique_times[boundary_index + 1]
    else:
        insertion = unique_times.searchsorted(boundary_hour)
        if insertion > 0:
            previous_time = unique_times[insertion - 1]
        if insertion < len(unique_times):
            next_time = unique_times[insertion]

    gap_before = (
        float((boundary_hour - previous_time) / pd.Timedelta(hours=1))
        if pd.notna(previous_time)
        else float("nan")
    )
    gap_after = (
        float((next_time - boundary_hour) / pd.Timedelta(hours=1))
        if pd.notna(next_time)
        else float("nan")
    )

    continuity_ok = (
        has_post_boundary_data
        and boundary_present
        and pd.notna(previous_time)
        and pd.notna(next_time)
        and gap_before <= 1.0
        and gap_after <= 1.0
    )

    return {
        "duplicate_timestamps": duplicate_count,
        "max_gap_hours": (
            float(max_gap_hours) if pd.notna(max_gap_hours) else float("nan")
        ),
        "boundary_present": bool(boundary_present),
        "boundary_gap_before_hours": gap_before,
        "boundary_gap_after_hours": gap_after,
        "boundary_continuity_ok": bool(continuity_ok),
    }


def summarize_station(
    station_row: pd.Series,
    frame: pd.DataFrame,
    variables: dict[str, str | None],
    comparison_window_days: int,
    freshness_days: int,
) -> dict[str, object]:
    boundary_time = (
        pd.Timestamp(station_row["boundary_time"])
        if pd.notna(station_row["boundary_time"])
        else pd.NaT
    )
    end_time = (
        pd.Timestamp(station_row["end"]) if pd.notna(station_row["end"]) else pd.NaT
    )
    freshness_cutoff = pd.Timestamp.now("UTC").tz_localize(None) - pd.Timedelta(
        days=freshness_days
    )

    summary: dict[str, object] = {
        "station": station_row["station"],
        "record_type": station_row.get("record_type", ""),
        "status_group": station_row.get("status_group", ""),
        "start": station_row.get("start"),
        "end": end_time,
        "boundary_time": boundary_time,
        "is_recent_within_45_days": bool(
            pd.notna(end_time) and end_time >= freshness_cutoff
        ),
        "has_post_boundary_data": bool(
            station_row.get("has_post_boundary_data", False)
        ),
    }

    summary.update(
        boundary_metrics(
            frame,
            boundary_time=boundary_time,
            has_post_boundary_data=bool(
                station_row.get("has_post_boundary_data", False)
            ),
        )
    )

    pre_start = boundary_time - pd.Timedelta(days=comparison_window_days)
    post_end = min(end_time, boundary_time + pd.Timedelta(days=comparison_window_days))

    for group, variable in variables.items():
        if variable is None:
            summary[f"{group}_variable"] = ""
            continue

        flag_column = (
            f"{variable}_eraqc" if f"{variable}_eraqc" in frame.columns else None
        )
        pre_coverage, pre_flag_rate = window_metrics(
            frame,
            value_column=variable,
            flag_column=flag_column,
            start=pre_start,
            end=boundary_time,
        )
        post_coverage, post_flag_rate = window_metrics(
            frame,
            value_column=variable,
            flag_column=flag_column,
            start=boundary_time,
            end=post_end,
        )

        summary[f"{group}_variable"] = variable
        summary[f"{group}_pre_coverage"] = pre_coverage
        summary[f"{group}_post_coverage"] = post_coverage
        summary[f"{group}_coverage_delta"] = post_coverage - pre_coverage
        summary[f"{group}_pre_flag_rate"] = pre_flag_rate
        summary[f"{group}_post_flag_rate"] = post_flag_rate
        summary[f"{group}_flag_rate_delta"] = post_flag_rate - pre_flag_rate

    return summary


def plot_station_timeline(selection: pd.DataFrame, output_path: Path) -> None:
    timeline = selection.sort_values(["status_group", "end", "station"]).reset_index(
        drop=True
    )
    figure, axis = plt.subplots(figsize=(14, max(4, len(timeline) * 0.55)))

    for idx, row in timeline.iterrows():
        color = RECORD_TYPE_COLORS.get(str(row.get("record_type", "")), "#4c78a8")
        axis.hlines(idx, row["start"], row["end"], color=color, linewidth=6)
        if pd.notna(row.get("boundary_time")):
            axis.scatter(row["boundary_time"], idx, color="#d62728", s=28, zorder=3)

    axis.set_yticks(range(len(timeline)))
    axis.set_yticklabels(timeline["station"])
    axis.set_title("Representative ASOSAWOS record spans and append boundaries")
    axis.set_xlabel("Time")
    axis.set_ylabel("Station")
    axis.grid(axis="x", alpha=0.3)
    axis.xaxis.set_major_locator(mdates.YearLocator())
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    figure.tight_layout()
    figure.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(figure)


def plot_quality_over_time(
    station_payloads: list[dict[str, object]],
    output_path: Path,
    source_transition: pd.Timestamp | None,
) -> None:
    figure, axes = plt.subplots(
        len(PRIMARY_VARIABLES),
        2,
        figsize=(16, 11),
        sharex=False,
    )

    for row_idx, group in enumerate(PRIMARY_VARIABLES):
        coverage_matrix: list[pd.Series] = []
        flagged_matrix: list[pd.Series] = []

        for payload in station_payloads:
            frame = payload["frame"]
            variables = payload["variables"]
            boundary_time = payload["boundary_time"]
            variable = variables.get(group)
            if variable is None:
                continue

            flag_column = (
                f"{variable}_eraqc" if f"{variable}_eraqc" in frame.columns else None
            )
            coverage, flagged = monthly_metrics(
                frame, variable, flag_column, boundary_time
            )
            if not coverage.empty:
                coverage.name = payload["station"]
                coverage_matrix.append(coverage)
            if not flagged.empty:
                flagged.name = payload["station"]
                flagged_matrix.append(flagged)

        coverage_axis = axes[row_idx, 0]
        flagged_axis = axes[row_idx, 1]
        plot_monthly_summary(
            coverage_matrix, coverage_axis, f"{group.title()} coverage"
        )
        plot_monthly_summary(flagged_matrix, flagged_axis, f"{group.title()} flag rate")

        if source_transition is not None:
            for axis in [coverage_axis, flagged_axis]:
                x_min, x_max = axis.get_xlim()
                source_num = mdates.date2num(source_transition)
                if x_min <= source_num <= x_max:
                    axis.axvline(
                        source_transition, color="#d62728", linestyle="--", linewidth=1
                    )

        coverage_axis.set_ylabel("Coverage ratio")
        flagged_axis.set_ylabel("Flagged fraction")

    figure.suptitle("Post-boundary monthly data quality across representative stations")
    figure.tight_layout()
    figure.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(figure)


def plot_monthly_summary(
    series_list: list[pd.Series], axis: plt.Axes, title: str
) -> None:
    axis.set_title(title)
    axis.grid(alpha=0.25)
    if not series_list:
        axis.text(
            0.5, 0.5, "No data", ha="center", va="center", transform=axis.transAxes
        )
        return

    matrix = pd.concat(series_list, axis=1).sort_index()
    for column in matrix.columns:
        axis.plot(matrix.index, matrix[column], color="#bfbfbf", linewidth=1, alpha=0.5)

    median = matrix.median(axis=1, skipna=True)
    lower = matrix.quantile(0.25, axis=1)
    upper = matrix.quantile(0.75, axis=1)
    axis.plot(median.index, median, color="#1f77b4", linewidth=2.2)
    axis.fill_between(median.index, lower, upper, color="#9ecae1", alpha=0.4)
    axis.set_ylim(-0.02, 1.02)
    axis.xaxis.set_major_locator(mdates.YearLocator())
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for label in axis.get_xticklabels():
        label.set_rotation(30)
        label.set_ha("right")


def plot_boundary_small_multiples(
    station_payloads: list[dict[str, object]],
    group: str,
    output_path: Path,
    boundary_window_days: int,
) -> None:
    payloads = [
        payload
        for payload in station_payloads
        if payload["variables"].get(group) is not None
        and payload["has_post_boundary_data"]
    ]
    if not payloads:
        return

    figure, axes = plt.subplots(
        len(payloads),
        1,
        figsize=(15, max(4, len(payloads) * 2.2)),
        sharex=False,
    )
    if len(payloads) == 1:
        axes = [axes]

    for axis, payload in zip(axes, payloads, strict=False):
        frame = payload["frame"]
        variable = payload["variables"][group]
        boundary_time = payload["boundary_time"]
        start = boundary_time - pd.Timedelta(days=boundary_window_days)
        end = boundary_time + pd.Timedelta(days=boundary_window_days)
        window = frame[(frame["time"] >= start) & (frame["time"] <= end)][
            ["time", variable]
        ].dropna()

        if window.empty:
            axis.text(
                0.5,
                0.5,
                "No data in window",
                ha="center",
                va="center",
                transform=axis.transAxes,
            )
        else:
            axis.plot(window["time"], window[variable], color="#1f77b4", linewidth=1.25)
            axis.scatter(window["time"], window[variable], color="#1f77b4", s=8)

        axis.axvline(boundary_time, color="#d62728", linestyle="--", linewidth=1.2)
        axis.set_title(f"{payload['station']}  |  {group}  |  {payload['record_type']}")
        axis.grid(alpha=0.25)
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        for label in axis.get_xticklabels():
            label.set_rotation(30)
            label.set_ha("right")

    axes[-1].set_xlabel("Time")
    figure.tight_layout()
    figure.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(figure)


def plot_pre_post_heatmaps(summary_df: pd.DataFrame, output_path: Path) -> None:
    plot_df = summary_df.set_index("station")
    coverage_delta = pd.DataFrame(
        {group: plot_df.get(f"{group}_coverage_delta") for group in PRIMARY_VARIABLES}
    )
    flag_delta = pd.DataFrame(
        {group: plot_df.get(f"{group}_flag_rate_delta") for group in PRIMARY_VARIABLES}
    )

    figure, axes = plt.subplots(1, 2, figsize=(15, max(4, len(plot_df) * 0.45)))
    render_heatmap(
        axes[0],
        coverage_delta,
        title="Post minus pre boundary coverage",
        vmin=-1,
        vmax=1,
    )
    render_heatmap(
        axes[1],
        flag_delta,
        title="Post minus pre boundary flag rate",
        vmin=-1,
        vmax=1,
    )
    figure.tight_layout()
    figure.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(figure)


def render_heatmap(
    axis: plt.Axes,
    frame: pd.DataFrame,
    title: str,
    vmin: float,
    vmax: float,
) -> None:
    matrix = frame.to_numpy(dtype=float)
    masked = np.ma.masked_invalid(matrix)
    image = axis.imshow(masked, aspect="auto", cmap="coolwarm", vmin=vmin, vmax=vmax)
    axis.set_title(title)
    axis.set_xticks(range(len(frame.columns)))
    axis.set_xticklabels(frame.columns)
    axis.set_yticks(range(len(frame.index)))
    axis.set_yticklabels(frame.index)

    for row_idx in range(masked.shape[0]):
        for col_idx in range(masked.shape[1]):
            value = masked[row_idx, col_idx]
            if np.ma.is_masked(value):
                continue
            axis.text(
                col_idx,
                row_idx,
                f"{float(value):.2f}",
                ha="center",
                va="center",
                fontsize=8,
            )

    plt.colorbar(image, ax=axis, fraction=0.046, pad=0.04)


def parse_station_override(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [station.strip() for station in raw.split(",") if station.strip()]


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(args.manifest_csv)
    boundaries = load_boundaries(args.boundaries_csv)
    metadata = load_station_metadata(args.stations_csv, network=args.network)
    inventory = build_inventory(
        manifest=manifest,
        boundaries=boundaries,
        metadata=metadata,
        freshness_days=args.freshness_days,
    )

    requested_stations = parse_station_override(args.stations)
    if requested_stations:
        selected_stations = requested_stations
    else:
        selected_stations = select_representative_stations(inventory, args.max_stations)

    selection = inventory[inventory["station"].isin(selected_stations)].copy()
    if selection.empty:
        raise ValueError("No stations selected for validation.")

    selection = selection.set_index("station").loc[selected_stations].reset_index()
    selection.to_csv(output_dir / "selected_stations.csv", index=False)

    station_payloads: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    for _, row in selection.iterrows():
        zarr_url = build_zarr_url(
            args.bucket, args.prefix, args.network, row["station"]
        )
        frame, variables = load_station_frame(zarr_url)
        boundary_time = pd.Timestamp(row["boundary_time"]).floor("h")
        payload = {
            "station": row["station"],
            "record_type": row.get("record_type", ""),
            "boundary_time": boundary_time,
            "has_post_boundary_data": bool(row.get("has_post_boundary_data", False)),
            "frame": frame,
            "variables": variables,
        }
        station_payloads.append(payload)
        summary_rows.append(
            summarize_station(
                station_row=row,
                frame=frame,
                variables=variables,
                comparison_window_days=args.comparison_window_days,
                freshness_days=args.freshness_days,
            )
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_dir / "station_summary.csv", index=False)

    plot_station_timeline(selection, output_dir / "station_timeline.png")

    source_transition = pd.to_datetime(args.source_transition_date, errors="coerce")
    plot_quality_over_time(
        station_payloads=station_payloads,
        output_path=output_dir / "quality_over_time.png",
        source_transition=source_transition if pd.notna(source_transition) else None,
    )

    for group in PRIMARY_VARIABLES:
        plot_boundary_small_multiples(
            station_payloads=station_payloads,
            group=group,
            output_path=output_dir / f"boundary_continuity_{group}.png",
            boundary_window_days=args.boundary_window_days,
        )

    plot_pre_post_heatmaps(summary_df, output_dir / "pre_post_quality_deltas.png")

    print(f"Wrote validation outputs to: {output_dir}")
    print(f"Selected stations: {', '.join(selected_stations)}")
    print(
        "Boundary continuity OK count: "
        f"{int(summary_df['boundary_continuity_ok'].fillna(False).sum())} / {len(summary_df)}"
    )
    print(
        "Duplicate-free stations: "
        f"{int((summary_df['duplicate_timestamps'] == 0).sum())} / {len(summary_df)}"
    )


if __name__ == "__main__":
    main()
