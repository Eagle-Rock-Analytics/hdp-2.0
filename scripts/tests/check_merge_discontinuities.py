"""Check for discontinuities at data source merge boundaries.

Loads raw daily time series for selected stations and plots around
the merge point to identify data value jumps or anomalies.
"""

import argparse
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from paths import PUBLISH_BUCKET, PUBLISH_PREFIX


def load_station_data(bucket: str, prefix: str, station: str) -> xr.Dataset:
    """Load a station's merged zarr from S3."""
    s3_path = f"s3://{bucket}/{prefix}/ASOSAWOS/{station}.zarr"
    try:
        ds = xr.open_dataset(s3_path, engine="zarr")
        return ds
    except Exception as e:
        print(f"Failed to load {station}: {e}", file=sys.stderr)
        return None


def plot_merge_boundary(ds: xr.Dataset, station: str, output_path: Path):
    """Plot tas around the merge boundary (last 60 days and first 60 days after gap)."""
    if "tas" not in ds:
        print(f"  No 'tas' variable in {station}", file=sys.stderr)
        return

    times = ds.time.values
    tas = ds.tas.values.flatten()

    # Find data availability
    valid = ~np.isnan(tas)
    if not valid.any():
        print(f"  No valid data in {station}", file=sys.stderr)
        return

    valid_times = times[valid]
    valid_tas = tas[valid]

    # Look for large gaps (potential merge points)
    time_diffs = np.diff(valid_times).astype("timedelta64[D]").astype(int)
    gaps = np.where(time_diffs > 10)[0]  # >10 day gaps

    fig, axes = plt.subplots(max(1, len(gaps)), 1, figsize=(14, 4 * max(1, len(gaps))))
    if len(gaps) == 1 or len(gaps) == 0:
        axes = [axes]

    for idx, gap_idx in enumerate(gaps[:3]):  # Max 3 gaps per station
        # Around this gap
        before_idx = max(0, gap_idx - 60)
        after_idx = min(len(valid_tas) - 1, gap_idx + 90)

        times_window = valid_times[before_idx:after_idx]
        tas_window = valid_tas[before_idx:after_idx]

        ax = axes[idx] if len(gaps) > 0 else axes[0]
        ax.plot(times_window, tas_window, "o-", markersize=3, linewidth=0.5)
        ax.axvline(
            valid_times[gap_idx], color="red", linestyle="--", alpha=0.7, label="Gap"
        )
        ax.set_title(f"{station} TAS around merge boundary {idx+1}")
        ax.set_ylabel("Temperature (K)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
        ax.grid(True, alpha=0.3)
        ax.legend()

    # If no large gaps, just plot the full series with rolling mean
    if len(gaps) == 0:
        ax = axes[0]
        ax.plot(valid_times, valid_tas, ".", markersize=2, alpha=0.5, label="Daily")

        # Add rolling mean to detect slow drifts
        window = 30
        rolling_mean = np.convolve(valid_tas, np.ones(window) / window, mode="valid")
        rolling_times = valid_times[window // 2 : -window // 2 + 1]
        ax.plot(rolling_times, rolling_mean, "r-", linewidth=1.5, label="30-day mean")

        ax.set_title(f"{station} TAS (continuous, no large gaps)")
        ax.set_ylabel("Temperature (K)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
        ax.grid(True, alpha=0.3)
        ax.legend()

    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Check for merge boundary discontinuities."
    )
    parser.add_argument("--bucket", default=PUBLISH_BUCKET)
    parser.add_argument("--prefix", default=PUBLISH_PREFIX)
    parser.add_argument("--network", default="ASOSAWOS")
    parser.add_argument(
        "--stations",
        default="ASOSAWOS_72578894182,ASOSAWOS_72690124231,ASOSAWOS_72782624141",
        help="Comma-separated station IDs to check",
    )
    parser.add_argument("--output-dir", default="temp/asosawos_validation_figures")

    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stations = [s.strip() for s in args.stations.split(",")]

    for station in stations:
        print(f"Processing {station}...")
        ds = load_station_data(args.bucket, args.prefix, station)
        if ds is None:
            continue

        output_path = output_dir / f"{station}_merge_boundary_check.png"
        plot_merge_boundary(ds, station, output_path)

    print("Done!")


if __name__ == "__main__":
    main()
