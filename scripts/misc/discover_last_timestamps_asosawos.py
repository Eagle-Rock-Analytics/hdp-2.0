"""Discover per-station baseline timestamps for ASOSAWOS append runs.

This utility reads the authoritative ASOSAWOS station list, checks which
stations have baseline merged zarrs in the publish bucket, and emits:

1. A CSV of (station_id, last_timestamp) for stations with readable baselines.
2. A CSV of (station_id, status, detail) for missing, empty, or unreadable
   baselines that should fall back to full-pull handling.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from paths import PUBLISH_BUCKET, PUBLISH_PREFIX, STATIONS_CSV_PATH


def load_station_ids(
    stations_csv_path: str = STATIONS_CSV_PATH, network: str = "ASOSAWOS"
) -> list[str]:
    """Return authoritative station ids for *network* from the master station list."""
    stations_df = pd.read_csv(stations_csv_path)
    network_df = stations_df[stations_df["network"] == network]
    station_ids = sorted(network_df["era-id"].dropna().astype(str).unique().tolist())
    return station_ids


def list_baseline_station_ids(
    bucket: str,
    publish_prefix: str,
    network: str = "ASOSAWOS",
    s3_client=None,
) -> set[str]:
    """List station ids with at least one object under the baseline zarr prefix."""
    s3_client = s3_client or boto3.client("s3")
    normalized_prefix = f"{publish_prefix.strip('/')}/{network}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    station_ids: set[str] = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            suffix = key[len(normalized_prefix) :]
            station_store = suffix.split("/", 1)[0]
            if station_store.endswith(".zarr"):
                station_ids.add(station_store[: -len(".zarr")])

    return station_ids


def read_baseline_last_timestamp(
    station_id: str,
    bucket: str = PUBLISH_BUCKET,
    publish_prefix: str = PUBLISH_PREFIX,
    network: str = "ASOSAWOS",
) -> tuple[datetime | None, str, str]:
    """Read the last time coordinate from a station baseline zarr.

    Returns ``(timestamp, status, detail)`` where ``status`` is one of
    ``ok``, ``missing``, ``empty``, or ``error``.
    """
    zarr_url = f"s3://{bucket}/{publish_prefix.strip('/')}/{network}/{station_id}.zarr"
    ds = None
    try:
        ds = xr.open_zarr(zarr_url)
        if "time" not in ds or ds.time.size == 0:
            return None, "empty", "missing or empty time coordinate"
        last = pd.Timestamp(ds.time.values[-1]).to_pydatetime()
        return last, "ok", ""
    except FileNotFoundError:
        return None, "missing", "zarr not found"
    except Exception as exc:
        return None, "error", f"{type(exc).__name__}: {exc}"
    finally:
        if ds is not None:
            ds.close()


def build_inventory_rows(
    station_ids: list[str],
    baseline_station_ids: set[str],
    bucket: str = PUBLISH_BUCKET,
    publish_prefix: str = PUBLISH_PREFIX,
    network: str = "ASOSAWOS",
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Return successful timestamp rows and missing/error rows."""
    success_rows: list[dict[str, str]] = []
    problem_rows: list[dict[str, str]] = []

    for station_id in station_ids:
        if station_id not in baseline_station_ids:
            problem_rows.append(
                {
                    "station_id": station_id,
                    "status": "missing",
                    "detail": "baseline zarr absent from publish bucket",
                }
            )
            continue

        last_timestamp, status, detail = read_baseline_last_timestamp(
            station_id=station_id,
            bucket=bucket,
            publish_prefix=publish_prefix,
            network=network,
        )
        if status == "ok" and last_timestamp is not None:
            success_rows.append(
                {
                    "station_id": station_id,
                    "last_timestamp": pd.Timestamp(last_timestamp).isoformat(),
                }
            )
        else:
            problem_rows.append(
                {
                    "station_id": station_id,
                    "status": status,
                    "detail": detail,
                }
            )

    return success_rows, problem_rows


def write_rows(
    rows: list[dict[str, str]], output_path: str, columns: list[str]
) -> None:
    """Write rows to CSV, ensuring parent directories exist."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(output, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover per-station last timestamps for ASOSAWOS baselines"
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
        "--network",
        default="ASOSAWOS",
        help="Network to inventory.",
    )
    parser.add_argument(
        "--stations-csv",
        default=STATIONS_CSV_PATH,
        help="Authoritative station list CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="temp/asosawos_last_timestamps.csv",
        help="Output CSV for successful timestamp reads.",
    )
    parser.add_argument(
        "--missing-csv",
        default="temp/asosawos_last_timestamps_missing.csv",
        help="Output CSV for missing, empty, or unreadable baselines.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    station_ids = load_station_ids(args.stations_csv, network=args.network)
    baseline_station_ids = list_baseline_station_ids(
        bucket=args.bucket,
        publish_prefix=args.prefix,
        network=args.network,
    )
    success_rows, problem_rows = build_inventory_rows(
        station_ids=station_ids,
        baseline_station_ids=baseline_station_ids,
        bucket=args.bucket,
        publish_prefix=args.prefix,
        network=args.network,
    )

    write_rows(success_rows, args.output_csv, ["station_id", "last_timestamp"])
    write_rows(problem_rows, args.missing_csv, ["station_id", "status", "detail"])

    print(
        f"Wrote {len(success_rows)} station timestamps and {len(problem_rows)} missing/error rows."
    )
    print(f"Success CSV: {args.output_csv}")
    print(f"Missing/error CSV: {args.missing_csv}")


if __name__ == "__main__":
    main()
