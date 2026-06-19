"""Run ASOSAWOS raw pulls from per-station discovered last timestamps.

This script consumes the successful timestamp inventory produced by
``scripts/misc/discover_last_timestamps_asosawos.py`` and orchestrates
ASASAWOS pulls in one-year buckets.

Default backend: ``ghcnh`` (fetches Parquet from NCEI; covers 1718-present).
Fallback backend: ``isd`` (ISD FTP; frozen as of Aug 2025, no 2026 data).

Because both sources are year-folder based, pulls are grouped by station
timestamp year (YYYY-01-01 lower bound) rather than exact timestamp.
Exact append boundaries are enforced downstream by clean --append.

By default, stations whose metadata ``end_time`` is before the pull cutoff
(``--end-date``, usually today-45d) are classified as offline and skipped.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

import boto3
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from ASOSAWOS_pullftp import get_asosawos_data_ftp, get_wecc_stations
from GHCNh_pull import pull_ghcnh_station_years
from paths import BUCKET_NAME, RAW_WX, WECC_MAR, WECC_TERR


def station_id_to_isd_id(station_id: str) -> str:
    """Convert station id ``ASOSAWOS_72630014733`` to ISD id ``726300-14733``."""
    stripped = station_id.replace("ASOSAWOS_", "")
    return stripped[:6] + "-" + stripped[6:]


def isd_id_to_station_id(isd_id: str) -> str:
    """Convert ISD id ``726300-14733`` to HDP station id ``ASOSAWOS_72630014733``."""
    return "ASOSAWOS_" + isd_id.replace("-", "")


def load_timestamp_inventory(timestamps_csv: str) -> pd.DataFrame:
    """Load timestamp inventory and derive ISD ID + start_year columns."""
    df = pd.read_csv(timestamps_csv)
    required = {"station_id", "last_timestamp"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in {timestamps_csv}: {sorted(missing)}"
        )

    out = df.copy()
    out["last_timestamp"] = pd.to_datetime(out["last_timestamp"], errors="coerce")
    out = out[out["last_timestamp"].notnull()].copy()
    out["isd_id"] = out["station_id"].astype(str).apply(station_id_to_isd_id)
    out["start_year"] = out["last_timestamp"].dt.year.astype(int)
    return out


def resolve_s3_target(directory: str) -> tuple[str, str]:
    """Resolve bucket/prefix from either s3://bucket/prefix or prefix-only input."""
    raw = directory.strip()
    if raw.startswith("s3://"):
        path = raw[len("s3://") :].strip("/")
        if "/" in path:
            bucket, prefix = path.split("/", 1)
        else:
            bucket, prefix = path, ""
        return bucket, prefix
    return BUCKET_NAME, raw.strip("/")


def list_existing_raw_filenames(bucket: str, prefix: str) -> set[str]:
    """List existing object basenames under a raw ASOSAWOS prefix."""
    s3 = boto3.client("s3")
    existing: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page_idx, page in enumerate(
        paginator.paginate(Bucket=bucket, Prefix=prefix), start=1
    ):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj.get("Key", "")
            if key:
                existing.add(os.path.basename(key))
        if page_idx % 100 == 0:
            print("S3 scan progress: " f"pages={page_idx}, files_seen={len(existing)}")
    return existing


def filter_existing_station_year_files(
    stations: pd.DataFrame,
    start_year: int,
    end_year: int,
    existing_filenames: set[str],
) -> tuple[pd.DataFrame, int]:
    """Drop stations only when all expected yearly files already exist in S3."""
    if end_year < start_year:
        return stations.copy(), 0

    years = range(start_year, end_year + 1)

    def station_complete(isd_id: str) -> bool:
        return all(f"{isd_id}-{year}.gz" in existing_filenames for year in years)

    complete_mask = stations["ISD-ID"].astype(str).apply(station_complete)
    skipped = int(complete_mask.sum())
    return stations[~complete_mask].copy(), skipped


def build_missing_year_subgroups(
    stations: pd.DataFrame,
    start_year: int,
    end_year: int,
    existing_filenames: set[str],
    force_years: set[int] | None = None,
) -> tuple[dict[int, pd.DataFrame], int]:
    """Build per-year station groups for missing files only.

    Returns (missing_year_groups, fully_present_station_count).
    """
    if end_year < start_year:
        return {}, 0

    years = range(start_year, end_year + 1)
    missing_by_year: dict[int, list[str]] = {}
    fully_present_count = 0

    station_ids = stations["ISD-ID"].astype(str)
    force_years = force_years or set()

    for isd_id in station_ids:
        missing_years = [
            year
            for year in years
            if year in force_years or f"{isd_id}-{year}.gz" not in existing_filenames
        ]
        if not missing_years:
            fully_present_count += 1
            continue
        for year in missing_years:
            missing_by_year.setdefault(year, []).append(isd_id)

    groups: dict[int, pd.DataFrame] = {}
    for year, isd_ids in missing_by_year.items():
        groups[year] = stations[
            stations["ISD-ID"].astype(str).isin(isd_ids)
        ].drop_duplicates(subset=["ISD-ID"])

    return groups, fully_present_count


def build_pull_groups(
    inventory_df: pd.DataFrame,
    station_df: pd.DataFrame,
    pull_cutoff: date,
    offline_baseline_cutoff: date = date(2022, 1, 1),
    start_year_floor: int = 1980,
    end_year: int | None = None,
    include_offline: bool = False,
) -> tuple[dict[int, pd.DataFrame], dict[str, int], pd.DataFrame, pd.DataFrame]:
    """Build year-bucketed station DataFrames for pull execution.

    Returns groups, stats, skipped_offline_df, unknown_end_time_df.
    """
    inventory = inventory_df.copy()
    inventory["start_year"] = inventory["start_year"].clip(lower=start_year_floor)

    merged = inventory.merge(
        station_df,
        left_on="isd_id",
        right_on="ISD-ID",
        how="left",
        indicator=True,
    )

    merged["end_time_dt"] = pd.to_datetime(merged.get("end_time"), errors="coerce")
    cutoff_ts = pd.Timestamp(pull_cutoff)

    baseline_cutoff_ts = pd.Timestamp(offline_baseline_cutoff)
    old_baseline_mask = merged["last_timestamp"] < baseline_cutoff_ts

    offline_mask = (
        (merged["_merge"] == "both")
        & old_baseline_mask
        & merged["end_time_dt"].notnull()
        & (merged["end_time_dt"] < cutoff_ts)
    )
    unknown_end_mask = (
        (merged["_merge"] == "both")
        & old_baseline_mask
        & merged["end_time_dt"].isnull()
    )

    skipped_offline = merged[offline_mask].copy()
    unknown_end_time = merged[unknown_end_mask].copy()

    unmatched = merged[merged["_merge"] == "left_only"]
    if include_offline:
        matched = merged[merged["_merge"] == "both"].copy()
    else:
        matched = merged[(merged["_merge"] == "both") & (~offline_mask)].copy()

    if end_year is not None:
        matched = matched[matched["start_year"] <= end_year]

    groups: dict[int, pd.DataFrame] = {}
    for year, group in matched.groupby("start_year"):
        station_cols = station_df.columns
        groups[int(year)] = group[station_cols].drop_duplicates(subset=["ISD-ID"])  # type: ignore[index]

    stats = {
        "inventory_rows": int(len(inventory_df)),
        "matched_rows": int(len(matched)),
        "unmatched_rows": int(len(unmatched)),
        "offline_skipped_rows": int(len(skipped_offline)) if not include_offline else 0,
        "unknown_end_time_rows": int(len(unknown_end_time)),
        "year_groups": int(len(groups)),
    }
    return groups, stats, skipped_offline, unknown_end_time


def _run_ghcnh_pull_groups(
    groups: dict[int, pd.DataFrame],
    end_date: str,
    dry_run: bool = False,
    skip_existing: bool = True,
) -> None:
    """Execute year-bucketed pulls via GHCNh (NCEI Parquet backend).

    For each year bucket, all stations in that bucket are pulled from
    ``start_year`` through ``end_year``.  The GHCNh pull function handles
    per-file skip_existing checks internally via S3 head_object.
    """
    end_year = datetime.fromisoformat(end_date).year
    years = sorted(groups)
    total_stations = 0
    all_errors: list[dict] = []

    for idx, start_year in enumerate(years, start=1):
        station_df = groups[start_year]
        isd_ids = station_df["ISD-ID"].dropna().astype(str).tolist()
        station_ids = [isd_id_to_station_id(isd_id) for isd_id in isd_ids]
        total_stations += len(station_ids)

        print(
            f"GHCNh year bucket {idx}/{len(years)} -> start_year={start_year}: "
            f"stations={len(station_ids)}, years={start_year}-{end_year}"
        )

        summary = pull_ghcnh_station_years(
            station_ids=station_ids,
            start_year=start_year,
            end_year=end_year,
            raw_prefix=RAW_WX,
            dry_run=dry_run,
            skip_existing=skip_existing,
        )
        all_errors.extend(summary.get("errors", []))
        print(
            f"  fetched={summary['fetched']}  "
            f"skipped_existing={summary['skipped_existing']}  "
            f"not_found={summary['not_found']}  "
            f"errors={len(summary['errors'])}"
        )

    print(
        "GHCNh pull run complete: "
        f"stations_total={total_stations}, "
        f"year_groups={len(years)}, "
        f"total_errors={len(all_errors)}"
    )
    if all_errors:
        import json

        print("Errors encountered:")
        for err in all_errors[:10]:
            print(f"  {json.dumps(err)}")
        if len(all_errors) > 10:
            print(f"  ... and {len(all_errors) - 10} more")


def run_pull_groups(
    groups: dict[int, pd.DataFrame],
    directory: str,
    end_date: str,
    dry_run: bool = False,
    skip_existing: bool = True,
    backend: str = "ghcnh",
) -> None:
    """Execute year-bucketed pulls.

    Parameters
    ----------
    backend : str
        ``"ghcnh"`` (default) fetches Parquet from NCEI; covers 1718-present.
        ``"isd"`` falls back to the ISD FTP (frozen Aug 2025; no 2026 data).
    """
    years = sorted(groups)
    print(
        "Starting year-bucket pull run: "
        f"year_groups={len(years)}, dry_run={dry_run}, "
        f"skip_existing={skip_existing}, backend={backend}"
    )

    if backend == "ghcnh":
        _run_ghcnh_pull_groups(groups, end_date, dry_run, skip_existing)
        return

    existing_filenames: set[str] = set()
    bucket, prefix = resolve_s3_target(directory)
    if skip_existing:
        print(
            "Scanning existing S3 files to avoid duplicate pulls: "
            f"s3://{bucket}/{prefix}"
        )
        existing_filenames = list_existing_raw_filenames(bucket, prefix)
        print(f"Finished S3 scan: existing_files={len(existing_filenames)}")

    total_stations = 0
    total_skipped_existing = 0
    total_submitted_station_years = 0
    pull_end_year = datetime.fromisoformat(end_date).year

    for idx, year in enumerate(years, start=1):
        stations = groups[year]
        total_stations += len(stations)
        start_date = f"{year}-01-01"
        print(
            f"Year bucket {idx}/{len(years)} -> {year}: "
            f"stations={len(stations)}, start_date={start_date}, end_date={end_date}"
        )

        if skip_existing:
            missing_year_groups, skipped_existing = build_missing_year_subgroups(
                stations=stations,
                start_year=year,
                end_year=pull_end_year,
                existing_filenames=existing_filenames,
                # Current year receives frequent upstream revisions; always refresh.
                force_years={pull_end_year},
            )
            total_skipped_existing += skipped_existing
            if skipped_existing > 0:
                print(
                    "  Stations fully present for all years "
                    f"{year}-{pull_end_year}: {skipped_existing}"
                )

            if not missing_year_groups:
                print(f"  Nothing to pull for year {year}; continuing")
                continue

            for pull_year in sorted(missing_year_groups):
                year_stations = missing_year_groups[pull_year]
                year_start_date = f"{pull_year}-01-01"
                year_end_date = (
                    end_date if pull_year == pull_end_year else f"{pull_year}-12-31"
                )
                total_submitted_station_years += len(year_stations)
                if dry_run:
                    print(
                        f"  Dry run: would pull {len(year_stations)} station files for year {pull_year}"
                    )
                    continue
                print(
                    "  Starting FTP pull for "
                    f"{len(year_stations)} stations in year {pull_year}"
                )
                get_asosawos_data_ftp(
                    year_stations,
                    directory,
                    start_date=year_start_date,
                    end_date=year_end_date,
                    get_all=True,
                )
                print(f"  Completed FTP pull for year {pull_year}")
            continue

        total_submitted_station_years += len(stations)
        if dry_run:
            print(
                f"  Dry run: would pull {len(stations)} station files for year {year}"
            )
            continue
        print(f"  Starting FTP pull for {len(stations)} stations in year {year}")
        get_asosawos_data_ftp(
            stations,
            directory,
            start_date=start_date,
            end_date=end_date,
            get_all=True,
        )
        print(f"  Completed FTP pull for year {year}")

    print(
        "ISD FTP pull run complete: "
        f"stations_in_groups={total_stations}, "
        f"submitted_station_years={total_submitted_station_years}, "
        f"skipped_existing={total_skipped_existing}"
    )


def parse_args() -> argparse.Namespace:
    today = datetime.now(timezone.utc).date()
    default_end_date = today - timedelta(days=45)

    parser = argparse.ArgumentParser(
        description="Pull ASOSAWOS raw data from per-station timestamp inventory"
    )
    parser.add_argument(
        "--timestamps-csv",
        default="temp/asosawos_last_timestamps.csv",
        help="CSV from last-timestamp discovery with station_id,last_timestamp.",
    )
    parser.add_argument(
        "--directory",
        default="1_raw_wx/ASOSAWOS/",
        help="Destination raw S3 prefix.",
    )
    parser.add_argument(
        "--end-date",
        default=str(default_end_date),
        help="Upper bound date (YYYY-MM-DD), typically today-45d.",
    )
    parser.add_argument(
        "--include-offline",
        action="store_true",
        help="Include stations with end_time before pull cutoff instead of skipping them.",
    )
    parser.add_argument(
        "--offline-csv",
        default="temp/asosawos_offline_skipped.csv",
        help="CSV output for stations skipped as offline.",
    )
    parser.add_argument(
        "--unknown-end-time-csv",
        default="temp/asosawos_unknown_end_time.csv",
        help="CSV output for stations with unknown end_time metadata.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for controlled rollout/testing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned year buckets without downloading files.",
    )
    parser.add_argument(
        "--backend",
        default="ghcnh",
        choices=["ghcnh", "isd"],
        help=(
            "Data source backend. 'ghcnh' (default) fetches Parquet from NCEI "
            "and covers 1718-present. 'isd' uses ISD FTP "
            "(frozen Aug 2025; no 2026 data; for historical reproducibility only)."
        ),
    )
    parser.add_argument(
        "--skip-retry",
        action="store_true",
        help="Skip stnlist retry_downloads call after pulls.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Loading timestamp inventory from {args.timestamps_csv}")
    inventory_df = load_timestamp_inventory(args.timestamps_csv)
    print(f"Loaded inventory rows={len(inventory_df)}")
    if args.limit is not None:
        print(f"Applying limit={args.limit}")
        inventory_df = inventory_df.head(args.limit).copy()
        print(f"Rows after limit={len(inventory_df)}")

    pull_cutoff = date.fromisoformat(args.end_date)
    end_year = pull_cutoff.year
    print("Loading ASOSAWOS station metadata from WECC territories/marine shapefiles")
    station_df = get_wecc_stations(WECC_TERR, WECC_MAR)
    print(f"Loaded station metadata rows={len(station_df)}")
    print(
        "Building pull groups with policy: "
        f"offline_baseline_cutoff=2022-01-01, pull_cutoff={pull_cutoff}, end_year={end_year}, "
        f"include_offline={args.include_offline}"
    )
    groups, stats, offline_df, unknown_end_df = build_pull_groups(
        inventory_df=inventory_df,
        station_df=station_df,
        pull_cutoff=pull_cutoff,
        offline_baseline_cutoff=date(2022, 1, 1),
        start_year_floor=1980,
        end_year=end_year,
        include_offline=args.include_offline,
    )

    # Persist classification artifacts for observability and acceptance checks.
    for df, path in (
        (offline_df, args.offline_csv),
        (unknown_end_df, args.unknown_end_time_csv),
    ):
        out = df.copy()
        keep_cols = [
            c
            for c in ["station_id", "isd_id", "last_timestamp", "end_time"]
            if c in out.columns
        ]
        out = out[keep_cols]
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        out.to_csv(path, index=False)

    print(
        "Inventory stats: "
        f"rows={stats['inventory_rows']}, "
        f"matched={stats['matched_rows']}, "
        f"unmatched={stats['unmatched_rows']}, "
        f"offline_skipped={stats['offline_skipped_rows']}, "
        f"unknown_end_time={stats['unknown_end_time_rows']}, "
        f"year_groups={stats['year_groups']}"
    )
    print(f"Offline CSV: {args.offline_csv}")
    print(f"Unknown end_time CSV: {args.unknown_end_time_csv}")

    run_pull_groups(
        groups=groups,
        directory=args.directory,
        end_date=args.end_date,
        dry_run=args.dry_run,
        skip_existing=True,
        backend=args.backend,
    )

    if not args.dry_run and not args.skip_retry:
        token = os.environ.get("HDP_GITHUB_TOKEN")
        if token is None:
            try:
                import config as _config  # type: ignore

                token = _config.token
            except Exception:
                token = None

        if token:
            from stnlist_update_pull import retry_downloads

            retry_downloads(token=token, networks=["ASOSAWOS"])
        else:
            print(
                "Skipping retry_downloads: no token found. "
                "Set HDP_GITHUB_TOKEN or provide scripts/1_pull_data/config.py token."
            )


if __name__ == "__main__":
    main()
