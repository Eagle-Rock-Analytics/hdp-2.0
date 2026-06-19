"""GHCNh_pull.py

Download GHCNh (Global Historical Climatology Network — Hourly) Parquet files
from the NCEI Ceph object store and upload them to S3.

GHCNh is the official ISD successor. It covers 1718-2026 and is updated with a
~10-day lag. The NCEI endpoint stores one Parquet file per station per year.

NCEI URL pattern:
  https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{YEAR}/parquet/GHCNh_{GHCNH_ID}_{YEAR}.parquet

S3 output path:
  s3://{HDP_BUCKET}/1_raw_wx/ASOSAWOS/{HDP_STATION_ID}/GHCNh_{GHCNH_ID}_{YEAR}.parquet

Station ID conversion:
  HDP:   ASOSAWOS_72630014733
  ISD:   726300-14733
  GHCNh: USW00014733  (USW + WBAN zero-padded to 8 digits)

Notes
-----
* 404 responses mean a station has no data for that year — logged as a warning,
  not an error.
* Non-404 HTTP errors (5xx, timeout) are retried up to HTTP_RETRY_ATTEMPTS times.
* Requires AWS credentials configured for the target bucket (via ~/.aws/credentials
  or IAM role).

Usage
-----
  # Single station, 2022-2026
  python GHCNh_pull.py --station ASOSAWOS_72630014733 --start-year 2022

  # Multiple stations from a CSV
  python GHCNh_pull.py --stations-csv /path/to/stations.csv

  # Override output bucket
  HDP_BUCKET=my-test-bucket python GHCNh_pull.py --station ASOSAWOS_72630014733

  # Dry run (no S3 writes)
  python GHCNh_pull.py --station ASOSAWOS_72630014733 --dry-run
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from io import StringIO

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from paths import BUCKET_NAME, RAW_WX

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NCEI_BASE_URL = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/"
    "hourly/access/by-year/{year}/parquet/GHCNh_{ghcnh_id}_{year}.parquet"
)

HTTP_TIMEOUT_SECONDS = 60
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_SLEEP_SECONDS = 10

# Pull through end-of-current-year by default
CURRENT_YEAR = datetime.now(tz=timezone.utc).year


# ---------------------------------------------------------------------------
# Station ID helpers
# ---------------------------------------------------------------------------


def hdp_to_ghcnh_id(station_id: str) -> str:
    """Convert HDP ASOSAWOS station ID to GHCNh station ID.

    Parameters
    ----------
    station_id : str
        HDP station ID, e.g. ``ASOSAWOS_72630014733``.

    Returns
    -------
    str
        GHCNh station ID, e.g. ``USW00014733``.

    Raises
    ------
    ValueError
        If station_id does not follow the expected ASOSAWOS format.

    Examples
    --------
    >>> hdp_to_ghcnh_id("ASOSAWOS_72630014733")
    'USW00014733'
    """
    if not station_id.startswith("ASOSAWOS_"):
        raise ValueError(
            f"Expected station_id prefixed with 'ASOSAWOS_', got: {station_id!r}"
        )
    stripped = station_id.replace("ASOSAWOS_", "")
    # ISD concatenated ID is USAF(6) + WBAN(5). Take the last 5 digits as WBAN.
    if len(stripped) != 11:
        raise ValueError(
            f"Expected 11-digit USAF+WBAN after stripping prefix, got {len(stripped)} "
            f"digits in: {station_id!r}"
        )
    wban = stripped[6:]
    return f"USW{int(wban):08d}"


def ghcnh_parquet_url(ghcnh_id: str, year: int) -> str:
    """Return the NCEI HTTPS URL for a single station-year Parquet file."""
    return NCEI_BASE_URL.format(year=year, ghcnh_id=ghcnh_id)


def s3_key(station_id: str, ghcnh_id: str, year: int, prefix: str) -> str:
    """Return the S3 object key for a GHCNh Parquet file.

    Pattern: ``{prefix}/ASOSAWOS/{station_id}/GHCNh_{ghcnh_id}_{year}.parquet``
    """
    return f"{prefix}/ASOSAWOS/{station_id}/GHCNh_{ghcnh_id}_{year}.parquet"


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------


def _fetch_parquet_bytes(
    url: str,
    session: requests.Session,
) -> bytes | None:
    """Fetch a Parquet file from NCEI.

    Returns raw bytes on success, ``None`` on HTTP 404 (station/year absent).
    Retries on transient errors (non-404 HTTP errors, timeouts).

    Raises
    ------
    requests.HTTPError
        On non-404, non-retriable HTTP errors after all retry attempts.
    """
    last_exc: Exception | None = None
    for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT_SECONDS)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            print(
                f"  Timeout on attempt {attempt}/{HTTP_RETRY_ATTEMPTS}: {url}; "
                f"retrying in {HTTP_RETRY_SLEEP_SECONDS}s"
            )
        except requests.exceptions.HTTPError as exc:
            # Don't retry client errors (4xx) other than 504/503
            status = exc.response.status_code if exc.response is not None else 0
            if status in (503, 504):
                last_exc = exc
                print(
                    f"  HTTP {status} on attempt {attempt}/{HTTP_RETRY_ATTEMPTS}: {url}; "
                    f"retrying in {HTTP_RETRY_SLEEP_SECONDS}s"
                )
            else:
                raise
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            print(
                f"  Connection error on attempt {attempt}/{HTTP_RETRY_ATTEMPTS}: {url}; "
                f"retrying in {HTTP_RETRY_SLEEP_SECONDS}s"
            )
        if attempt < HTTP_RETRY_ATTEMPTS:
            time.sleep(HTTP_RETRY_SLEEP_SECONDS)

    raise RuntimeError(
        f"All {HTTP_RETRY_ATTEMPTS} attempts failed for {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if the S3 object already exists."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def _upload_parquet(s3_client, parquet_bytes: bytes, bucket: str, key: str) -> None:
    """Upload raw Parquet bytes to S3."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# Core pull function (importable by pull_asosawos_from_last_timestamps.py)
# ---------------------------------------------------------------------------


def pull_ghcnh_station_years(
    station_ids: list[str],
    start_year: int,
    end_year: int,
    bucket: str = BUCKET_NAME,
    raw_prefix: str = RAW_WX,
    dry_run: bool = False,
    skip_existing: bool = True,
) -> dict:
    """Fetch GHCNh Parquet files for a list of stations over a year range.

    Parameters
    ----------
    station_ids : list[str]
        HDP station IDs, e.g. ``["ASOSAWOS_72630014733", ...]``.
    start_year : int
        First year to pull (inclusive).
    end_year : int
        Last year to pull (inclusive).
    bucket : str
        S3 bucket for output. Defaults to ``HDP_BUCKET`` env var.
    raw_prefix : str
        S3 prefix for raw data (no leading/trailing slash). Default ``1_raw_wx``.
    dry_run : bool
        If True, print planned downloads without writing to S3.
    skip_existing : bool
        If True, skip station-years already present in S3.

    Returns
    -------
    dict
        Summary with keys ``fetched``, ``skipped_existing``, ``not_found``,
        ``errors`` (list of dicts), ``stations_total``, ``years_total``.
    """
    s3_client = boto3.client("s3")
    session = requests.Session()
    session.headers.update({"User-Agent": "hdp-pipeline/2.0 (NCEI GHCNh pull)"})

    errors: list[dict] = []
    fetched = 0
    skipped_existing = 0
    not_found = 0

    years = list(range(start_year, end_year + 1))
    total_pairs = len(station_ids) * len(years)

    print(
        f"GHCNh pull: {len(station_ids)} stations × {len(years)} years "
        f"= {total_pairs} station-years  [dry_run={dry_run}]"
    )
    print(f"  bucket={bucket}  prefix={raw_prefix}  years={start_year}–{end_year}")

    for station_id in station_ids:
        try:
            ghcnh_id = hdp_to_ghcnh_id(station_id)
        except ValueError as exc:
            print(f"  SKIP {station_id}: {exc}")
            errors.append(
                {
                    "station_id": station_id,
                    "year": None,
                    "error": str(exc),
                    "time": datetime.now(tz=timezone.utc).isoformat(),
                }
            )
            continue

        for year in years:
            url = ghcnh_parquet_url(ghcnh_id, year)
            key = s3_key(station_id, ghcnh_id, year, raw_prefix)

            if dry_run:
                print(f"  [DRY RUN] would fetch {url}")
                print(f"            -> s3://{bucket}/{key}")
                continue

            # Skip if already present
            if skip_existing and _object_exists(s3_client, bucket, key):
                skipped_existing += 1
                continue

            try:
                parquet_bytes = _fetch_parquet_bytes(url, session)
            except Exception as exc:
                print(f"  ERROR {station_id} {year}: {exc}")
                errors.append(
                    {
                        "station_id": station_id,
                        "year": year,
                        "error": str(exc),
                        "time": datetime.now(tz=timezone.utc).isoformat(),
                    }
                )
                continue

            if parquet_bytes is None:
                print(f"  NOT FOUND {station_id} / {ghcnh_id} {year} (404)")
                not_found += 1
                continue

            _upload_parquet(s3_client, parquet_bytes, bucket, key)
            size_kb = len(parquet_bytes) / 1024
            print(
                f"  OK {station_id} / {ghcnh_id} {year}  ({size_kb:.0f} KB)  -> s3://{bucket}/{key}"
            )
            fetched += 1

    summary = {
        "stations_total": len(station_ids),
        "years_total": len(years),
        "fetched": fetched,
        "skipped_existing": skipped_existing,
        "not_found": not_found,
        "errors": errors,
    }
    return summary


# ---------------------------------------------------------------------------
# Error log upload helper
# ---------------------------------------------------------------------------


def _upload_error_log(
    errors: list[dict],
    bucket: str,
    raw_prefix: str,
    run_timestamp: str,
) -> None:
    """Upload error summary CSV to S3 if any errors occurred."""
    if not errors:
        return
    s3_client = boto3.client("s3")
    df = pd.DataFrame(errors)
    key = f"{raw_prefix}/ASOSAWOS/pull_errors_ghcnh_{run_timestamp}.csv"
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buf.getvalue(),
        ContentType="text/csv",
    )
    print(f"Error log uploaded -> s3://{bucket}/{key}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pull GHCNh Parquet files from NCEI and upload to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    station_group = parser.add_mutually_exclusive_group(required=True)
    station_group.add_argument(
        "-s",
        "--station",
        nargs="+",
        metavar="STATION_ID",
        help="One or more HDP ASOSAWOS station IDs (e.g. ASOSAWOS_72630014733).",
    )
    station_group.add_argument(
        "--stations-csv",
        metavar="CSV_PATH",
        help="Path to a CSV with a 'station_id' column listing HDP station IDs.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1972,
        help="First year to pull (inclusive). GHCNh covers 1718-present; "
        "WECC ASOS/AWOS stations typically start 1972+.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=CURRENT_YEAR,
        help="Last year to pull (inclusive). Defaults to current year.",
    )
    parser.add_argument(
        "--bucket",
        default=BUCKET_NAME,
        help="S3 bucket for output. Overrides HDP_BUCKET env var.",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download and overwrite files already present in S3.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned downloads without writing to S3.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output (currently a no-op; all output is printed).",
    )
    return parser


def _load_stations_from_csv(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path)
    if "station_id" not in df.columns:
        raise ValueError(
            f"CSV {csv_path!r} must have a 'station_id' column. "
            f"Found: {list(df.columns)}"
        )
    return df["station_id"].dropna().astype(str).tolist()


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.station:
        station_ids = args.station
    else:
        station_ids = _load_stations_from_csv(args.stations_csv)

    if not station_ids:
        print("No stations to process. Exiting.")
        sys.exit(0)

    run_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Run started at {run_ts}")

    summary = pull_ghcnh_station_years(
        station_ids=station_ids,
        start_year=args.start_year,
        end_year=args.end_year,
        bucket=args.bucket,
        raw_prefix=RAW_WX,
        dry_run=args.dry_run,
        skip_existing=not args.no_skip_existing,
    )

    print(
        f"\nDone. fetched={summary['fetched']}  "
        f"skipped_existing={summary['skipped_existing']}  "
        f"not_found={summary['not_found']}  "
        f"errors={len(summary['errors'])}"
    )

    if summary["errors"] and not args.dry_run:
        _upload_error_log(
            summary["errors"],
            bucket=args.bucket,
            raw_prefix=RAW_WX,
            run_timestamp=run_ts,
        )

    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
