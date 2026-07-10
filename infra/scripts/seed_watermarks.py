#!/usr/bin/env python3
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
if str(SCRIPTS_DIR / "misc") not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR / "misc"))

from misc.discover_last_timestamps_asosawos import (  # type: ignore
    build_inventory_rows,
    list_baseline_station_ids,
    load_station_ids,
)
from paths import PUBLISH_PREFIX  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed hdp-watermarks from existing ASOSAWOS published zarr baselines"
    )
    parser.add_argument("--profile", default="neil.AE")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--bucket", default="auto-hdp")
    parser.add_argument(
        "--prefix",
        default=PUBLISH_PREFIX,
        help="Published baseline prefix in the source bucket (defaults to HDP_PUBLISH_PREFIX).",
    )
    parser.add_argument("--network", default="ASOSAWOS")
    parser.add_argument("--watermark-table", default="hdp-watermarks")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3_client = session.client("s3")
    ddb = session.client("dynamodb")

    station_ids = load_station_ids(network=args.network)
    baseline_station_ids = list_baseline_station_ids(
        bucket=args.bucket,
        publish_prefix=args.prefix,
        network=args.network,
        s3_client=s3_client,
    )

    success_rows, problem_rows = build_inventory_rows(
        station_ids=station_ids,
        baseline_station_ids=baseline_station_ids,
        bucket=args.bucket,
        publish_prefix=args.prefix,
        network=args.network,
    )

    print(
        f"seed candidate rows={len(success_rows)} missing_or_error={len(problem_rows)} "
        f"table={args.watermark_table} dry_run={args.dry_run}"
    )

    if args.dry_run:
        return

    now = datetime.now(timezone.utc).isoformat()
    written = 0

    for row in success_rows:
        ddb.put_item(
            TableName=args.watermark_table,
            Item={
                "station_id": {"S": row["station_id"]},
                "last_timestamp": {"S": row["last_timestamp"]},
                "network": {"S": args.network},
                "last_run_status": {"S": "seeded"},
                "updated_at": {"S": now},
            },
        )
        written += 1

    print(f"seed complete written={written} table={args.watermark_table}")


if __name__ == "__main__":
    main()
