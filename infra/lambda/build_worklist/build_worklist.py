import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    ts_norm = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_norm)
    except ValueError:
        return None


def _to_run_history_item(
    run_id: str,
    station_id: str,
    status: str,
    now: datetime,
) -> dict[str, dict[str, str]]:
    expires_at = int(now.timestamp()) + 60 * 60 * 24 * 180
    return {
        "run_id": {"S": run_id},
        "station_id": {"S": station_id},
        "stage": {"S": "build-worklist"},
        "status": {"S": status},
        "error": {"S": ""},
        "started_at": {"S": now.isoformat()},
        "finished_at": {"S": now.isoformat()},
        "expires_at": {"N": str(expires_at)},
    }


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    watermark_table = os.environ["WATERMARK_TABLE"]
    run_history_table = os.environ["RUN_HISTORY_TABLE"]
    freshness_days = int(os.environ.get("FRESHNESS_DAYS", "7"))

    run_id = (
        event.get("run_id")
        or f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    network_filter = event.get("network", "ASOSAWOS")

    now = datetime.now(timezone.utc)
    threshold_epoch = now.timestamp() - freshness_days * 24 * 60 * 60

    ddb = boto3.client("dynamodb")
    cw = boto3.client("cloudwatch")

    paginator = ddb.get_paginator("scan")
    work_items: list[dict[str, str]] = []
    no_ops: list[dict[str, str]] = []

    for page in paginator.paginate(TableName=watermark_table):
        for item in page.get("Items", []):
            station_id = item["station_id"]["S"]
            network = item.get("network", {}).get("S", "ASOSAWOS")
            last_timestamp = item.get("last_timestamp", {}).get("S")

            if network != network_filter:
                continue

            last_dt = _parse_iso(last_timestamp)
            is_fresh = bool(last_dt and last_dt.timestamp() >= threshold_epoch)

            base = {
                "station_id": station_id,
                "network": network,
                "last_timestamp": last_timestamp or "",
            }

            if is_fresh:
                no_ops.append(base)
                ddb.put_item(
                    TableName=run_history_table,
                    Item=_to_run_history_item(
                        run_id=run_id,
                        station_id=station_id,
                        status="no-op",
                        now=now,
                    ),
                )
            else:
                work_items.append(base)

    result = {
        "run_id": run_id,
        "network": network_filter,
        "total_station_count": len(work_items) + len(no_ops),
        "work_items": work_items,
        "no_ops": no_ops,
    }

    cw.put_metric_data(
        Namespace="HDP/Phase2",
        MetricData=[
            {
                "MetricName": "TotalStations",
                "Dimensions": [{"Name": "Network", "Value": network_filter}],
                "Value": float(result["total_station_count"]),
                "Unit": "Count",
            },
            {
                "MetricName": "WorkItemCount",
                "Dimensions": [{"Name": "Network", "Value": network_filter}],
                "Value": float(len(work_items)),
                "Unit": "Count",
            },
            {
                "MetricName": "NoOpCount",
                "Dimensions": [{"Name": "Network", "Value": network_filter}],
                "Value": float(len(no_ops)),
                "Unit": "Count",
            },
            {
                "MetricName": "FreshnessBreaches",
                "Dimensions": [{"Name": "Network", "Value": network_filter}],
                "Value": float(len(work_items)),
                "Unit": "Count",
            },
        ],
    )

    return result


if __name__ == "__main__":
    sample = handler({"network": "ASOSAWOS"}, None)
    print(json.dumps(sample, indent=2))
