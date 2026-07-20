import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    ts_norm = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_norm)
    except ValueError:
        return None


def _normalize_station_ids(raw_station_ids: Any) -> list[str] | None:
    if raw_station_ids is None:
        return None
    if isinstance(raw_station_ids, str):
        station_ids = [item.strip() for item in raw_station_ids.split(",")]
    elif isinstance(raw_station_ids, Iterable):
        station_ids = [str(item).strip() for item in raw_station_ids]
    else:
        raise TypeError("station_ids must be a list or comma-delimited string")

    normalized = [station_id for station_id in station_ids if station_id]
    return normalized or None


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
    station_id_filter = _normalize_station_ids(event.get("station_ids"))
    selected_station_ids = set(station_id_filter or [])
    max_stations_raw = event.get("max_stations")
    max_stations = int(max_stations_raw) if max_stations_raw is not None else None
    if max_stations is not None and max_stations < 1:
        raise ValueError("max_stations must be >= 1")

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
            if selected_station_ids and station_id not in selected_station_ids:
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

    if station_id_filter:
        order_lookup = {
            station_id: index for index, station_id in enumerate(station_id_filter)
        }
        work_items.sort(
            key=lambda item: order_lookup.get(item["station_id"], len(order_lookup))
        )
        no_ops.sort(
            key=lambda item: order_lookup.get(item["station_id"], len(order_lookup))
        )
    else:
        work_items.sort(key=lambda item: item["station_id"])
        no_ops.sort(key=lambda item: item["station_id"])

    if max_stations is not None:
        selected_count = len(work_items)
        work_items = work_items[:max_stations]
        if selected_count > max_stations:
            no_ops = []
        else:
            remaining = max_stations - len(work_items)
            no_ops = no_ops[:remaining]

    result = {
        "run_id": run_id,
        "network": network_filter,
        "requested_station_ids": station_id_filter or [],
        "max_stations": max_stations,
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
