import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


def _count_statuses(items: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"success": 0, "failure": 0, "no-op": 0, "unknown": 0}
    for item in items:
        status = item.get("final_status", {}).get("S") or item.get("status", {}).get(
            "S", "unknown"
        )
        if status in counts:
            counts[status] += 1
        else:
            counts["unknown"] += 1
    return counts


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    run_history_table = os.environ["RUN_HISTORY_TABLE"]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "").strip()

    run_id = event.get("run_id", "")
    network = event.get("network", "ASOSAWOS")
    if not run_id:
        raise ValueError("run_id is required")

    ddb = boto3.client("dynamodb")
    cw = boto3.client("cloudwatch")
    paginator = ddb.get_paginator("query")

    rows: list[dict[str, Any]] = []
    for page in paginator.paginate(
        TableName=run_history_table,
        KeyConditionExpression="run_id = :rid",
        ExpressionAttributeValues={":rid": {"S": run_id}},
    ):
        rows.extend(page.get("Items", []))

    counts = _count_statuses(rows)
    summary = {
        "run_id": run_id,
        "network": network,
        "total": len(rows),
        "success": counts["success"],
        "failure": counts["failure"],
        "no_op": counts["no-op"],
        "unknown": counts["unknown"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    cw.put_metric_data(
        Namespace="HDP/Phase2",
        MetricData=[
            {
                "MetricName": "StationsTotal",
                "Dimensions": [{"Name": "Network", "Value": network}],
                "Value": float(summary["total"]),
                "Unit": "Count",
            },
            {
                "MetricName": "StationSuccessCount",
                "Dimensions": [{"Name": "Network", "Value": network}],
                "Value": float(summary["success"]),
                "Unit": "Count",
            },
            {
                "MetricName": "StationFailureCount",
                "Dimensions": [{"Name": "Network", "Value": network}],
                "Value": float(summary["failure"]),
                "Unit": "Count",
            },
            {
                "MetricName": "StationNoOpCount",
                "Dimensions": [{"Name": "Network", "Value": network}],
                "Value": float(summary["no_op"]),
                "Unit": "Count",
            },
            {
                "MetricName": "PrivateTargetObjectDelta",
                "Dimensions": [{"Name": "Network", "Value": network}],
                "Value": float(summary["success"]),
                "Unit": "Count",
            },
        ],
    )

    if sns_topic_arn:
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"HDP Phase 2 Run Summary: {run_id}",
            Message=json.dumps(summary, indent=2),
        )

    return summary
