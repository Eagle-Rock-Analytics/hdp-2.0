# HDP 2.0 — Phase 2 Architecture Guide

> **Status:** NOT STARTED — pending private-target validation, schedule gating, and Docker image readiness
> **Prerequisites:** Phase 1 ASOSAWOS validated in the private publish target; Docker image working
> **Estimated effort:** ~22 hours
> **Estimated recurring cost:** ~$45/month

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Prerequisites and Dependencies](#3-prerequisites-and-dependencies)
4. [Component Specifications](#4-component-specifications)
   - [Docker Image](#41-docker-image)
   - [ECR Repository](#42-ecr-repository)
   - [S3 Buckets and IAM](#43-s3-buckets-and-iam)
   - [AWS Batch Compute Environment](#44-aws-batch-compute-environment)
   - [Step Functions State Machine](#45-step-functions-state-machine)
   - [EventBridge Schedule](#46-eventbridge-schedule)
   - [CloudWatch and Alerting](#47-cloudwatch-and-alerting)
5. [CDK Stack Breakdown](#5-cdk-stack-breakdown)
6. [Implementation Checklist](#6-implementation-checklist)
7. [Rollout and Verification](#7-rollout-and-verification)
8. [Operating the Pipeline](#8-operating-the-pipeline)

---

## 1. Overview

Phase 2 converts the HDP pipeline from a manually-triggered pcluster batch process
into a fully automated, event-driven monthly pipeline for private publication.
The design goals are:

- **Monthly cadence** — runs on the 1st of each month at 08:00 UTC.
- **Append-only** — processes only new timesteps per station (Phase 0 refactor).
- **Fault-tolerant** — transient station failures are retried once; non-transient
  failures are captured for investigation and mark the run failed after fanout completes.
- **Observable** — CloudWatch dashboard, Step Functions execution history, a durable
  DynamoDB run-history table, and an aggregated email summary per run.
- **Reproducible** — a single Docker image with pinned dependencies handles all
  pipeline stages; no environment drift between runs.

The infrastructure is designed to support all 27 networks, but the first scheduled
rollout only enables ASOSAWOS. ASOSAWOS and OtherISD use `GHCNh_pull.py` for the
pull stage; all other networks use their existing pull scripts unchanged.

**Runtime model (locked 2026-07-07):** the pipeline is *per-station
timestamp-check-first*. A `build-worklist` Lambda reads a DynamoDB **watermark**
table (`hdp-watermarks`, one row per station) and emits the per-station work array;
each station then pulls only from its own last timestamp (`--since`, 7-day cutoff).
Every stage records a per-station outcome (`success` / `failure` / `no-op`) to a
durable DynamoDB **run-history** table (`hdp-run-history`), which the Notify stage
reads to compose a single aggregated summary email per run. This supersedes the
earlier network-pull-then-diff design and the S3-CSV watermark approach. The
DynamoDB state layer, seeder, and `build-worklist` Lambda are tracked in
`hdp-gh5.9`.

---

## 2. Architecture Diagram

```
EventBridge (cron: 1st of month)
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│                 Step Functions State Machine             │
│                                                          │
│  1. Build-Worklist (Lambda)                              │
│     Query DynamoDB hdp-watermarks (one row/station)       │
│     → Array of {station_id, last_timestamp, network}     │
│     → Stations already fresh flagged as no-op            │
│                                                          │
│  2. Pull-Fanout (Map, per-station)                       │
│     → Batch job: run-pull --station=<ID>                 │
│                  --since=<last_timestamp> (7-day cutoff)  │
│     → Output: s3://hdp-staging-pull/{NETWORK}/           │
│                                                          │
│  3. Clean-Fanout (Map, per-station)                      │
│     → Batch job: run-clean --station=<ID> --append       │
│                                                          │
│  4. QAQC-Fanout (Map, per-station, MaxConcurrency=500)   │
│     → Batch job: run-qaqc --station=<ID> --append        │
│     → Output: s3://hdp-staging-qaqc/{NETWORK}/           │
│                                                          │
│  5. Merge-Fanout (Map, per-station)                      │
│     → Batch job: run-merge --station=<ID> --append       │
│     → Output: s3://auto-hdp/hdp/{NETWORK}/{STATION}.zarr │
│     → On success: update hdp-watermarks last_timestamp   │
│                                                          │
│  6. Stationlist-Update (single job)                      │
│     → runs stnlist_update_*.py for all networks          │
│                                                          │
│  7. Notify (Lambda → SNS)                                │
│     → Read hdp-run-history; send ONE aggregated summary  │
│       email (successes / failures / no-ops)              │
└──────────────────────────────────────────────────────────┘
        │              │                 │
  CloudWatch      DynamoDB           SNS Topic
  Dashboard   hdp-watermarks +        (email)
              hdp-run-history

Every fanout stage writes a per-station {stage, status, error} row to the
DynamoDB hdp-run-history table (PK run_id, SK station_id) for durable audit
and for the aggregated Notify email.
```

---

## 3. Prerequisites and Dependencies

Before starting Phase 2 implementation, the following must be in place:

| # | Prerequisite | Owner | Status |
|---|---|---|---|
| P-1 | P1.6 timeseries validation passes against the private publish target | Engineering | ○ |
| P-2 | GHCNh publication/update documentation reviewed before schedule enablement | Engineering | ○ |
| P-3 | Private publish target retention and access policy confirmed | Engineering | ○ |
| P-4 | Docker image builds and passes smoke test | Engineering | ○ |
| P-5 | ECR repository created | Engineering | ○ |
| P-6 | `hdp-staging-pull` and `hdp-staging-qaqc` buckets created | Engineering | ○ |
| P-7 | CDK bootstrapped in target AWS account | Engineering | ○ |

---

## 4. Component Specifications

### 4.1 Docker Image

**Base image:** `ghcr.io/astral-sh/uv:python3.10-bookworm-slim`

The image uses `uv` for dependency installation (consistent with the development
environment). A single image handles all three pipeline entrypoints, selected via
the `CMD` argument:

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# System deps for cartopy (GEOS, PROJ)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev libproj-dev libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY scripts/ ./scripts/
COPY data/ ./data/

# Entrypoints
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
```

**Entrypoint routing** (`docker/entrypoint.sh`):
```bash
#!/bin/bash
case "$1" in
  run-pull)  exec uv run python scripts/1_pull_data/pull_asosawos_from_last_timestamps.py "${@:2}" ;;
  run-clean) exec uv run python scripts/2_clean_data/GHCNh_clean.py "${@:2}" ;;
  run-qaqc)  exec uv run python scripts/3_qaqc_data/QAQC_run_for_single_station.py "${@:2}" ;;
  run-merge) exec uv run python scripts/4_merge_data/MERGE_run_for_single_station.py "${@:2}" ;;
  *)         exec "$@" ;;
esac
```

**Smoke test** (run after every image build):
```bash
docker run --rm hdp:latest run-qaqc --station=CW3E_HDC --dry-run
```

#### Security considerations
- Do NOT embed AWS credentials in the image. Batch jobs receive credentials via the
  task IAM role.
- Use `--frozen` with uv to prevent dependency drift between image builds.
- Scan image with `docker scout` or Trivy before pushing to ECR.
- Set `USER hdp` (non-root) in the Dockerfile.

---

### 4.2 ECR Repository

```python
# CDK (Python)
from aws_cdk import aws_ecr as ecr

repo = ecr.Repository(
    self, "HdpRepo",
    repository_name="hdp",
    lifecycle_rules=[
        ecr.LifecycleRule(max_image_count=10, description="Keep last 10 images")
    ],
    removal_policy=RemovalPolicy.RETAIN,
)
```

Push workflow (CI or manual):
```bash
aws ecr get-login-password --region us-west-2 | docker login --username AWS \
  --password-stdin <ACCOUNT>.dkr.ecr.us-west-2.amazonaws.com
docker build -t hdp:latest .
docker tag hdp:latest <ACCOUNT>.dkr.ecr.us-west-2.amazonaws.com/hdp:latest
docker push <ACCOUNT>.dkr.ecr.us-west-2.amazonaws.com/hdp:latest
```

---

### 4.3 S3 Buckets and IAM

#### Staging buckets (new)
Two new buckets serve as ephemeral staging areas with 30-day lifecycle rules:

| Bucket | Purpose | Lifecycle |
|---|---|---|
| `hdp-staging-pull` | Raw pull output (mirrors `1_raw_wx`) | 30-day object expiry |
| `hdp-staging-qaqc` | QAQC output (mirrors `3_qaqc_wx_v2`) | 30-day object expiry |

```python
from aws_cdk import aws_s3 as s3, Duration

staging_pull = s3.Bucket(
    self, "HdpStagingPull",
    bucket_name="hdp-staging-pull",
    lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(30))],
    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
    encryption=s3.BucketEncryption.S3_MANAGED,
)
```

#### Pipeline IAM role
The Batch job execution role needs:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `wecc-historical-wx`,
  `hdp-staging-pull`, `hdp-staging-qaqc`
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the private publish target
  (`auto-hdp/hdp/`) so merge can read the baseline zarr and write the updated zarr

`cadcat` publishing is explicitly out of scope for the first private Phase 2 rollout.
If HDP output is made public later, add a separate publish/promotion path rather than
hard-wiring cross-account writes into the initial automation.

---

### 4.4 AWS Batch Compute Environment

> **Decision (2026-07-08): amd64-only for the ASOSAWOS rollout.** `c7g.large`
> (arm64 / Graviton) is intentionally dropped because the published image
> (`hdp:2.0.0`) is single-arch `linux/amd64`. Revisit a multi-arch image
> (amd64 + arm64 via buildx / manifest list) when scaling beyond ASOSAWOS to all
> 27 networks, where the wider Spot pool and ~15–20% Graviton price/perf become
> material. Tracked under `hdp-gh5.8`; compute-env decision on `hdp-gh5.4`.

#### Compute environment
```python
from aws_cdk import aws_batch as batch, aws_ec2 as ec2

compute_env = batch.ManagedEc2EcsComputeEnvironment(
    self, "HdpCompute",
    instance_types=[
        # amd64 only — c7g (arm64) dropped until a multi-arch image exists.
        ec2.InstanceType("c7i-flex.large"),
        ec2.InstanceType("m7i-flex.large"),
    ],
    use_optimal_instance_classes=False,
    spot=True,
    spot_bid_percentage=60,
    max_v_cpus=512,
    vpc=vpc,
    vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
)
```

#### Job queue
```python
job_queue = batch.JobQueue(
    self, "HdpQueue",
    compute_environments=[
        batch.OrderedComputeEnvironment(compute_environment=compute_env, order=1)
    ],
    priority=10,
)
```

#### Job definitions

| Job | vCPU | Memory | Timeout | Command |
|---|---|---|---|---|
| `hdp-pull-job` | 1 | 2 GB | 2h | `run-pull --network={network} --since={date}` |
| `hdp-clean-job` | 1 | 4 GB | 2h | `run-clean --station={station_id} --append` |
| `hdp-qaqc-job` | 1 | 4 GB | 2h | `run-qaqc --station={station_id} --append` |
| `hdp-merge-job` | 1 | 4 GB | 1h | `run-merge --station={station_id} --append` |

All job definitions reference the same ECR image. Environment variables are injected
at submission time by the Step Functions state machine (`HDP_BUCKET`, `HDP_PUBLISH_BUCKET`,
`HDP_PUBLISH_PREFIX`). For the private rollout, `HDP_PUBLISH_BUCKET` points at the
durable private target and not `cadcat`.

---

### 4.5 Step Functions State Machine

The state machine is defined in AWS CDK using the `aws_stepfunctions` and
`aws_stepfunctions_tasks` constructs.

#### Stage 1 — Build-Worklist (Lambda)
A small Lambda queries the DynamoDB `hdp-watermarks` table (one row per station)
and returns the per-station work array `[{station_id, last_timestamp, network}]`
for the enabled network(s). Stations whose `last_timestamp` is already within the
freshness window (i.e. no new GHCNh data expected under the 7-day pull cutoff and
~10-day GHCNh lag) are flagged `no-op` and excluded from the pull array; the no-op
is still recorded to `hdp-run-history` for that run.

```python
build_worklist_lambda = lambda_.Function(
    self, "BuildWorklist",
    runtime=lambda_.Runtime.PYTHON_3_10,
    handler="build_worklist.handler",
    code=lambda_.Code.from_asset("lambda/build_worklist/"),
    timeout=Duration.minutes(5),
    memory_size=512,
    environment={
        "WATERMARK_TABLE": watermark_table.table_name,
        "RUN_HISTORY_TABLE": run_history_table.table_name,
        "PULL_CUTOFF_DAYS": "7",
    },
)
```

The watermark table, its seeder (initial population from the published station
zarrs), and this Lambda are provisioned in `hdp-gh5.9`. If the work array is empty
(all stations fresh), the run is a successful no-op unless source freshness
thresholds are breached.

#### Stage 2 — Pull-Fanout (Map, per-station)
Parallel `Map` over the work array from Stage 1. Each iteration submits a
per-station Batch pull job that fetches only from that station's last timestamp
(`--since`, floored to the 7-day cutoff):
```python
pull_job = sfn_tasks.BatchSubmitJob(
    self, "PullJob",
    job_name=sfn.JsonPath.string_at("$.station_id"),
    job_definition_arn=pull_job_def.job_definition_arn,
    job_queue_arn=job_queue.job_queue_arn,
    container_overrides=sfn_tasks.BatchContainerOverrides(
        command=sfn.JsonPath.list_at("$.pull_command"),
        environment={
            "HDP_BUCKET": "hdp-staging-pull",
        },
    ),
    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
)

pull_fanout = sfn.Map(
    self, "PullFanout",
    items_path=sfn.JsonPath.string_at("$.worklist"),
    max_concurrency=500,
).iterator(pull_job)
```

#### Stages 3–5 — Clean / QAQC / Merge Fanout
Each stage is a `Map` state over the per-station work array. Clean and QAQC fan
out per station (QAQC uses `MaxConcurrency=500`; Batch enforces the real limit via
the compute environment `maxVCpus`):

```python
qaqc_fanout = sfn.Map(
    self, "QaqcFanout",
    items_path=sfn.JsonPath.string_at("$.worklist"),
    max_concurrency=500,
).iterator(qaqc_job_task)
```

Each task passes `--append` to the job command. The **merge** stage (Stage 5) is
the watermark writer: on a successful per-station merge into `auto-hdp`, the job
updates that station's row in `hdp-watermarks` with the new `last_timestamp`. Every
stage writes a `{run_id, station_id, stage, status, error}` row to `hdp-run-history`.

Failed stations are classified as transient or non-transient. Transient failures
are retried once using Step Functions `Retry` plus the Batch retry strategy.
Non-transient failures are recorded to `hdp-run-history`, processing continues for
remaining stations, and the overall execution ends in failure-for-investigation if
any non-transient station failure remains after the retry path.

#### Stage 6 — Stationlist-Update
A single Batch job runs `stnlist_update_qaqc.py` and `stnlist_update_merge.py`
for all networks in sequence. This stage runs only after all merge jobs complete.

#### Stage 7 — Notify
A Notify Lambda reads the `hdp-run-history` rows for this `run_id`, composes a
single aggregated summary, and publishes it via SNS as **one email per run** to
`neil.schroeder@eaglerockanalytics.com` (not one email per station). The summary
includes:
- Total stations processed, plus counts of success / failure / no-op.
- The list of failed station IDs with their error classes.
- Wall-clock duration of the state machine execution.
- Link to the CloudWatch dashboard.
- Source freshness status and whether the whole run was a no-op.

A separate CloudWatch alarm still fires an immediate SNS alert on a
state-machine-level `FAILED` execution.

---

### 4.6 EventBridge Schedule

```python
from aws_cdk import aws_events as events, aws_events_targets as targets

rule = events.Rule(
    self, "HdpMonthlyRun",
    schedule=events.Schedule.cron(
        minute="0",
        hour="8",
        day="1",
        month="*",
        year="*",
    ),
    description="Monthly HDP pipeline run — 1st of month at 08:00 UTC",
    enabled=False,
)

rule.add_target(targets.SfnStateMachine(
    state_machine,
    input=events.RuleTargetInput.from_object({
        "networks": NETWORK_LIST,
        "run_id": events.EventField.from_path("$.id"),
        "run_date": events.EventField.from_path("$.time"),
        "triggered_by": "eventbridge-monthly",
    }),
))
```

**Important:** Create the rule disabled. Manually trigger the state machine until
the service-by-service walkthrough passes, verify the full execution end-to-end,
then deliberately inject a failure to confirm the email alert path works before
enabling the monthly schedule.

---

### 4.7 CloudWatch and Alerting

#### CloudWatch dashboard
The dashboard should show:
- **Batch success rate** — `SucceededJobCount / (SucceededJobCount + FailedJobCount)` per stage.
- **Step Functions execution duration** — per state, 7-day trailing average.
- **Private publish target object count delta** — compare count pre- vs post-run.
- **Station failure breakdown** — top 10 failed stations by error message.
- **Source freshness** — newest available source data age by network.

#### SNS alerting
Create email notifications for:
1. **Step Functions execution FAILED** — any unhandled failure in the state machine.
2. **Any non-transient station failure after the single retry path**.
3. **Source freshness breach** — warning when source age exceeds 60 days; alert when
  source age exceeds 75 days or the same network has two consecutive monthly no-op runs.

```python
from aws_cdk import aws_sns as sns, aws_cloudwatch as cw, aws_cloudwatch_actions as cw_actions

alert_topic = sns.Topic(self, "HdpAlerts", topic_name="hdp-pipeline-alerts")

failure_alarm = cw.Alarm(
    self, "SfnFailureAlarm",
    metric=cw.Metric(
        namespace="AWS/States",
        metric_name="ExecutionsFailed",
        dimensions_map={"StateMachineArn": state_machine.state_machine_arn},
        period=Duration.hours(1),
    ),
    threshold=1,
    evaluation_periods=1,
    alarm_description="HDP state machine execution failed",
)
failure_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))
```

---

## 5. CDK Stack Breakdown

The `infra/` directory will contain five CDK stacks. They should be deployed in order:

| Stack | Class | Depends on | Contents |
|---|---|---|---|
| `BucketsStack` | `HdpBucketsStack` | — | Staging buckets (`hdp-staging-pull`, `hdp-staging-qaqc`); private publish target policy/retention |
| `StateStack` | `HdpStateStack` | `BucketsStack` | DynamoDB `hdp-watermarks` + `hdp-run-history` tables (on-demand); `build-worklist` Lambda (see `hdp-gh5.9`) |
| `ComputeStack` | `HdpComputeStack` | `StateStack` | ECR repo; Batch compute env, job queue, 4 job definitions; IAM role (incl. DynamoDB read/write) |
| `OrchestratorStack` | `HdpOrchestratorStack` | `ComputeStack` | Step Functions state machine; `build-worklist` + `notify` Lambdas; SNS email topic |
| `ScheduleStack` | `HdpScheduleStack` | `OrchestratorStack` | EventBridge monthly cron (disabled by default); CloudWatch dashboard; failure alarms |

```
infra/
├── app.py                     # CDK app entry point
├── stacks/
│   ├── buckets_stack.py
│   ├── state_stack.py
│   ├── compute_stack.py
│   ├── orchestrator_stack.py
│   └── schedule_stack.py
└── lambda/
    ├── build_worklist/        # Stage 1: read hdp-watermarks → work array
    │   ├── build_worklist.py
    │   └── requirements.txt
    └── notify/                # Stage 7: read hdp-run-history → summary email
        ├── notify.py
        └── requirements.txt
```

```python
# infra/app.py
app = cdk.App()
env = cdk.Environment(account=os.environ["CDK_ACCOUNT"], region="us-west-2")

buckets  = HdpBucketsStack(app,      "HdpBuckets",      env=env)
state    = HdpStateStack(app,        "HdpState",        buckets_stack=buckets,    env=env)
compute  = HdpComputeStack(app,      "HdpCompute",      state_stack=state,        env=env)
orch     = HdpOrchestratorStack(app, "HdpOrchestrator", compute_stack=compute,    env=env)
schedule = HdpScheduleStack(app,     "HdpSchedule",     orchestrator_stack=orch,  env=env)
```

---

## 6. Implementation Checklist

### P2.1 — Containerize (~8h)
- [ ] Write `Dockerfile` from `environment/` using uv base image
- [ ] Write `docker/entrypoint.sh` routing `run-pull`, `run-clean`, `run-qaqc`, `run-merge`
- [ ] Build and verify all four entrypoints locally
- [ ] Run smoke test: `docker run hdp:latest run-qaqc --station=CW3E_HDC --dry-run`
- [ ] GHCNh smoke test: `docker run hdp:latest run-pull --station=ASOSAWOS_72290993115 --since=2026-06-01 --dry-run`
- [ ] Push to ECR
- [ ] Document image tag strategy (semver vs `:latest`)

### P2.2 — Staging buckets + IAM (~3h)
- [ ] Create `HdpBucketsStack` CDK stack
- [ ] Create `hdp-staging-pull` (30-day lifecycle)
- [ ] Create `hdp-staging-qaqc` (30-day lifecycle)
- [ ] Confirm the reused private publish target has durable retention and no auto-expiry on merged zarrs
- [ ] Create `HdpBatchJobRole` IAM role with correct S3 policies
- [ ] Verify read/write access to the private publish target with a scratch object test

### P2.3 — Batch compute (~3h)
- [ ] Create `HdpComputeStack` CDK stack
- [ ] Compute environment: EC2 Spot, `c7i-flex.large` / `m7i-flex.large` (amd64 only; `c7g` deferred), maxVCpus=512
- [ ] Job queue with priority ordering
- [ ] Job definitions: pull, clean, qaqc, merge
- [ ] Manual Batch job submission test (bypassing Step Functions)
- [ ] Verify Batch job logs appear in CloudWatch

### P2.4 — Step Functions (~8h)
- [ ] Create `HdpOrchestratorStack` CDK stack
- [ ] Write `build_worklist` Lambda (read `hdp-watermarks`, emit per-station work array, flag no-ops under 7-day cutoff)
- [ ] Write `notify` Lambda (read `hdp-run-history` for the run, send one aggregated summary email)
- [ ] State machine: Build-Worklist → Pull-Fanout → Clean-Fanout → QAQC-Fanout → Merge-Fanout → Stationlist-Update → Notify
- [ ] Merge stage updates `hdp-watermarks`; every stage writes `hdp-run-history`
- [ ] Implement zero-work no-op behavior with source freshness reporting
- [ ] Implement one retry for transient failures and fail-for-investigation behavior for non-transient station failures
- [ ] Unit test `build_worklist` and `notify` Lambdas locally with mocked DynamoDB
- [ ] Manual trigger of state machine with 1-station and 5-station ASOSAWOS subsets
- [ ] Verify email alert delivery with an intentionally broken station

### P2.5 — EventBridge + observability (~3h)
- [ ] Create `HdpScheduleStack` CDK stack
- [ ] Review GHCNh update/publication documentation and current object timestamps before enabling the monthly rule
- [ ] EventBridge monthly cron rule pointing to state machine, created disabled by default
- [ ] CloudWatch dashboard (Batch success rate, SFN duration, private-target object delta, source freshness)
- [ ] SNS failure alarm (SFN execution failure)
- [ ] SNS email subscription and failure notifications
- [ ] Manually trigger EventBridge rule; verify end-to-end execution while disabled-by-default remains in place
- [ ] Trigger email alert with intentional failure; verify delivery

---

## 7. Rollout and Verification

### Pre-rollout gate (P1.6 first)
Phase 2 should not be scheduled until P1.6 passes:
1. `notebooks/validation_asosawos_boundary.ipynb` (to be created) plots 5+ stations
   at the Oct 2025 ISD→GHCNh boundary with no visible discontinuity.
2. `scripts/tests/` validators pass against `s3://auto-hdp/hdp/ASOSAWOS/`.
3. `data-access/data_access.ipynb` examples work against the private target.
4. GHCNh publication/update docs and current object timestamps are reviewed to confirm
  the monthly cadence remains appropriate.

### Staged rollout sequence

1. **ECR / image walkthrough** — build the Docker image locally, run the QAQC and GHCNh pull smoke tests, and push a tagged image to ECR only after both succeed.
2. **Buckets / IAM walkthrough** — deploy `BucketsStack`, verify staging bucket lifecycle rules, and verify that the private publish target is durable and writable without enabling any schedule.
3. **Batch walkthrough** — deploy `ComputeStack`, submit one pull job, one clean job, one QAQC job, and one merge job manually for a known ASOSAWOS station; compare the merge output against the pcluster-produced zarr.
4. **State machine walkthrough** — deploy `OrchestratorStack`, manually run a 1-station subset, then a 5-station ASOSAWOS subset, then a zero-touch/no-op scenario.
5. **Failure-path walkthrough** — inject one transient failure to verify the single retry path, then one non-transient failure to verify fail-for-investigation behavior and email notification.
6. **Schedule walkthrough** — deploy `ScheduleStack` with the EventBridge rule disabled, confirm alarms and dashboard wiring, and keep the rule disabled until the manual walkthrough is fully green.
7. **ASOSAWOS first scheduled run** — enable the monthly rule for ASOSAWOS only and monitor the first automated run in CloudWatch and email.
8. **Progressive expansion** — add one non-ISD network after ASOSAWOS is stable, then expand further network-by-network.

---

## 8. Operating the Pipeline

### Manual trigger (ad-hoc catch-up or re-run)
```bash
# Trigger state machine for the currently enabled rollout set
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-west-2:<ACCOUNT>:stateMachine:HdpPipeline \
  --input '{"networks": ["ASOSAWOS","HADS","CW3E"], "triggered_by": "manual"}' \
  --profile neil.AE

# Monitor execution
aws stepfunctions describe-execution \
  --execution-arn <EXECUTION_ARN> \
  --profile neil.AE
```

### Pause or enable the monthly run
```bash
# Disable EventBridge rule (does not delete it)
aws events disable-rule --name HdpMonthlyRun --region us-west-2 --profile neil.AE

# Re-enable
aws events enable-rule --name HdpMonthlyRun --region us-west-2 --profile neil.AE
```

### Checking for failed stations
Failed stations are logged to CloudWatch Logs by the Batch jobs and to the per-run
error summary in S3 (following the existing error accumulation pattern). To list
failures from a specific run:
```bash
# Error CSVs follow the existing pattern: uploaded to S3 at end of Batch job
aws s3 ls s3://wecc-historical-wx/qaqc_logs/ --profile neil.AE | grep $(date +%Y-%m)
```

### Updating the pipeline code
1. Make changes to `scripts/` in the `hdp-2.0` repo.
2. Build and push a new Docker image to ECR.
3. Update the Batch job definition to reference the new image tag (or use `:latest`
   with force-new-revision).
4. No CDK re-deploy is needed unless infrastructure changes were made.

### Debugging a Batch job failure
```bash
# Get job details
aws batch describe-jobs --jobs <JOB_ID> --profile neil.AE

# Get CloudWatch log stream (logStreamName from describe-jobs output)
aws logs get-log-events \
  --log-group-name /aws/batch/job \
  --log-stream-name <LOG_STREAM_NAME> \
  --profile neil.AE
```

### Cost monitoring
The CloudWatch dashboard includes an estimated monthly cost widget based on Batch
vCPU-hours. Set a budget alert at $100/month in AWS Budgets to catch runaway
executions early.

---

## Appendix: Network Family Summary

| Network | Count | Pull script | GHCNh migration needed? |
|---|---|---|---|
| ASOSAWOS | 455 | `GHCNh_pull.py` | ✓ Done |
| OtherISD | ~200 | `GHCNh_pull.py` | ○ Not started |
| HADS | ~5,000 | `HADS_pull.py` | No |
| MADIS | ~3,000 | `MADIS_pull.py` | No |
| CIMIS | ~145 | `CIMIS_pull.py` | No |
| CW3E | ~200 | `CW3E_pull.py` | No |
| SCAN/SNOTEL | ~800 | `SCANSNOTEL_pull.py` | No |
| MARITIME | ~50 | `MARITIME_pull.py` | No |

> Total station count: ~15,000 across all 27 networks.
> Only ASOSAWOS and OtherISD require GHCNh migration (both were ISD-sourced).
