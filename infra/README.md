# HDP Phase 2 Infra

This directory contains AWS CDK infrastructure for HDP Phase 2.

## gh5.9 scope
- DynamoDB tables: `hdp-watermarks`, `hdp-run-history`
- Lambda: `build-worklist`
- Lambda: `summarize-run` (aggregates per-run status for notify)
- Seeder script: populates `hdp-watermarks` from `s3://auto-hdp/hdp/ASOSAWOS/`

## gh5.4 / orchestration scope
- AWS Batch compute, queue, and job definitions (clean/qaqc/merge)
- Step Functions state machine: `hdp-phase2-state-machine`
- Per-station workflow from `build-worklist` `work_items`
- Explicit NOOP handling for stage exit code `3` via failure catch-and-route

## gh5.5 orchestration scope
- Added pull stage (`GHCNh_pull.py`) before clean/qaqc/merge in per-station fanout
- Added per-stage run-history updates in `hdp-run-history` rows keyed by (`run_id`, `station_id`)
- Added one retry policy for transient Step Functions task failures
- Added watermark updates after successful merge
- Added `stationlist-update` single Batch stage (`stnlist_update_merge.py ASOSAWOS`)
- Added notify stage via `hdp-summarize-run` Lambda and fail-for-investigation post-fanout

## Deploy
Set account and region:

```bash
export CDK_ACCOUNT=390197508439
export CDK_REGION=us-west-2
```

Install dependencies and deploy:

```bash
cd infra
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
npx cdk deploy HdpStateStack --require-approval never --profile neil.AE
npx cdk deploy HdpComputeStack --require-approval never --profile neil.AE
npx cdk deploy HdpOrchestratorStack --require-approval never --profile neil.AE
```

## Run state machine
Start an execution (manual trigger):

```bash
aws stepfunctions start-execution \
	--state-machine-arn <STATE_MACHINE_ARN> \
	--name manual-$(date +%Y%m%dT%H%M%S) \
	--input '{"network":"ASOSAWOS"}' \
	--region us-west-2 \
	--profile neil.AE
```

NOOP semantics:
- Stage scripts return `0` (SUCCESS), `1` (FAILURE), or `3` (NOOP)
- AWS Batch reports non-zero as task failure
- State machine catches Batch task failures and routes exit code `3` to NOOP branch, while other non-zero exits fail execution

## Seed watermarks
```bash
cd /home/nschroed/Work/hdp-2.0
python3 infra/scripts/seed_watermarks.py --profile neil.AE --region us-west-2
```
