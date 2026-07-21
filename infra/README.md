# HDP Phase 2 Infra

This directory contains AWS CDK infrastructure for HDP Phase 2.

## Mandatory preflight: context lock

Run these checks before every manual execution or schedule change:

```bash
aws sts get-caller-identity --region us-west-2 --profile neil.AE
aws events describe-rule --name hdp-phase2-monthly --region us-west-2 --profile neil.AE
aws events list-targets-by-rule --rule hdp-phase2-monthly --region us-west-2 --profile neil.AE

aws batch describe-job-definitions --job-definition-name hdp-pull-job --status ACTIVE \
	--query 'jobDefinitions[0].[containerProperties.image,containerProperties.environment]' \
	--region us-west-2 --profile neil.AE
aws batch describe-job-definitions --job-definition-name hdp-clean-job --status ACTIVE \
	--query 'jobDefinitions[0].[containerProperties.image,containerProperties.environment]' \
	--region us-west-2 --profile neil.AE
aws batch describe-job-definitions --job-definition-name hdp-qaqc-job --status ACTIVE \
	--query 'jobDefinitions[0].[containerProperties.image,containerProperties.environment]' \
	--region us-west-2 --profile neil.AE
aws batch describe-job-definitions --job-definition-name hdp-merge-job --status ACTIVE \
	--query 'jobDefinitions[0].[containerProperties.image,containerProperties.environment]' \
	--region us-west-2 --profile neil.AE
```

Expected env values for the private ASOSAWOS rollout:
- `HDP_STAGING_BUCKET=hdp-staging-pull`
- `HDP_SOURCE_BUCKET=auto-hdp`
- `HDP_PUBLISH_BUCKET=auto-hdp`
- `HDP_PUBLISH_PREFIX=hdp`

If source/publish context drifts, append merge can overwrite baseline history.

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
	--state-machine-arn arn:aws:states:us-west-2:<ACCOUNT>:stateMachine:hdp-phase2-state-machine \
	--name manual-$(date +%Y%m%dT%H%M%S) \
	--input '{"network":"ASOSAWOS"}' \
	--region us-west-2 \
	--profile neil.AE
```

Targeted validation runs:

```bash
# One station
aws stepfunctions start-execution \
	--state-machine-arn arn:aws:states:us-west-2:<ACCOUNT>:stateMachine:hdp-phase2-state-machine \
	--name manual-one-$(date +%Y%m%dT%H%M%S) \
	--input '{"network":"ASOSAWOS","station_ids":["ASOSAWOS_72092300310"]}' \
	--region us-west-2 \
	--profile neil.AE

# Five stations in a fixed order
aws stepfunctions start-execution \
	--state-machine-arn arn:aws:states:us-west-2:<ACCOUNT>:stateMachine:hdp-phase2-state-machine \
	--name manual-five-$(date +%Y%m%dT%H%M%S) \
	--input '{"network":"ASOSAWOS","station_ids":["ASOSAWOS_72092300310","ASOSAWOS_72290023188","ASOSAWOS_72386023169","ASOSAWOS_72483023183","ASOSAWOS_72606014738"]}' \
	--region us-west-2 \
	--profile neil.AE
```

`build-worklist` also accepts `max_stations` for capped sample runs when exact station IDs are not important.

NOOP semantics:
- Stage scripts return `0` (SUCCESS), `1` (FAILURE), or `3` (NOOP)
- AWS Batch reports non-zero as task failure
- State machine catches Batch task failures and routes exit code `3` to NOOP branch, while other non-zero exits fail execution

## Seed watermarks
```bash
cd /home/nschroed/Work/hdp-2.0
python3 infra/scripts/seed_watermarks.py --profile neil.AE --region us-west-2
```

## Schedule controls

```bash
aws events disable-rule --name hdp-phase2-monthly --region us-west-2 --profile neil.AE
aws events enable-rule --name hdp-phase2-monthly --region us-west-2 --profile neil.AE
```

## Post-run integrity check (required)

Validate date ranges after one-station or five-station tests:

```bash
python - <<'PY'
import xarray as xr
stations=["ASOSAWOS_69007093217","ASOSAWOS_72012200114","ASOSAWOS_72019300117","ASOSAWOS_72020200118","ASOSAWOS_72025400119"]
for s in stations:
	ds=xr.open_zarr(f"s3://auto-hdp/hdp/ASOSAWOS/{s}.zarr", consolidated=False)
	t=ds.time.values
	print(s, str(t.min()), str(t.max()), len(t))
	ds.close()
PY
```

If a station start date jumps forward unexpectedly, stop and restore baseline before further runs.
