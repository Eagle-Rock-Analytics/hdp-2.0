# HDP Phase 2 Infra

This directory contains AWS CDK infrastructure for HDP Phase 2.

## gh5.9 scope
- DynamoDB tables: `hdp-watermarks`, `hdp-run-history`
- Lambda: `build-worklist`
- Seeder script: populates `hdp-watermarks` from `s3://auto-hdp/hdp/ASOSAWOS/`

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
```

## Seed watermarks
```bash
cd /home/nschroed/Work/hdp-2.0
python3 infra/scripts/seed_watermarks.py --profile neil.AE --region us-west-2
```
