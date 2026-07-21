# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->


## Build & Test

```bash
# Install dependencies (including dev tools + pre-commit)
make install-dev

# Format code
make format

# Run linters
make lint

# Run tests
make test

# Run all pre-commit hooks
make pre-commit
```

## Architecture Overview

Pipeline stages, each in `scripts/<stage>/`:

1. **1_pull_data/** — download raw data from 27 weather networks
2. **2_clean_data/** — standardize units (SI) and format per network
3. **3_qaqc_data/** — 10+ modular QA/QC checks per station
4. **4_merge_data/** — hourly standardization; for the current ASOSAWOS private workflow, use `s3://auto-hdp/hdp/ASOSAWOS/` as the bucket source/target

`scripts/paths.py` is env-var driven — all bucket names come from env vars (`HDP_STAGING_BUCKET` (legacy alias `HDP_BUCKET`), `HDP_PUBLISH_BUCKET`, `HDP_PUBLISH_PREFIX`). Never hardcode S3 paths.

## Phase 2 Safety Checks (Required)

Before running Step Functions or Batch tests, verify runtime context:

```bash
aws sts get-caller-identity
aws configure get region
aws events describe-rule --name hdp-phase2-monthly
aws batch describe-job-definitions --job-definition-name hdp-merge-job --status ACTIVE \
   --query 'jobDefinitions[0].[containerProperties.image,containerProperties.environment]'
```

Expected env for current ASOSAWOS private rollout:
- `HDP_STAGING_BUCKET=hdp-staging-pull`
- `HDP_SOURCE_BUCKET=auto-hdp`
- `HDP_PUBLISH_BUCKET=auto-hdp`
- `HDP_PUBLISH_PREFIX=hdp`

If `HDP_SOURCE_BUCKET` is wrong, append merge can overwrite baseline history.

After manual runs, verify date ranges with xarray and confirm station starts did not jump forward.

## Conventions & Patterns

- Run scripts from `scripts/<stage>/` directory so relative log paths resolve correctly
- Logging: `from log_config import setup_logger` (QAQC) or `from merge_log_config import setup_logger` (merge)
- Paths: always `from paths import BUCKET_NAME, CLEAN_WX, QAQC_WX, ...`
- Error accumulation: `{"File": [...], "Time": [...], "Error": [...]}` dicts, uploaded to S3 at run end
- Single-station dev run: `python scripts/3_qaqc_data/QAQC_run_for_single_station.py --station=CW3E_HDC`
