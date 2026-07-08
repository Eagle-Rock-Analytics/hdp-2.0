#!/usr/bin/env bash
# Build-time smoke test for the HDP 2.0 image.
#
# Validates that the image is wired correctly WITHOUT needing AWS credentials or
# network data: for each stage it invokes the underlying script's --help, which
# exercises the interpreter, the venv, PYTHONPATH, and every transitive import
# (xarray, boto3, cartopy, geopandas, the pipeline modules, paths.py). If any
# dependency is missing or a module fails to import, argparse never gets to print
# usage and the stage exits non-zero.
#
# Functional smoke tests that touch S3/NCEI (e.g. `run-pull --dry-run`) require
# credentials and are exercised during the Batch one-station validation
# (hdp-gh5.4), not here.
set -euo pipefail

stages=(run-pull run-clean run-qaqc run-merge)
failed=0

for stage in "${stages[@]}"; do
    echo "=== smoke: ${stage} --help ==="
    if /app/docker/entrypoint.sh "${stage}" --help >/dev/null 2>&1; then
        echo "ok: ${stage}"
    else
        echo "FAIL: ${stage} (--help returned non-zero; likely an import error)" >&2
        # Re-run showing output to aid debugging.
        /app/docker/entrypoint.sh "${stage}" --help || true
        failed=1
    fi
done

if [[ "${failed}" -ne 0 ]]; then
    echo "smoke test FAILED" >&2
    exit 1
fi

echo "smoke test passed: all four entrypoints import and parse args"
