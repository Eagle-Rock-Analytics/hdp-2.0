#!/usr/bin/env bash
# Entrypoint router for the HDP 2.0 pipeline image.
#
# Selects a pipeline stage by its first argument and forwards the remaining
# arguments to the underlying script. Each stage runs from its own directory so
# that sibling-module imports (e.g. QAQC_pipeline) and relative log paths
# (qaqc_logs/, merge_logs/) resolve the same way they do on ParallelCluster.
#
# Stages:
#   run-pull   -> scripts/1_pull_data/pull_asosawos_from_last_timestamps.py
#   run-clean  -> scripts/2_clean_data/GHCNh_clean.py
#   run-qaqc   -> scripts/3_qaqc_data/QAQC_run_for_single_station.py
#   run-merge  -> scripts/4_merge_data/MERGE_run_for_single_station.py
#   smoke      -> docker/smoke_test.sh (no-AWS build validation)
set -euo pipefail

cmd="${1:-}"
shift || true

case "${cmd}" in
    run-pull)
        cd /app/scripts/1_pull_data
        exec python pull_asosawos_from_last_timestamps.py "$@"
        ;;
    run-clean)
        cd /app/scripts/2_clean_data
        exec python GHCNh_clean.py "$@"
        ;;
    run-qaqc)
        cd /app/scripts/3_qaqc_data
        exec python QAQC_run_for_single_station.py "$@"
        ;;
    run-merge)
        cd /app/scripts/4_merge_data
        exec python MERGE_run_for_single_station.py "$@"
        ;;
    smoke)
        exec /app/docker/smoke_test.sh
        ;;
    ""|-h|--help|help)
        echo "usage: {run-pull|run-clean|run-qaqc|run-merge|smoke} [args]" >&2
        exit 2
        ;;
    *)
        # Escape hatch: run an arbitrary command inside the image.
        exec "${cmd}" "$@"
        ;;
esac
