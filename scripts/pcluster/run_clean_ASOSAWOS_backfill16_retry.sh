#!/bin/bash -l

################################################################################
# SLURM Batch Script: run_clean_ASOSAWOS_backfill16_retry.sh
#
# Description:
#   Retries the ASOSAWOS clean stage for only the stations that still failed
#   after the smaller-node retry, using a larger memory request so Slurm will
#   favor the 4vcpu/8gb spot nodes.
#
# Inputs:
#   - Station list: stations_input/ASOSAWOS-backfill16-input.dat
#   - Python script: scripts/2_clean_data/GHCNh_clean.py
#   - uv venv: /home/ec2-user/hdp-2.0/.venv
################################################################################

# Job Information:
#SBATCH --job-name=hdp-clean-bf16
#SBATCH --array=1-16%4
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=1:00:00
#SBATCH --mem=7G
#SBATCH --partition=compute
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

REPO_ROOT=/home/ec2-user/hdp-2.0

export HDP_SOURCE_BUCKET="wecc-historical-wx"

STATION=$(awk "NR==$SLURM_ARRAY_TASK_ID" ${REPO_ROOT}/scripts/pcluster/stations_input/ASOSAWOS-backfill16-input.dat)
if [ -z "$STATION" ]; then
  echo "Station lookup failed for array task $SLURM_ARRAY_TASK_ID"
  exit 1
fi

ORIG_OUT="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_output.txt"
ORIG_ERR="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_error.txt"
NEW_OUT="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${STATION}_output.txt"
NEW_ERR="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${STATION}_error.txt"

sleep 2
[ -f "$ORIG_OUT" ] && mv "$ORIG_OUT" "$NEW_OUT"
[ -f "$ORIG_ERR" ] && mv "$ORIG_ERR" "$NEW_ERR"

log_file="$NEW_OUT"

{
  echo "====================================="
  echo "Station: $STATION"
  echo "Baseline source: s3://${HDP_SOURCE_BUCKET}/4_merge_wx_v2"
  echo "Job Name: $SLURM_JOB_NAME"
  echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
  echo "Task ID: $SLURM_ARRAY_TASK_ID"
  echo "Partition: $SLURM_JOB_PARTITION"
  echo "Job Start Time: $(date)"
  echo "====================================="
} >> "$log_file"

source ${REPO_ROOT}/.venv/bin/activate

cd ${REPO_ROOT}/scripts/2_clean_data/ || { echo "Directory change failed"; exit 1; }

start_time=$(date +%s)

python3 GHCNh_clean.py --station="$STATION" --append

end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

cd ${REPO_ROOT}/scripts/pcluster/ || true

{
  echo "====================================="
  echo "Job completed in $elapsed_time seconds."
  echo "Job End Time: $(date)"
  echo "====================================="
} >> "$log_file"
