#!/bin/bash -l

################################################################################
# SLURM Batch Script Template: run_pull_ASOSAWOS.sh
#
# Description:
#   Pulls GHCNh Parquet files from NCEI for a single ASOSAWOS station.
#   Each SLURM array task handles one station (embarrassingly parallel).
#
#   Fetches years 2022-present; skip-existing logic in GHCNh_pull.py avoids
#   re-downloading Parquet files already in S3.
#
#   Output: s3://wecc-historical-wx/1_raw_wx/ASOSAWOS/{STATION}/GHCNh_*.parquet
#
# Inputs:
#   - Station list: stations_input/ASOSAWOS-input.dat
#   - Python script: scripts/1_pull_data/GHCNh_pull.py
#   - uv venv: /home/ec2-user/hdp-2.0/.venv
#
# SLURM Configuration:
#   - Partition: compute
#   - CPUs per task: 1
#   - 30 min per station (mostly HTTP download + S3 upload)
#
# Working Directory:
#   Submit from scripts/pcluster/
################################################################################

# Job Information:
#SBATCH --job-name=hdp-pull
#SBATCH --array=1-455
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0:30:00
#SBATCH --partition=compute
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

REPO_ROOT=/home/ec2-user/hdp-2.0

# Get the station name for this array task
STATION=$(awk "NR==$SLURM_ARRAY_TASK_ID" stations_input/ASOSAWOS-input.dat)

# Rename SLURM-generated output and error files to include station name
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
  echo "Job Name: $SLURM_JOB_NAME"
  echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
  echo "Task ID: $SLURM_ARRAY_TASK_ID"
  echo "Partition: $SLURM_JOB_PARTITION"
  echo "Job Start Time: $(date)"
  echo "====================================="
} >> "$log_file"

# Activate uv-managed venv (head node NFS share, no EFS)
source ${REPO_ROOT}/.venv/bin/activate

cd ${REPO_ROOT}/scripts/1_pull_data/ || { echo "Directory change failed"; exit 1; }

start_time=$(date +%s)

# Pull GHCNh Parquet from NCEI, years 2022-present; skip already-uploaded files
python3 GHCNh_pull.py --station="$STATION" --start-year 2022

end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

cd ${REPO_ROOT}/scripts/pcluster/ || true

{
  echo "====================================="
  echo "Job completed in $elapsed_time seconds."
  echo "Job End Time: $(date)"
  echo "====================================="
} >> "$log_file"
