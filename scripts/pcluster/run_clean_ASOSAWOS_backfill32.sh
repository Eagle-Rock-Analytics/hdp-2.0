#!/bin/bash -l

################################################################################
# SLURM Batch Script Template: run_clean_ASOSAWOS.sh
#
# Description:
#   Cleans GHCNh Parquet files into the HDP NetCDF append slice for one station.
#   Each SLURM array task handles one station (embarrassingly parallel).
#
#   Uses --append mode: reads the station's baseline zarr last timestamp from
#   the source baseline bucket and processes only new data since that point.
#
#   Output: s3://wecc-historical-wx/2_clean_wx_append/ASOSAWOS/{STATION}.nc
#
# Inputs:
#   - Station list: stations_input/ASOSAWOS-backfill32-input.dat
#   - Python script: scripts/2_clean_data/GHCNh_clean.py
#   - uv venv: /home/ec2-user/hdp-2.0/.venv
#
# SLURM Configuration:
#   - Partition: compute
#   - CPUs per task: 1
#   - 1 h per station
#
# Working Directory:
#   Submit from scripts/pcluster/
################################################################################

# Job Information:
#SBATCH --job-name=hdp-clean-bf32
#SBATCH --array=1-32%8
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=1:00:00
#SBATCH --partition=compute
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

REPO_ROOT=/home/ec2-user/hdp-2.0

# Explicit baseline source for append boundary lookup.
export HDP_SOURCE_BUCKET="wecc-historical-wx"

# Get the station name for this array task
STATION=$(awk "NR==$SLURM_ARRAY_TASK_ID" stations_input/ASOSAWOS-backfill32-input.dat)

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
  echo "Baseline source: s3://${HDP_SOURCE_BUCKET}/4_merge_wx_v2"
  echo "Job Name: $SLURM_JOB_NAME"
  echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
  echo "Task ID: $SLURM_ARRAY_TASK_ID"
  echo "Partition: $SLURM_JOB_PARTITION"
  echo "Job Start Time: $(date)"
  echo "====================================="
} >> "$log_file"

# Activate uv-managed venv (head node NFS share, no EFS)
source ${REPO_ROOT}/.venv/bin/activate

cd ${REPO_ROOT}/scripts/2_clean_data/ || { echo "Directory change failed"; exit 1; }

start_time=$(date +%s)

# Clean GHCNh Parquet → HDP NetCDF append slice
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
