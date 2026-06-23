#!/bin/bash -l

################################################################################
# SLURM Batch Script Template: run_merge_append_{NETWORK}.sh
#
# Description:
#   Launches the merge pipeline in --append mode for historical weather station
#   data. Reads QAQC append slice from wecc-historical-wx/3_qaqc_wx_v2/{NETWORK}/_append/,
#   reads existing baseline from HDP_PUBLISH_BUCKET/HDP_PUBLISH_PREFIX/{NETWORK}/,
#   concatenates, deduplicates on time, writes back.
#
#   Set HDP_PUBLISH_BUCKET / HDP_PUBLISH_PREFIX below:
#     - Test run  : auto-hdp / hdp
#     - Production: cadcat   / hdp
#
# Inputs:
#   - Station list: stations_input/{NETWORK}-input.dat
#   - Python script: ../4_merge_data/MERGE_run_for_single_station.py
#   - Conda environment: hist-obs
#
# SLURM Configuration:
#   - Partition: compute
#   - CPUs per task: 1
#   - One array task per station (set using line count of input file)
#
# Working Directory:
#   This script should be submitted from the `pcluster/` directory.
#
# Output:
#   - Output and error logs are saved per-task, including the station name
################################################################################

# Job Information:
#SBATCH --job-name=hist-obs-merge-append
#SBATCH --array=1-{NROWS}
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=2:00:00
#SBATCH --partition=compute
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

# Publish target — change to cadcat/hdp for production catch-up
export HDP_PUBLISH_BUCKET="auto-hdp"
export HDP_PUBLISH_PREFIX="hdp"

# Get the station name for this array task
STATION=$(awk "NR==$SLURM_ARRAY_TASK_ID" stations_input/{NETWORK}-input.dat)

# AWS credentials
# Don't need to hard code them in if they are already saved as environment variables
# export AWS_ACCESS_KEY_ID="put-your-key-id-here"
# export AWS_SECRET_ACCESS_KEY="put-your-key-here"  # pragma: allowlist secret
# export AWS_DEFAULT_REGION="us-west-2"

# Rename SLURM-generated output and error files to include station name
ORIG_OUT="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_output.txt"
ORIG_ERR="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_error.txt"
NEW_OUT="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${STATION}_output.txt"
NEW_ERR="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${STATION}_error.txt"

# Wait for SLURM to generate the default files, then rename
sleep 2
[ -f "$ORIG_OUT" ] && mv "$ORIG_OUT" "$NEW_OUT"
[ -f "$ORIG_ERR" ] && mv "$ORIG_ERR" "$NEW_ERR"

# Use new output filename as log file
log_file="$NEW_OUT"

# Print SBATCH job settings for debugging to the log file
{
  echo "====================================="
  echo "Station: $STATION"
  echo "Publish target: s3://${HDP_PUBLISH_BUCKET}/${HDP_PUBLISH_PREFIX}"
  echo "Job Name: $SLURM_JOB_NAME"
  echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
  echo "Task ID: $SLURM_ARRAY_TASK_ID"
  echo "Partition: $SLURM_JOB_PARTITION"
  echo "Number of Nodes: $SLURM_JOB_NUM_NODES"
  echo "Tasks Per Node: $SLURM_NTASKS_PER_NODE"
  echo "Total Tasks: $SLURM_NTASKS"
  echo "CPUs Per Task: $SLURM_CPUS_PER_TASK"
  echo "Job Start Time: $(date)"
  echo "====================================="
} >> "$log_file"

REPO_ROOT=/home/ec2-user/hdp-2.0

# Activate uv-managed venv (head node NFS share, no EFS cost)
source ${REPO_ROOT}/.venv/bin/activate

# Change to the directory containing the script
cd ${REPO_ROOT}/scripts/4_merge_data/ || { echo "Directory change failed"; exit 1; }

# Define the path to your Python script
PYSCRIPT="MERGE_run_for_single_station.py"

# Start time tracking
start_time=$(date +%s)

# Run the Python script in append mode
python3 ${PYSCRIPT} --station="$STATION" --append

# End time tracking
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

# Return to pcluster dir for log rename
cd ${REPO_ROOT}/scripts/pcluster/ || true

# Write end-of-job info
{
  echo "====================================="
  echo "Job completed in $elapsed_time seconds."
  echo "Job End Time: $(date)"
  echo "====================================="
} >> "$log_file"
