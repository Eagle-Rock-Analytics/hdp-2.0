#!/bin/bash -l

################################################################################
# SLURM Batch Script Template: run_merge_append_ASOSAWOS.sh
#
# Description:
#   Launches the merge pipeline in --append mode for historical weather station
#   data. Reads QAQC append slice from wecc-historical-wx/3_qaqc_wx_v2/ASOSAWOS/_append/,
#   reads existing baseline from HDP_SOURCE_BUCKET/4_merge_wx_v2/ASOSAWOS/,
#   concatenates, deduplicates on time, writes back.
################################################################################

# Job Information:
#SBATCH --job-name=hdp-merge-bf5
#SBATCH --array=1-5%4
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=2:00:00
#SBATCH --partition=compute
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

export HDP_SOURCE_BUCKET="wecc-historical-wx"
export HDP_PUBLISH_BUCKET="auto-hdp"
export HDP_PUBLISH_PREFIX="hdp"

REPO_ROOT=/home/ec2-user/hdp-2.0

STATION=$(awk "NR==$SLURM_ARRAY_TASK_ID" ${REPO_ROOT}/scripts/pcluster/stations_input/ASOSAWOS-backfill5-input.dat)
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

source ${REPO_ROOT}/.venv/bin/activate

cd ${REPO_ROOT}/scripts/4_merge_data/ || { echo "Directory change failed"; exit 1; }

PYSCRIPT="MERGE_run_for_single_station.py"

start_time=$(date +%s)

python3 ${PYSCRIPT} --station="$STATION" --append

end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

cd ${REPO_ROOT}/scripts/pcluster/ || true

{
  echo "====================================="
  echo "Job completed in $elapsed_time seconds."
  echo "Job End Time: $(date)"
  echo "====================================="
} >> "$log_file"
