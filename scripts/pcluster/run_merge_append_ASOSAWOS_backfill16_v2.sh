#!/bin/bash -l

################################################################################
# SLURM Batch Script: run_merge_append_ASOSAWOS_backfill16_v2.sh
#
# Description:
#   Merges 16 ASOSAWOS backfill stations to auto-hdp/hdp/ASOSAWOS/ in append mode.
#   Validates data and publishes to live S3 bucket.
#
# Dependencies: Depends on the output of run_qaqc_ASOSAWOS_backfill16_v2.sh
################################################################################

# Job Information:
#SBATCH --job-name=hdp-merge-bf16-v2
#SBATCH --array=1-16%2
#SBATCH --ntasks=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=1:00:00
#SBATCH --mem=5G
#SBATCH --partition=compute
#SBATCH --dependency=afterok:JOBID_PLACEHOLDER
#SBATCH --output=%x_%A_%a_output.txt
#SBATCH --error=%x_%A_%a_error.txt

REPO_ROOT=/home/ec2-user/hdp-2.0

# Inline station list (16 backfill targets)
declare -a STATIONS=(
  "ASOSAWOS_72491723233"
  "ASOSAWOS_72493623254"
  "ASOSAWOS_72578894182"
  "ASOSAWOS_72667524037"
  "ASOSAWOS_72678524135"
  "ASOSAWOS_72679624138"
  "ASOSAWOS_72683524230"
  "ASOSAWOS_72683894185"
  "ASOSAWOS_72690124231"
  "ASOSAWOS_72779624137"
  "ASOSAWOS_72782624141"
  "ASOSAWOS_72785494119"
  "ASOSAWOS_72792794225"
  "ASOSAWOS_99999903102"
  "ASOSAWOS_99999923136"
  "ASOSAWOS_99999994274"
)

# Array indices are 1-based in Slurm; bash arrays are 0-based
STATION_INDEX=$((SLURM_ARRAY_TASK_ID - 1))
if [ $STATION_INDEX -lt 0 ] || [ $STATION_INDEX -ge ${#STATIONS[@]} ]; then
  echo "ERROR: Station index $STATION_INDEX out of range [0, $((${#STATIONS[@]}-1)))]" >&2
  exit 1
fi

STATION="${STATIONS[$STATION_INDEX]}"

# Use auto-hdp as the live publication target for ASOSAWOS data
export HDP_PUBLISH_BUCKET="auto-hdp"
export HDP_PUBLISH_PREFIX="hdp"

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
  echo "Publish Bucket: s3://${HDP_PUBLISH_BUCKET}/${HDP_PUBLISH_PREFIX}"
  echo "Job Name: $SLURM_JOB_NAME"
  echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
  echo "Task ID: $SLURM_ARRAY_TASK_ID"
  echo "Partition: $SLURM_JOB_PARTITION"
  echo "Job Start Time: $(date)"
  echo "====================================="
} >> "$log_file"

source ${REPO_ROOT}/.venv/bin/activate

cd ${REPO_ROOT}/scripts/4_merge_data/ || { echo "Directory change failed"; exit 1; }

start_time=$(date +%s)

python3 MERGE_run_for_single_station.py --station="$STATION" --verbose=False

end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

cd ${REPO_ROOT}/scripts/pcluster/ || true

{
  echo "====================================="
  echo "Job completed in $elapsed_time seconds."
  echo "Job End Time: $(date)"
  echo "====================================="
} >> "$log_file"
