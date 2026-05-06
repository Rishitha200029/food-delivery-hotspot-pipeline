#!/bin/bash
# launch_job.sh
# Submits the hotspot Spark job to an existing EMR cluster.
#
# Usage:
#   bash launch_job.sh <cluster_id> <script_path> <start_date> <end_date> \
#                      <metadata_bucket> <collaboration_id> <membership_id>

set -euo pipefail

CLUSTER_ID="${1}"
SCRIPT_PATH="${2}"
START_DATE="${3}"
END_DATE="${4}"
METADATA_BUCKET="${5:-your-metadata-bucket}"
COLLABORATION_ID="${6}"
MEMBERSHIP_ID="${7}"
REGION="us-east-1"

echo "[launch_job] Submitting Spark step to cluster ${CLUSTER_ID}"
echo "[launch_job] Window: ${START_DATE} → ${END_DATE}"

STEP_ID=$(aws emr add-steps \
  --cluster-id "${CLUSTER_ID}" \
  --steps "Type=Spark,Name=hotspot-spark,ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
--conf,spark.yarn.submit.waitAppCompletion=true,\
${SCRIPT_PATH},\
--start-date,${START_DATE},\
--end-date,${END_DATE},\
--metadata-bucket,${METADATA_BUCKET},\
--collaboration-id,${COLLABORATION_ID},\
--membership-id,${MEMBERSHIP_ID}]" \
  --region "${REGION}" \
  --query 'StepIds[0]' \
  --output text)

echo "[launch_job] Step submitted: ${STEP_ID}"
echo "${STEP_ID}" > /tmp/step_id.txt

WAIT=30; MAX_WAIT=300; ELAPSED=0; TIMEOUT=18000

while [ "${ELAPSED}" -lt "${TIMEOUT}" ]; do
  STATE=$(aws emr describe-step \
    --cluster-id "${CLUSTER_ID}" --step-id "${STEP_ID}" \
    --region "${REGION}" --query 'Step.Status.State' --output text)
  echo "[launch_job] ${STEP_ID} — ${STATE} (${ELAPSED}s)"
  case "${STATE}" in
    COMPLETED) echo "Done."; exit 0 ;;
    FAILED|CANCELLED|INTERRUPTED) echo "Step failed: ${STATE}" >&2; exit 1 ;;
  esac
  sleep "${WAIT}"; ELAPSED=$((ELAPSED+WAIT))
  WAIT=$((WAIT*2)); [ "${WAIT}" -gt "${MAX_WAIT}" ] && WAIT="${MAX_WAIT}"
done

echo "Timed out." >&2; exit 1
