#!/bin/bash
# provision_cluster.sh
# Creates an AWS EMR cluster for the Food Delivery Hotspot Pipeline.
#
# Usage:
#   bash provision_cluster.sh <metadata_bucket> <subnet_id> <key_pair> [instance_type] [core_nodes]

set -euo pipefail

METADATA_BUCKET="${1:-your-metadata-bucket}"
SUBNET_ID="${2:-subnet-xxxxxxxx}"
KEY_PAIR="${3:-your-key-pair}"
INSTANCE_TYPE="${4:-m5.4xlarge}"
CORE_NODES="${5:-4}"
EMR_RELEASE="emr-6.3.0"
REGION="us-east-1"

echo "[provision_cluster] Creating EMR cluster for hotspot pipeline..."

CLUSTER_ID=$(aws emr create-cluster \
  --name "hotspot-pipeline-$(date +%Y%m%d-%H%M)" \
  --release-label "${EMR_RELEASE}" \
  --applications Name=Spark Name=Hadoop \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType="${INSTANCE_TYPE}" \
    InstanceGroupType=CORE,InstanceCount="${CORE_NODES}",InstanceType="${INSTANCE_TYPE}" \
  --ec2-attributes KeyName="${KEY_PAIR}",SubnetId="${SUBNET_ID}" \
  --configurations '[
    {"Classification":"spark-defaults","Properties":{
      "spark.executor.memory":"12g",
      "spark.driver.memory":"8g",
      "spark.executor.cores":"4",
      "spark.sql.shuffle.partitions":"200",
      "spark.dynamicAllocation.enabled":"true"
    }}
  ]' \
  --service-role EMR_DefaultRole \
  --job-flow-role EMR_EC2_DefaultRole \
  --log-uri "s3://${METADATA_BUCKET}/emr-logs/" \
  --no-auto-terminate \
  --no-termination-protected \
  --visible-to-all-users \
  --tags project=food-delivery-hotspot team=data-engineering managed-by=airflow \
  --region "${REGION}" \
  --query 'JobFlowId' \
  --output text)

echo "[provision_cluster] Cluster created: ${CLUSTER_ID}"
echo "${CLUSTER_ID}" > /tmp/cluster_id.txt
aws emr wait cluster-running --cluster-id "${CLUSTER_ID}" --region "${REGION}"
echo "[provision_cluster] Cluster ready."
