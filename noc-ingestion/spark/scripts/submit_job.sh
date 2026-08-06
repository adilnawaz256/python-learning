#!/usr/bin/env bash
# ===================================================================
# Submit PySpark Job to Spark Master Cluster Container
# ===================================================================
set -e

CONTAINER_NAME="noc-spark-master"
JOB_PATH="/opt/bitnami/spark/spark-apps/jobs/read_minio.py"
CONF_FILE="/opt/bitnami/spark/conf/spark-defaults.conf"

echo "🚀 Submitting PySpark Job to ${CONTAINER_NAME}..."

docker exec -it "${CONTAINER_NAME}" spark-submit \
  --master spark://spark-master:7077 \
  --properties-file "${CONF_FILE}" \
  "${JOB_PATH}"

echo "✅ Job execution finished."
