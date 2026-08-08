#!/usr/bin/env bash
# ===================================================================
# Submit PySpark Incremental Streaming Job to Spark Master Cluster Container
# ===================================================================
set -e

CONTAINER_NAME="noc-spark-master"
JOB_PATH="/opt/spark/spark-apps/jobs/spark_processor.py"
CONF_FILE="/opt/spark/conf/spark-defaults.conf"

EXEC_FLAGS="-it"
if [ "$1" == "-d" ] || [ "$1" == "--detached" ]; then
  EXEC_FLAGS="-d"
  echo "🚀 Submitting PySpark Streaming Job in background (detached) to ${CONTAINER_NAME}..."
else
  echo "🚀 Submitting PySpark Streaming Job to ${CONTAINER_NAME}..."
fi

docker exec ${EXEC_FLAGS} "${CONTAINER_NAME}" /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --properties-file "${CONF_FILE}" \
  "${JOB_PATH}"

echo "✅ PySpark Streaming Job submitted."
