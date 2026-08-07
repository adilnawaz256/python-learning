#!/bin/bash

EXTRA_JARS_DIR="${EXTRA_JARS_DIR:-/opt/spark/extra-jars}"
mkdir -p /tmp/.ivy2 2>/dev/null || true

echo "[NOC-Spark] Verifying vendored runtime JAR dependencies in $EXTRA_JARS_DIR..."

FAILED=0

verify_jar() {
    local jar_name="$1"
    local target_path="$EXTRA_JARS_DIR/$jar_name"

    if [ -f "$target_path" ] && [ $(stat -c%s "$target_path" 2>/dev/null || stat -f%z "$target_path" 2>/dev/null || echo 0) -gt 100000 ]; then
        echo "[NOC-Spark] ✅ $jar_name verified ($(du -h "$target_path" | cut -f1))."
    else
        echo "[NOC-Spark] ❌ ERROR: Vendored JAR $jar_name is missing or invalid in $EXTRA_JARS_DIR!"
        FAILED=1
    fi
}

verify_jar "aws-java-sdk-bundle-1.12.262.jar"
verify_jar "hadoop-aws-3.3.4.jar"
verify_jar "iceberg-spark-runtime-3.5_2.12-1.5.0.jar"

if [ $FAILED -ne 0 ]; then
    echo "[NOC-Spark] ❌ Fatal Startup Error: Missing required vendored JARs in $EXTRA_JARS_DIR. Aborting."
    exit 1
fi

echo "[NOC-Spark] All required vendored JARs verified successfully."
echo "[NOC-Spark] Launching Spark service..."
exec "$@"
