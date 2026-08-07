#!/bin/bash

EXTRA_JARS_DIR="/opt/spark/extra-jars"
mkdir -p "$EXTRA_JARS_DIR" /tmp/.ivy2 2>/dev/null || true

echo "[NOC-Spark] Checking runtime JAR dependencies in $EXTRA_JARS_DIR..."

download_jar_if_missing() {
    local jar_name="$1"
    local jar_url="$2"
    local target_path="$EXTRA_JARS_DIR/$jar_name"

    if [ -f "$target_path" ] && [ $(stat -c%s "$target_path" 2>/dev/null || stat -f%z "$target_path" 2>/dev/null || echo 0) -gt 1000000 ]; then
        echo "[NOC-Spark] ✅ $jar_name already present."
    else
        echo "[NOC-Spark] ⬇️ Pre-fetching $jar_name..."
        curl -fL --connect-timeout 10 --max-time 120 "$jar_url" -o "$target_path.tmp" 2>/dev/null || true
        if [ -f "$target_path.tmp" ] && [ $(stat -c%s "$target_path.tmp" 2>/dev/null || stat -f%z "$target_path.tmp" 2>/dev/null || echo 0) -gt 1000000 ]; then
            mv "$target_path.tmp" "$target_path"
            echo "[NOC-Spark] ✅ Downloaded $jar_name successfully."
        else
            rm -f "$target_path.tmp" 2>/dev/null || true
            echo "[NOC-Spark] ⚠️ Warning: Pre-fetch for $jar_name skipped or timed out. Proceeding with Spark startup."
        fi
    fi
}

download_jar_if_missing "aws-java-sdk-bundle-1.12.262.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
download_jar_if_missing "hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_jar_if_missing "iceberg-spark-runtime-3.5_2.12-1.5.0.jar" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"

echo "[NOC-Spark] Launching Spark service..."
exec "$@"
