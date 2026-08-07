#!/bin/bash
set -e

EXTRA_JARS_DIR="/opt/spark/extra-jars"
mkdir -p "$EXTRA_JARS_DIR" /tmp/.ivy2

echo "Ensuring required AWS S3A & Iceberg JARs exist in $EXTRA_JARS_DIR..."

if [ ! -f "$EXTRA_JARS_DIR/aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "Downloading aws-java-sdk-bundle-1.12.262.jar..."
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o "$EXTRA_JARS_DIR/aws-java-sdk-bundle-1.12.262.jar"
fi

if [ ! -f "$EXTRA_JARS_DIR/hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading hadoop-aws-3.3.4.jar..."
    curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o "$EXTRA_JARS_DIR/hadoop-aws-3.3.4.jar"
fi

if [ ! -f "$EXTRA_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.5.0.jar" ]; then
    echo "Downloading iceberg-spark-runtime-3.5_2.12-1.5.0.jar..."
    curl -sL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -o "$EXTRA_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"
fi

echo "All required Spark runtime JARs verified successfully in $EXTRA_JARS_DIR."
exec "$@"
