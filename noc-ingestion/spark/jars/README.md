# Spark Custom JARS Directory

This folder is designated for custom JAR files required by Apache Spark jobs.

## Default Package Resolution
By default, required Maven packages are resolved automatically via `spark.jars.packages` in `spark/config/spark-defaults.conf`:
- `org.apache.hadoop:hadoop-aws:3.3.4`
- `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0`
- `com.crealytics:spark-excel_2.12:3.5.0_0.20.4`

## Manual Offline JAR Provisioning
If deploying in an isolated network without public Maven access, place `.jar` files here and mount them into `/opt/bitnami/spark/jars/`.
