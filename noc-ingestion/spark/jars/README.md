# Spark Custom JARS Directory

This project vendors required runtime dependencies in `spark/extra-jars/` to avoid network dependency resolution at runtime.

## Vendored Dependency Resolution
The following vendored JARs are provided in `spark/extra-jars/` and loaded directly into Spark via `spark.driver.extraClassPath`, `spark.executor.extraClassPath`, and `spark.jars` in `spark/config/spark-defaults.conf`:
- `aws-java-sdk-bundle-1.12.262.jar`
- `hadoop-aws-3.3.4.jar`
- `iceberg-spark-runtime-3.5_2.12-1.5.0.jar`

`spark.jars.packages` and runtime Ivy downloads have been removed completely.
