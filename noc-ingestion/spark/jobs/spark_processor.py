import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    upper,
    when,
    coalesce,
    lit,
    current_timestamp,
    date_format,
)
from pyspark.sql.utils import AnalysisException

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NOC-Spark-ETL-Processor")


def create_spark_session() -> SparkSession:
    """Initializes SparkSession configured for MinIO S3A storage and Apache Iceberg catalog."""
    step_start = time.time()
    logger.info("[STEP 1] Starting SparkSession initialization")
    try:
        builder = (
            SparkSession.builder.appName("NOC-Spark-ETL-Processor")
            .config("spark.driver.extraClassPath", "/opt/spark/extra-jars/*:/opt/spark/jars/*")
            .config("spark.executor.extraClassPath", "/opt/spark/extra-jars/*:/opt/spark/jars/*")
            .config(
                "spark.jars",
                "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,"
                "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,"
                "/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
            )
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "rest")
            .config("spark.sql.catalog.iceberg.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
            .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{os.getenv('MINIO_BUCKET', 'noc-raw-data')}/iceberg-warehouse")
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("INFO")

        # Enable INFO logging for Hadoop S3A
        try:
            log4j_manager = spark.sparkContext._jvm.org.apache.log4j.LogManager
            log4j_level = spark.sparkContext._jvm.org.apache.log4j.Level
            log4j_manager.getLogger("org.apache.hadoop.fs.s3a").setLevel(log4j_level.INFO)
        except Exception as log_err:
            logger.info(f"Hadoop S3A logger configuration info: {log_err}")

        elapsed = time.time() - step_start
        logger.info("[STEP 1 COMPLETE] SparkSession initialized. Took %.2f sec", elapsed)

        # Create namespace schema 'noc' inside Iceberg catalog
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.noc")
            logger.info("Namespace 'iceberg.noc' verified/created.")
        except Exception as e:
            logger.info(f"Namespace setup info: {e}")

        return spark
    except Exception:
        logger.exception("[STEP 1 FAILED] SparkSession initialization failed")
        raise


def normalize_and_clean_dataframe(df):
    """Performs schema normalization, severity standardization, timestamp parsing, and deduplication."""
    # 1. Normalize Severity
    if "severity" in df.columns:
        df = df.withColumn(
            "severity_clean",
            when(upper(col("severity")).contains("CRIT") | upper(col("severity")).contains("P1"), "CRITICAL")
            .when(upper(col("severity")).contains("MAJ") | upper(col("severity")).contains("HIGH") | upper(col("severity")).contains("P2"), "MAJOR")
            .when(upper(col("severity")).contains("MIN") | upper(col("severity")).contains("MED") | upper(col("severity")).contains("P3"), "MINOR")
            .when(upper(col("severity")).contains("WARN"), "WARNING")
            .otherwise("INFO")
        )
    else:
        df = df.withColumn("severity_clean", lit("INFO"))

    # 2. Normalize Timestamps & Dates
    ts_col = None
    for candidate in ["timestamp", "created_at", "event_time", "time"]:
        if candidate in df.columns:
            ts_col = candidate
            break

    if ts_col:
        df = df.withColumn("event_timestamp", to_timestamp(col(ts_col)))
    else:
        df = df.withColumn("event_timestamp", current_timestamp())

    df = df.withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))

    # 3. Deduplication
    id_col = None
    for candidate in ["event_id", "alarm_id", "ticket_id", "metric_id"]:
        if candidate in df.columns:
            id_col = candidate
            break

    if id_col:
        df = df.dropDuplicates([id_col])

    return df


def process_category(spark: SparkSession, s3_input_path: str, category: str, iceberg_table_name: str, file_format: str = "json") -> None:
    """Reads raw telemetry, cleans & normalizes, and appends to Apache Iceberg catalog table + Parquet landing zone."""
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    target_parquet_path = f"s3a://{bucket_name}/processed/parquet/{category}/"

    logger.info("=== Category Process Context ===")
    logger.info("raw_path: %s", s3_input_path)
    logger.info("Spark master: %s", spark.sparkContext.master)
    logger.info("Spark version: %s", spark.version)
    logger.info("spark.sparkContext.applicationId: %s", spark.sparkContext.applicationId)
    logger.info("spark.sparkContext.master: %s", spark.sparkContext.master)
    logger.info("spark.sparkContext.defaultParallelism: %s", spark.sparkContext.defaultParallelism)

    # STEP 3: Read Raw Data
    step3_start = time.time()
    logger.info("[STEP 3] Reading raw data format '%s' for category '%s' from: %s", file_format, category, s3_input_path)
    try:
        if file_format == "json":
            df = spark.read.option("multiline", "true").json(s3_input_path)
        elif file_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path)
        else:
            logger.warning(f"Unsupported format '{file_format}'")
            return
        step3_elapsed = time.time() - step3_start
        logger.info("[STEP 3 COMPLETE] Reading JSON from %s took %.2f sec", s3_input_path, step3_elapsed)
    except AnalysisException as ae:
        if "Path does not exist" in str(ae) or "No files found" in str(ae):
            logger.info(f"Path '{s3_input_path}' is empty (handled gracefully).")
            return
        else:
            logger.error(f"Spark analysis error for '{s3_input_path}': {ae}")
            return
    except Exception:
        logger.exception("[STEP 3 FAILED] Reading raw data from %s failed", s3_input_path)
        raise

    # Print Schema & Record Count Immediately
    try:
        logger.info("Schema:")
        df.printSchema()

        logger.info("Executing count()")
        count_start = time.time()
        count = df.count()
        count_elapsed = time.time() - count_start
        logger.info("Record count = %s (took %.2f sec)", count, count_elapsed)

        if count == 0:
            logger.info(f"No records found at {s3_input_path}")
            return
    except Exception:
        logger.exception("Failed executing schema inspection / count()")
        raise

    # STEP 4: Data Transformations & Cleaning
    step4_start = time.time()
    logger.info("[STEP 4] Executing transformations and schema normalization for category '%s'", category)
    try:
        cleaned_df = normalize_and_clean_dataframe(df)
        row_count_after = cleaned_df.count()
        step4_elapsed = time.time() - step4_start
        logger.info("[STEP 4 COMPLETE] Transformations for category '%s' completed (Rows: %s -> %s) in %.2f sec", category, count, row_count_after, step4_elapsed)
    except Exception:
        logger.exception("[STEP 4 FAILED] Transformations failed for category '%s'", category)
        raise

    # STEP 5: Parquet Landing Zone Write
    step5_start = time.time()
    logger.info("[STEP 5] Writing clean Parquet files to %s", target_parquet_path)
    try:
        cleaned_df.write.mode("append").partitionBy("event_date").parquet(target_parquet_path)
        step5_elapsed = time.time() - step5_start
        logger.info("[STEP 5 COMPLETE] Wrote clean Parquet files to %s in %.2f sec", target_parquet_path, step5_elapsed)
    except Exception:
        logger.exception("[STEP 5 FAILED] Parquet write failed for %s", target_parquet_path)
        raise

    # STEP 6: Apache Iceberg Table Write
    full_iceberg_target = f"iceberg.noc.{iceberg_table_name}"
    step6_start = time.time()
    logger.info("Writing to Iceberg table %s", full_iceberg_target)
    logger.info("[STEP 6] Appending records to Apache Iceberg table '%s'", full_iceberg_target)
    try:
        try:
            cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").append()
            logger.info(f"Appended records to Iceberg table '{full_iceberg_target}'")
        except Exception:
            # Fallback table creation if not existing
            logger.info(f"Table '{full_iceberg_target}' does not exist yet. Creating table...")
            cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").create()
            logger.info(f"Created and populated Iceberg table '{full_iceberg_target}'")

        step6_elapsed = time.time() - step6_start
        logger.info("[STEP 6 COMPLETE] Iceberg write to '%s' completed in %.2f sec", full_iceberg_target, step6_elapsed)
    except Exception:
        logger.exception("[STEP 6 FAILED] Iceberg table write failed for %s", full_iceberg_target)
        raise


def main():
    start_time = time.time()
    logger.info("Initializing Spark NOC ETL Engine for Iceberg Tables...")

    # Step 1: Spark Session
    spark = create_spark_session()

    # Step 2: Source Path Construction
    step2_start = time.time()
    logger.info("[STEP 2] Constructing raw data source S3A paths")
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    base_s3_uri = f"s3a://{bucket_name}"

    sources = [
        (f"{base_s3_uri}/raw/kafka/alarms/*/*/*/*", "alarms", "alarms", "json"),
        (f"{base_s3_uri}/raw/kafka/tickets/*/*/*/*", "tickets", "tickets", "json"),
        (f"{base_s3_uri}/raw/kafka/network/*/*/*/*", "network", "network_events", "json"),
        (f"{base_s3_uri}/raw/kafka/security/*/*/*/*", "security", "security_events", "json"),
        (f"{base_s3_uri}/raw/kafka/performance/*/*/*/*", "performance", "performance_metrics", "json"),
        (f"{base_s3_uri}/raw/rest/*/*/*/*", "rest", "network_events", "json"),
        (f"{base_s3_uri}/raw/uploads/csv/*/*/*/*", "uploads_csv", "network_events", "csv"),
        (f"{base_s3_uri}/raw/uploads/json/*/*/*/*", "uploads_json", "network_events", "json"),
    ]
    step2_elapsed = time.time() - step2_start
    logger.info("[STEP 2 COMPLETE] Source paths constructed (%d sources) in %.2f sec", len(sources), step2_elapsed)

    for input_path, category, iceberg_table, fmt in sources:
        process_category(spark, input_path, category, iceberg_table, fmt)

    # Step 7: Spark Session Stop
    step7_start = time.time()
    logger.info("[STEP 7] Stopping SparkSession")
    spark.stop()
    step7_elapsed = time.time() - step7_start
    logger.info("[STEP 7 COMPLETE] SparkSession stopped in %.2f sec", step7_elapsed)

    exec_time = round(time.time() - start_time, 2)
    logger.info(f"🎉 Spark NOC ETL Ingestion finished in {exec_time}s.")


if __name__ == "__main__":
    main()
