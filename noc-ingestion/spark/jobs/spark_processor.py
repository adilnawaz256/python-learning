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
    builder = (
        SparkSession.builder.appName("NOC-Spark-ETL-Processor")
        .config("spark.driver.extraClassPath", "/opt/spark/extra-jars/*:/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/extra-jars/*:/opt/spark/jars/*")
        .config("spark.jars", "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar")
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
    spark.sparkContext.setLogLevel("WARN")

    # Create namespace schema 'noc' inside Iceberg catalog
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.noc")
    except Exception as e:
        logger.info(f"Namespace setup info: {e}")

    return spark


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

    logger.info(f"Reading raw data for category '{category}' from: {s3_input_path}")
    try:
        if file_format == "json":
            df = spark.read.option("multiline", "true").json(s3_input_path)
        elif file_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path)
        else:
            logger.warning(f"Unsupported format '{file_format}'")
            return

        if df.rdd.isEmpty():
            logger.info(f"No records found at {s3_input_path}")
            return

        row_count_before = df.count()
        cleaned_df = normalize_and_clean_dataframe(df)
        row_count_after = cleaned_df.count()

        logger.info(f"Category '{category}': Ingested {row_count_before} rows -> Cleaned to {row_count_after} rows.")

        # 1. Write to Parquet landing zone
        cleaned_df.write.mode("append").partitionBy("event_date").parquet(target_parquet_path)
        logger.info(f" Wrote clean Parquet files to {target_parquet_path}")

        # 2. Write directly into Apache Iceberg table
        full_iceberg_target = f"iceberg.noc.{iceberg_table_name}"
        try:
            cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").append()
            logger.info(f" Appended records to Iceberg table '{full_iceberg_target}'")
        except Exception:
            # Fallback table creation if not existing
            cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").create()
            logger.info(f" Created and populated Iceberg table '{full_iceberg_target}'")

    except AnalysisException as ae:
        if "Path does not exist" in str(ae) or "No files found" in str(ae):
            logger.info(f"Path '{s3_input_path}' is empty (handled gracefully).")
        else:
            logger.error(f"Spark analysis error for '{s3_input_path}': {ae}")
    except Exception as e:
        logger.error(f"Error processing category '{category}': {e}")


def main():
    start_time = time.time()
    logger.info("Initializing Spark NOC ETL Engine for Iceberg Tables...")
    spark = create_spark_session()

    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    base_s3_uri = f"s3a://{bucket_name}"

    # Ingestion Sources mapped to Apache Iceberg tables:
    # 1. alarms -> iceberg.noc.alarms
    # 2. tickets -> iceberg.noc.tickets
    # 3. network -> iceberg.noc.network_events
    # 4. security -> iceberg.noc.security_events
    # 5. performance -> iceberg.noc.performance_metrics
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

    for input_path, category, iceberg_table, fmt in sources:
        process_category(spark, input_path, category, iceberg_table, fmt)

    exec_time = round(time.time() - start_time, 2)
    logger.info(f"🎉 Spark NOC ETL Ingestion finished in {exec_time}s.")
    spark.stop()


if __name__ == "__main__":
    main()
