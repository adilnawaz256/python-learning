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
    """Initializes SparkSession configured for MinIO S3A storage and Parquet output."""
    builder = (
        SparkSession.builder.appName("NOC-Spark-ETL-Processor")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
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


def process_and_convert_to_parquet(spark: SparkSession, s3_input_path: str, category: str, file_format: str = "json") -> None:
    """Reads raw JSON/CSV/Excel dataset from MinIO, cleans & normalizes it, and writes partitioned Parquet output."""
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    target_parquet_path = f"s3a://{bucket_name}/processed/parquet/{category}/"

    logger.info(f"Reading raw data from: {s3_input_path}")
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

        logger.info(f"Category '{category}': Ingested {row_count_before} rows -> Deduplicated to {row_count_after} rows.")

        # Write clean Parquet file ready for Iceberg external table creation
        cleaned_df.write.mode("append").partitionBy("event_date").parquet(target_parquet_path)
        logger.info(f" Successfully wrote clean Parquet files to {target_parquet_path}")

    except AnalysisException as ae:
        if "Path does not exist" in str(ae) or "No files found" in str(ae):
            logger.info(f"Path '{s3_input_path}' is empty (handled gracefully).")
        else:
            logger.error(f"Spark analysis error for '{s3_input_path}': {ae}")
    except Exception as e:
        logger.error(f"Error processing category '{category}': {e}")


def main():
    start_time = time.time()
    logger.info("Initializing Spark NOC ETL Pipeline...")
    spark = create_spark_session()

    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    base_s3_uri = f"s3a://{bucket_name}"

    # Ingestion Sources to process
    sources = [
        (f"{base_s3_uri}/raw/kafka/alarms/*/*/*/*", "alarms", "json"),
        (f"{base_s3_uri}/raw/kafka/tickets/*/*/*/*", "tickets", "json"),
        (f"{base_s3_uri}/raw/kafka/network/*/*/*/*", "network", "json"),
        (f"{base_s3_uri}/raw/kafka/security/*/*/*/*", "security", "json"),
        (f"{base_s3_uri}/raw/kafka/performance/*/*/*/*", "performance", "json"),
        (f"{base_s3_uri}/raw/rest/*/*/*/*", "rest", "json"),
        (f"{base_s3_uri}/raw/uploads/csv/*/*/*/*", "uploads_csv", "csv"),
        (f"{base_s3_uri}/raw/uploads/json/*/*/*/*", "uploads_json", "json"),
    ]

    for input_path, category, fmt in sources:
        process_and_convert_to_parquet(spark, input_path, category, fmt)

    exec_time = round(time.time() - start_time, 2)
    logger.info(f"🎉 Spark NOC ETL Ingestion finished in {exec_time}s.")
    spark.stop()


if __name__ == "__main__":
    main()
