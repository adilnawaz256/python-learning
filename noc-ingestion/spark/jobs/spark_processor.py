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
    """Initializes SparkSession configured for MinIO S3A storage, Iceberg catalog, and Structured Streaming."""
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
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.threads.max", "20")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.streaming.schemaInference", "true")
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


def make_micro_batch_handler(category: str, iceberg_table_name: str):
    """Creates a micro-batch handler function for Structured Streaming writeStream.foreachBatch."""
    def process_micro_batch(batch_df, batch_id):
        if batch_df is None:
            return

        try:
            if batch_df.rdd.isEmpty():
                return
        except Exception:
            pass

        logger.info(f"[STREAMING BATCH] Processing micro-batch {batch_id} for category '{category}' -> Iceberg 'iceberg.noc.{iceberg_table_name}'")

        cleaned_df = normalize_and_clean_dataframe(batch_df)
        bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")

        # 1. Append clean Parquet files to landing zone
        target_parquet_path = f"s3a://{bucket_name}/processed/parquet/{category}/"
        try:
            cleaned_df.write.mode("append").partitionBy("event_date").parquet(target_parquet_path)
        except Exception as pe:
            logger.warning(f"Parquet landing zone write note for {category}: {pe}")

        # 2. Append to Apache Iceberg catalog table
        full_iceberg_target = f"iceberg.noc.{iceberg_table_name}"
        try:
            try:
                cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").append()
                logger.info(f"✅ Micro-batch {batch_id} appended to Iceberg table '{full_iceberg_target}'")
            except Exception:
                logger.info(f"Table '{full_iceberg_target}' not present. Creating table...")
                cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").create()
                logger.info(f"✅ Created and populated Iceberg table '{full_iceberg_target}'")
        except Exception as ie:
            logger.error(f"❌ Failed to write micro-batch {batch_id} to Iceberg table '{full_iceberg_target}': {ie}")

    return process_micro_batch


def start_streaming_query(spark: SparkSession, s3_input_path: str, category: str, iceberg_table_name: str, file_format: str = "json"):
    """Starts an incremental PySpark Structured Streaming query with S3A MinIO checkpointing."""
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    checkpoint_location = f"s3a://{bucket_name}/checkpoints/{category}/"
    trigger_seconds = int(os.getenv("SPARK_TRIGGER_SECONDS", "10"))

    logger.info(f"Initializing streaming query for '{category}' at: {s3_input_path}")
    logger.info(f"Checkpoint location: {checkpoint_location}")

    try:
        if file_format == "json":
            reader = spark.readStream.option("recursiveFileLookup", "true").option("multiline", "true").format("json")
        elif file_format == "csv":
            reader = spark.readStream.option("recursiveFileLookup", "true").option("header", "true").format("csv")
        else:
            logger.warning(f"Unsupported format '{file_format}' for category '{category}'")
            return None

        df_stream = reader.load(s3_input_path)
        handler = make_micro_batch_handler(category, iceberg_table_name)

        query = (
            df_stream.writeStream
            .foreachBatch(handler)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        logger.info(f"🚀 Streaming query started for '{category}' (Query ID: {query.id})")
        return query
    except Exception as e:
        logger.error(f"Failed to start streaming query for {s3_input_path}: {e}")
        return None


def main():
    start_time = time.time()
    logger.info("Initializing Continuous Incremental PySpark ETL Engine for Iceberg Tables...")

    # Step 1: Spark Session
    spark = create_spark_session()

    # Step 2: Source Path Construction
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    base_s3_uri = f"s3a://{bucket_name}"

    sources = [
        (f"{base_s3_uri}/raw/kafka/alarms", "alarms", "alarms", "json"),
        (f"{base_s3_uri}/raw/kafka/tickets", "tickets", "tickets", "json"),
        (f"{base_s3_uri}/raw/kafka/network", "network", "network_events", "json"),
        (f"{base_s3_uri}/raw/kafka/security", "security", "security_events", "json"),
        (f"{base_s3_uri}/raw/kafka/performance", "performance", "performance_metrics", "json"),
        (f"{base_s3_uri}/raw/rest", "rest", "network_events", "json"),
        (f"{base_s3_uri}/raw/uploads/csv", "uploads_csv", "network_events", "csv"),
        (f"{base_s3_uri}/raw/uploads/json", "uploads_json", "network_events", "json"),
    ]

    active_queries = []
    for input_path, category, iceberg_table, fmt in sources:
        q = start_streaming_query(spark, input_path, category, iceberg_table, fmt)
        if q:
            active_queries.append(q)

    logger.info(f"Active streaming queries: {len(active_queries)}. Entering continuous streaming loop...")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Stopping active streaming queries...")
        for q in active_queries:
            q.stop()
        spark.stop()
        logger.info("Spark session stopped gracefully.")


if __name__ == "__main__":
    main()
