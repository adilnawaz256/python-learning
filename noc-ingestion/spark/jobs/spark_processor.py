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
    get_json_object,
)
from pyspark.sql.types import StructType, MapType, StringType

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NOC-Spark-ETL-Processor")

# Global in-memory cache for confirmed existing Iceberg tables
KNOWN_ICEBERG_TABLES = set()


def extract_field_from_payload(df, target_col_name, payload_candidates):
    """
    Safely extracts `target_col_name` from top-level `df` or nested `payload`.
    Supports StructType, MapType, and JSON StringType safely.
    """
    if target_col_name in df.columns:
        return df

    if "payload" not in df.columns:
        return df.withColumn(target_col_name, lit(None).cast("string"))

    payload_dtype = df.schema["payload"].dataType

    # 1. PySpark StructType (ROW)
    if isinstance(payload_dtype, StructType):
        struct_field_names = [f.name for f in payload_dtype.fields]
        matched_field = next((c for c in payload_candidates if c in struct_field_names), None)
        if matched_field:
            return df.withColumn(target_col_name, col(f"payload.{matched_field}").cast("string"))
        else:
            return df.withColumn(target_col_name, lit(None).cast("string"))

    # 2. PySpark MapType
    elif isinstance(payload_dtype, MapType):
        expr = None
        for candidate in payload_candidates:
            cand_expr = col("payload")[candidate]
            expr = cand_expr if expr is None else coalesce(expr, cand_expr)
        return df.withColumn(target_col_name, expr.cast("string"))

    # 3. PySpark StringType (JSON string)
    elif isinstance(payload_dtype, StringType):
        expr = None
        for candidate in payload_candidates:
            cand_expr = get_json_object(col("payload"), f"$.{candidate}")
            expr = cand_expr if expr is None else coalesce(expr, cand_expr)
        return df.withColumn(target_col_name, expr.cast("string"))

    else:
        return df.withColumn(target_col_name, lit(None).cast("string"))


def create_spark_session() -> SparkSession:
    """Initializes SparkSession configured for MinIO S3A storage, Iceberg catalog, FAIR scheduling, and Structured Streaming."""
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
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
            .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "4"))
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
        logger.info("[STEP 1 COMPLETE] SparkSession initialized with FAIR scheduler. Took %.2f sec", elapsed)

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
    """Performs schema normalization, severity standardization, timestamp parsing, deduplication, and nested payload extraction."""
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

    # 3. Extract key entity attributes from nested payload to top-level columns if missing
    df = extract_field_from_payload(df, "vendor", ["vendor", "vendor_name", "vendorName", "manufacturer"])
    df = extract_field_from_payload(df, "site_name", ["site_name", "siteName", "site_id", "siteId"])
    df = extract_field_from_payload(df, "device_name", ["device_name", "deviceName", "device_id", "deviceId", "node_name"])
    df = extract_field_from_payload(df, "ticket_id", ["ticket_id", "ticketId", "inc_id", "incident_id"])

    # 4. Deduplication
    id_col = None
    for candidate in ["event_id", "alarm_id", "ticket_id", "metric_id"]:
        if candidate in df.columns:
            id_col = candidate
            break

    if id_col:
        df = df.dropDuplicates([id_col])

    return df


def check_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    """Checks whether an Apache Iceberg catalog table already exists with in-memory caching."""
    if full_table_name in KNOWN_ICEBERG_TABLES:
        return True

    try:
        if spark.catalog.tableExists(full_table_name):
            KNOWN_ICEBERG_TABLES.add(full_table_name)
            return True
    except Exception:
        pass

    try:
        spark.read.table(full_table_name).limit(1).collect()
        KNOWN_ICEBERG_TABLES.add(full_table_name)
        return True
    except Exception:
        return False


def make_micro_batch_handler(category: str, iceberg_table_name: str):
    """Creates a micro-batch handler function for Structured Streaming writeStream.foreachBatch."""
    def process_micro_batch(batch_df, batch_id):
        # EXPLICIT FIRST LINE LOGGING FOR DIAGNOSTICS
        logger.info(f"🔔 [CALLBACK ENTERED] Category '{category}', Micro-batch ID: {batch_id}, DataFrame is None? {batch_df is None}")

        if batch_df is None:
            return

        # Assign micro-batch job execution to category pool under Spark FAIR scheduler
        try:
            batch_df.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", category)
            logger.info(f"  └─ Set FAIR scheduler pool property to '{category}'")
        except Exception as pe:
            logger.warning(f"Could not set scheduler pool property: {pe}")

        # Check if micro-batch contains input rows
        has_data = False
        try:
            has_data = len(batch_df.head(1)) > 0
        except Exception as head_err:
            logger.warning(f"  └─ Note checking head(1) for category '{category}': {head_err}")

        if not has_data:
            logger.info(f"ℹ️ [MICRO-BATCH {batch_id}] Category '{category}' triggered with 0 input rows (idle trigger or offsets already processed).")
            return

        logger.info(f"⚡ [STREAMING BATCH] Processing micro-batch {batch_id} for category '{category}' with input data -> Iceberg 'iceberg.noc.{iceberg_table_name}'")

        try:
            cleaned_df = normalize_and_clean_dataframe(batch_df)
            if not cleaned_df.head(1):
                logger.info(f"ℹ️ [MICRO-BATCH {batch_id}] Category '{category}' cleaning resulted in 0 rows.")
                return

            bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")

            # 1. Append clean Parquet files to landing zone
            target_parquet_path = f"s3a://{bucket_name}/processed/parquet/{category}/"
            try:
                cleaned_df.write.mode("append").partitionBy("event_date").parquet(target_parquet_path)
            except Exception as pe:
                logger.warning(f"Parquet landing zone write note for {category}: {pe}")

            # 2. Append to Apache Iceberg catalog table
            full_iceberg_target = f"iceberg.noc.{iceberg_table_name}"
            spark = batch_df.sparkSession

            is_existing = check_table_exists(spark, full_iceberg_target)

            if is_existing:
                cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").append()
                logger.info(f"✅ Micro-batch {batch_id} appended to existing Iceberg table '{full_iceberg_target}'")
            else:
                logger.info(f"Table '{full_iceberg_target}' does not exist yet. Creating table...")
                cleaned_df.writeTo(full_iceberg_target).tableProperty("write.format.default", "parquet").create()
                KNOWN_ICEBERG_TABLES.add(full_iceberg_target)
                logger.info(f"✅ Created and populated Iceberg table '{full_iceberg_target}'")

        except Exception as ie:
            logger.error(f"❌ Error processing micro-batch {batch_id} for category '{category}' (Iceberg table 'iceberg.noc.{iceberg_table_name}'): {ie}")

    return process_micro_batch


def start_streaming_query(spark: SparkSession, s3_input_path: str, category: str, iceberg_table_name: str, file_format: str = "json"):
    """Starts an incremental PySpark Structured Streaming query with S3A MinIO checkpointing and maxFilesPerTrigger."""
    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    checkpoint_location = f"s3a://{bucket_name}/checkpoints/{category}/"
    trigger_seconds = int(os.getenv("SPARK_TRIGGER_SECONDS", "15"))
    max_files_per_trigger = int(os.getenv("SPARK_MAX_FILES_PER_TRIGGER", "50"))

    # EXPLICIT LOGGING IMMEDIATELY BEFORE QUERY START
    logger.info(f"➡️ [PRE-START] Preparing writeStream for category '{category}' at: {s3_input_path}")
    logger.info(f"   Checkpoint: {checkpoint_location}")
    logger.info(f"   maxFilesPerTrigger: {max_files_per_trigger}, triggerSeconds: {trigger_seconds}")

    try:
        if file_format == "json":
            reader = (
                spark.readStream
                .option("recursiveFileLookup", "true")
                .option("multiline", "true")
                .option("maxFilesPerTrigger", max_files_per_trigger)
                .format("json")
            )
        elif file_format == "csv":
            reader = (
                spark.readStream
                .option("recursiveFileLookup", "true")
                .option("header", "true")
                .option("maxFilesPerTrigger", max_files_per_trigger)
                .format("csv")
            )
        else:
            logger.warning(f"Unsupported format '{file_format}' for category '{category}'")
            return None

        df_stream = reader.load(s3_input_path)
        handler = make_micro_batch_handler(category, iceberg_table_name)

        query = (
            df_stream.writeStream
            .queryName(f"query_{category}")
            .foreachBatch(handler)
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
        
        # EXPLICIT LOGGING IMMEDIATELY AFTER QUERY START SUCCESS
        logger.info(f"✅ [POST-START] Query '{category}' started successfully! (Query ID: {query.id}, Run ID: {query.runId}, isActive: {query.isActive})")
        return query
    except Exception as e:
        logger.error(f"❌ [START ERROR] Failed to start streaming query for {s3_input_path}: {e}")
        return None


def main():
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
        loop_counter = 0
        while True:
            time.sleep(15)
            loop_counter += 1
            if loop_counter % 2 == 0:  # Every 30 seconds
                for q in active_queries:
                    if q.isActive:
                        status_str = "Active"
                        try:
                            status_str = q.status.get('message', 'Active') if isinstance(q.status, dict) else str(q.status)
                        except Exception:
                            pass
                        num_rows = 0
                        try:
                            if q.lastProgress and isinstance(q.lastProgress, dict):
                                num_rows = q.lastProgress.get('numInputRows', 0)
                        except Exception:
                            pass
                        logger.info(f"📊 [MONITOR] Query '{q.name}' (ID: {q.id[:8]}...): status='{status_str}', lastInputRows={num_rows}")
                    else:
                        err = q.exception()
                        logger.error(f"❌ [MONITOR] Query '{q.name}' (ID: {q.id[:8]}...) STOPPED! Exception: {err}")
    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Stopping active streaming queries...")
        for q in active_queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        logger.info("Spark session stopped gracefully.")


if __name__ == "__main__":
    main()
