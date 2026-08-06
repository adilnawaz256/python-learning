import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NOC-Spark-MinIO-Reader")


def create_spark_session() -> SparkSession:
    """Initializes SparkSession with Hadoop S3A MinIO settings."""
    builder = (
        SparkSession.builder.appName("NOC-Spark-MinIO-Reader")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_data_type(spark: SparkSession, s3_path: str, file_format: str) -> None:
    """Reads dataset from S3A path, prints schema, row count, and top 20 rows."""
    logger.info(f"Checking for files in path: {s3_path}")
    try:
        if file_format == "json":
            df = spark.read.option("multiline", "true").json(s3_path)
        elif file_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        elif file_format == "excel":
            df = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(s3_path)
        else:
            logger.warning(f"Unsupported format '{file_format}' for path {s3_path}")
            return

        row_count = df.count()
        logger.info(f"Files Found under {s3_path}")
        logger.info(f"Rows Read: {row_count}")

        print(f"\n=======================================================")
        print(f" Schema for {s3_path}")
        print(f"=======================================================")
        df.printSchema()

        print(f"\n=======================================================")
        print(f" First 20 Rows ({s3_path})")
        print(f"=======================================================")
        df.show(20, truncate=False)

    except AnalysisException as ae:
        if "Path does not exist" in str(ae) or "No files found" in str(ae):
            logger.info(f"Path '{s3_path}' is empty or does not exist yet (handled gracefully).")
        else:
            logger.error(f"Analysis error reading '{s3_path}': {ae}")
    except Exception as e:
        logger.error(f"Failed reading '{s3_path}': {e}")


def main():
    start_time = time.time()
    logger.info("Connecting to MinIO...")

    try:
        spark = create_spark_session()
        logger.info("Connected to MinIO")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO / initialize Spark session: {e}")
        sys.exit(1)

    bucket_name = os.getenv("MINIO_BUCKET", "noc-raw-data")
    base_s3_uri = f"s3a://{bucket_name}"
    logger.info(f"Bucket Found: {bucket_name}")

    # Data categories to inspect in MinIO raw landing zone
    target_paths = [
        (f"{base_s3_uri}/raw/json/", "json"),
        (f"{base_s3_uri}/raw/kafka/", "json"),
        (f"{base_s3_uri}/raw/rest/", "json"),
        (f"{base_s3_uri}/raw/csv/", "csv"),
        (f"{base_s3_uri}/raw/excel/", "excel"),
    ]

    for path, fmt in target_paths:
        read_data_type(spark, path, fmt)

    execution_time = round(time.time() - start_time, 3)
    logger.info(f"Execution Time: {execution_time} seconds")
    spark.stop()


if __name__ == "__main__":
    main()
