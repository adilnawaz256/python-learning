# Apache Spark Processing Environment (Phase 2)

This directory contains the complete Apache Spark cluster configuration, PySpark jobs, helper scripts, and future-ready lakehouse definitions for the Telecom NOC Data Platform.

---

## 🏗️ Architecture Overview

```
External Systems ──► FastAPI Ingestion ──► Kafka ──► MinIO (Raw Landing Zone)
                                                         │
                                                         ▼
                                            Apache Spark Cluster (Master + Worker)
                                            [Hadoop S3A FileSystem Connector]
                                                         │
                                                         ▼
                                            Lakehouse Target (Iceberg / Parquet)
```

---

## 📁 Directory Structure

```
spark/
├── config/
│   ├── spark-defaults.conf     # Global Spark & Hadoop S3A credentials/packages configuration
│   └── log4j2.properties        # Structured logging configuration for Spark jobs
├── jobs/
│   └── read_minio.py           # First PySpark job inspecting MinIO landing zone datasets
├── scripts/
│   └── submit_job.sh           # Helper script to execute spark-submit inside container
├── jars/
│   └── README.md               # Custom/Offline JAR dependency store
└── README.md                   # Complete Apache Spark Environment Documentation
```

---

## 🔌 MinIO Connectivity (Hadoop S3A)

Spark communicates with MinIO object storage using the **Hadoop S3A FileSystem connector** (`org.apache.hadoop.fs.s3a.S3AFileSystem`).

The connection parameters configured in `spark/config/spark-defaults.conf`:
- **Endpoint**: `http://minio:9000`
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
- **Path Style Access**: `true` (required for MinIO S3 API emulation)
- **Target Bucket**: `s3a://noc-raw-data`

---

## 🚀 How to Run & Submit Spark Jobs

### 1. Launch Docker Compose Environment
```bash
docker compose up -d
```

### 2. Verify Spark Master UI & Worker Status
- **Spark Master Web UI**: Open `http://<EC2-IP-or-localhost>:8080`
- **Spark Worker Web UI**: Open `http://<EC2-IP-or-localhost>:8081`
- Confirm `1` Worker is registered and `ALIVE` under Master `spark://spark-master:7077`.

### 3. Submit the MinIO Inspection PySpark Job

Option A: Using the helper script
```bash
./spark/scripts/submit_job.sh
```

Option B: Manual `spark-submit` command
```bash
docker exec -it noc-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --properties-file /opt/bitnami/spark/conf/spark-defaults.conf \
  /opt/bitnami/spark/spark-apps/jobs/read_minio.py
```

---

## 🔮 Future-Ready Architecture (Phase 3+)

The configuration in `spark-defaults.conf` includes pre-configured extensions for:
1. **Apache Iceberg**:
   - `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
   - Catalog `demo` pointing to `s3a://noc-raw-data/iceberg-warehouse`
2. **Hive Metastore & Trino**:
   - Cluster networking and catalog structures allow adding a Hive Metastore container or Trino coordinator without modifying the existing Spark cluster configuration.
