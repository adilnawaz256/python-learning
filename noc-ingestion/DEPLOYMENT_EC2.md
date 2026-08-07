# AWS EC2 Deployment & Validation Guide - Production Apache Iceberg REST Catalog, Trino & Grafana

This guide provides step-by-step instructions for deploying and validating the **Analytics Layer** (Apache Iceberg REST Catalog backed by PostgreSQL, Apache Trino 435 SQL Engine, and Grafana Dashboards) on your AWS EC2 instance.

---

## 1. Production Apache Iceberg REST Catalog (PostgreSQL-backed) & Trino 435 Configuration

In production architectures, Apache Iceberg uses an open-standard **Iceberg REST Catalog** (`http://iceberg-rest:8181`) to coordinate table metadata atomically between Spark (Writer) and Trino 435 (Reader). The Iceberg REST service persists catalog table metadata in the existing PostgreSQL database (`noc-postgres`), ensuring catalog metadata survives container restarts.

### `trino/catalog/iceberg.properties`:
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.path-style-access=true
```

### `spark/config/spark-defaults.conf`:
```conf
spark.sql.extensions                org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg           org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type      rest
spark.sql.catalog.iceberg.uri       http://iceberg-rest:8181
spark.sql.catalog.iceberg.warehouse s3a://noc-raw-data/iceberg-warehouse
```

---

## 2. Commit and Deploy to AWS EC2 Instance

On your local development machine:

```bash
git add .
git commit -m "feat: Persistent PostgreSQL-backed Apache Iceberg REST catalog for Spark and Trino 435"
git push origin main
```

SSH into your AWS EC2 instance:

```bash
ssh -i /path/to/your-key.pem ec2-user@<YOUR-EC2-PUBLIC-IP>
```

Navigate to your project directory and pull the latest code:

```bash
cd noc-ingestion
git pull origin main
```

Rebuild and launch all containers with Docker Compose:

```bash
docker-compose down
docker-compose up -d --build
```

Verify all 9 services are running and in the `Up` state:

```bash
docker-compose ps
```

You should see:
1. `noc-ingestion-app` (FastAPI Ingestion Service: 8000)
2. `noc-mock-external-api` (Mock Monitoring REST API: 8001)
3. `noc-kafka` (Apache Kafka Broker: 9092)
4. `noc-minio` (MinIO Object Storage: 9000/9001)
5. `noc-postgres` (PostgreSQL Audit & Iceberg Metadata DB: 5432)
6. `noc-spark-master` & `noc-spark-worker` (Apache Spark Cluster: 8080/8081/7077)
7. `noc-iceberg-rest` (PostgreSQL-backed Iceberg REST Catalog Server: 8181)
8. `noc-trino` (Apache Trino SQL Query Engine: 8082)
9. `noc-grafana` (Grafana Dashboards Engine: 3000)

---

## 3. Step-by-Step Runtime Validation on EC2

### Step A: Execute Spark Job to Populate Iceberg Tables
Trigger the PySpark ETL job to process raw telemetry and populate the Apache Iceberg REST tables:

```bash
docker exec -it noc-spark-master /opt/spark/bin/spark-submit /opt/spark/spark-apps/jobs/spark_processor.py
```

Check Spark logs:
Look for:
- `Appended records to Iceberg table 'iceberg.noc.alarms'`
- `Appended records to Iceberg table 'iceberg.noc.tickets'`
- `Appended records to Iceberg table 'iceberg.noc.network_events'`
- `Appended records to Iceberg table 'iceberg.noc.security_events'`
- `Appended records to Iceberg table 'iceberg.noc.performance_metrics'`

---

### Step B: Verify Trino SQL Queries on Iceberg REST Tables
Connect to the Trino container and query all 5 Apache Iceberg tables with standard SQL:

```bash
docker exec -it noc-trino trino
```

Inside Trino SQL Prompt, run:

```sql
-- 1. Show available catalogs
SHOW CATALOGS;

-- 2. Show schemas inside Iceberg catalog
SHOW SCHEMAS FROM iceberg;

-- 3. Show tables inside schema 'noc'
SHOW TABLES FROM iceberg.noc;

-- 4. Query Alarms Iceberg Table
SELECT event_id, node_id, region, vendor, severity_clean, event_timestamp
FROM iceberg.noc.alarms
LIMIT 10;

-- 5. Query Trouble Tickets Iceberg Table
SELECT event_id, source, severity_clean, event_timestamp
FROM iceberg.noc.tickets
LIMIT 10;

-- 6. Query Network Events Iceberg Table
SELECT event_id, node_id, region, severity_clean, event_timestamp
FROM iceberg.noc.network_events
LIMIT 10;

-- 7. Query Security Events Iceberg Table
SELECT event_id, node_id, region, severity_clean, event_timestamp
FROM iceberg.noc.security_events
LIMIT 10;

-- 8. Query Performance Metrics Iceberg Table
SELECT event_id, node_id, region, severity_clean, event_timestamp
FROM iceberg.noc.performance_metrics
LIMIT 10;
```

---

### Step C: Verify Grafana Dashboards & Trino Data Source
Access Grafana Web Console in your browser:

- **URL**: `http://<EC2-PUBLIC-IP>:3000`
- **Username**: `admin`
- **Password**: `admin`

Navigate to **Dashboards** > **NOC Analytics** folder to view the 5 live dashboards querying Apache Iceberg REST tables via Trino.
