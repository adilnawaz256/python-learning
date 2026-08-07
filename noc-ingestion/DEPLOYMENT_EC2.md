# AWS EC2 Deployment & Validation Guide - Production Apache Iceberg REST Catalog, Trino & Grafana

This guide provides step-by-step instructions for deploying and validating the **Analytics Layer** (Apache Iceberg REST Catalog backed by PostgreSQL, Apache Trino 435 SQL Engine, and Grafana Dashboards) on your AWS EC2 instance.

---

## 1. Classpath & AWS S3A SDK Pre-fetching Configuration

To guarantee `com.amazonaws.AmazonClientException` and `org.apache.hadoop.fs.s3a.S3AFileSystem` are present on Spark's JVM ClassLoader at runtime without relying on Ivy dynamic resolution:

- **Automatic Pre-fetching (`spark/entrypoint.sh`)**: Pre-fetches `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`, and `iceberg-spark-runtime-3.5_2.12-1.5.0.jar` into `/opt/spark/extra-jars/` when container boots up.
- **System Classpath Injection (`spark/config/spark-defaults.conf`)**:
  ```conf
  spark.driver.extraClassPath         /opt/spark/extra-jars/*:/opt/spark/jars/*
  spark.executor.extraClassPath       /opt/spark/extra-jars/*:/opt/spark/jars/*
  spark.jars                          /opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar
  ```

---

## 2. Commit and Deploy to AWS EC2 Instance

On your local development machine:

```bash
git add .
git commit -m "fix: Pre-fetch AWS SDK & S3A JARs into extra-jars and configure JVM extraClassPath"
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

Launch all containers:

```bash
docker-compose down
docker-compose up -d
docker-compose ps
```

Verify all 9 services are running and in the `Up` state:
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

### Step B: Verify Trino SQL Queries on Iceberg REST Tables
Connect to the Trino container and query all 5 Apache Iceberg tables with standard SQL:

```bash
docker exec -it noc-trino trino
```

Inside Trino SQL Prompt, run:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.noc;

SELECT * FROM iceberg.noc.alarms LIMIT 10;
SELECT * FROM iceberg.noc.tickets LIMIT 10;
SELECT * FROM iceberg.noc.network_events LIMIT 10;
SELECT * FROM iceberg.noc.security_events LIMIT 10;
SELECT * FROM iceberg.noc.performance_metrics LIMIT 10;
```

---

### Step C: Verify Grafana Dashboards & Trino Data Source
Access Grafana Web Console in your browser:

- **URL**: `http://<EC2-PUBLIC-IP>:3000`
- **Username**: `admin`
- **Password**: `admin`

Navigate to **Dashboards** > **NOC Analytics** folder to view the 5 live dashboards querying Apache Iceberg REST tables via Trino.
