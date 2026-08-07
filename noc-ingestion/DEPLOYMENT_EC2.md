# AWS EC2 Deployment & Validation Guide - Phase 2 Extension

This guide provides step-by-step instructions for deploying and validating the **Enterprise NOC Ingestion Platform Phase 2 Extension** on your AWS EC2 instance environment.

---

## 1. Commit and Push Changes to Git

On your local development machine:

```bash
git add .
git commit -m "feat: Enterprise NOC Platform Phase 2 Extension multi-source ingestion"
git push origin main
```

---

## 2. Deploy on AWS EC2 Instance

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

Verify that all 6 services are running:

```bash
docker-compose ps
```

You should see:
- `noc-ingestion-app` (FastAPI: 8000)
- `noc-mock-external-api` (FastAPI Mock REST API: 8001)
- `noc-kafka` (Apache Kafka: 9092)
- `noc-minio` (MinIO Object Storage: 9000/9001)
- `noc-postgres` (PostgreSQL Audit: 5432)
- `noc-spark-master` & `noc-spark-worker` (Apache Spark Cluster: 8080/8081/7077)

---

## 3. Step-by-Step Runtime Validation on EC2

### Step A: Validate Health Endpoints
```bash
# Check Main Ingestion API
curl -s http://localhost:8000/health | jq

# Check Mock External REST API
curl -s http://localhost:8001/health | jq
```

### Step B: Validate Mock External REST APIs (`:8001`)
```bash
# Test Alarms Telemetry
curl -s http://localhost:8001/api/v1/alarms | jq

# Test ServiceNow Tickets Telemetry
curl -s http://localhost:8001/api/v1/tickets | jq

# Test Network Events
curl -s http://localhost:8001/api/v1/network-events | jq

# Test Security Events
curl -s http://localhost:8001/api/v1/security-events | jq

# Test Performance Metrics
curl -s http://localhost:8001/api/v1/performance | jq
```

### Step C: Validate Automated REST Connector Scheduler
Check application logs to observe the APScheduler polling external REST endpoints every 60 seconds:

```bash
docker logs -f noc-ingestion-app
```
Look for:
`REST Connector Scheduler started (polling every 60s)`
`REST Connector Poll Status: SUCCESS`

### Step D: Validate File Upload & Upload History APIs (`:8000`)
```bash
# 1. Test CSV Upload
curl -X POST -F "file=@sample-data/alarms.csv" http://localhost:8000/api/v1/upload/csv | jq

# 2. Test Excel Upload
curl -X POST -F "file=@sample-data/network_kpi.xlsx" http://localhost:8000/api/v1/upload/excel | jq

# 3. Test Upload History Listing (PostgreSQL)
curl -s http://localhost:8000/api/v1/uploads | jq
```

### Step E: Validate Multi-Topic Kafka Streams
Verify active Kafka topics inside the container:

```bash
docker exec -it noc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Expected topics:
- `telecom-events` (Existing Demo Pipeline)
- `telecom-alarms`
- `tickets`
- `network-events`
- `security-events`
- `performance`

### Step F: Validate MinIO Raw Storage Folder Layout
Access MinIO Console at `http://<EC2-PUBLIC-IP>:9001` (Credentials: `minioadmin` / `minioadmin`) or check bucket `noc-raw-data`:

Structure:
```
noc-raw-data/
├── raw/
│   ├── kafka/
│   │   ├── alarms/
│   │   ├── tickets/
│   │   ├── network/
│   │   ├── security/
│   │   └── performance/
│   ├── rest/
│   └── uploads/
│       ├── csv/
│       ├── excel/
│       ├── json/
│       └── pdf/
```

### Step G: Validate Spark PySpark ETL Processing (Iceberg Ready)
Execute Spark batch ETL processing on demand:

```bash
# Trigger via API
curl -X POST http://localhost:8000/jobs/run | jq

# Or execute job directly on Spark Master container
docker exec -it noc-spark-master /opt/spark/bin/spark-submit /opt/spark/spark-apps/jobs/spark_processor.py
```

Verify clean Parquet files are generated under:
`noc-raw-data/processed/parquet/`

### Step H: Validate Dashboard & Monitoring APIs
```bash
# Dashboard Summary Overview
curl -s http://localhost:8000/dashboard/summary | jq

# Alarms Severity Breakdown
curl -s http://localhost:8000/dashboard/alarms | jq

# Real-Time Processing Status
curl -s http://localhost:8000/processing/status | jq

# Jobs History
curl -s http://localhost:8000/jobs | jq
```
