# Telecom NOC Enterprise Ingestion Platform - Phase 2 Extension (`noc-ingestion`)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688.svg)](https://fastapi.tiangolo.com/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-red.svg)](https://min.io/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Multi_Topic-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.1_PySpark-orange.svg)](https://spark.apache.org/)
[![Iceberg Ready](https://img.shields.io/badge/Apache_Iceberg-Compatible_Parquet-blue.svg)](https://iceberg.apache.org/)

Enterprise-grade multi-source telemetry data ingestion platform for a **Telecom Network Operations Center (NOC) Dashboard Platform**. Built with Clean Architecture, non-blocking async IO, automated REST connector scheduling, multi-topic Kafka streaming, file upload processing, PostgreSQL audit tracking, and PySpark ETL processing outputting Iceberg-ready Parquet datasets.

---

## 🚀 Phase 2 Architectural Overview

The system extends the existing pipeline with additional ingestion vectors (Mock External Monitoring REST API, automated APScheduler REST connector, multipart file upload module, multi-topic Kafka producers/consumers, PostgreSQL upload history audit table, PySpark data cleaning, and Dashboard & Job monitoring REST APIs).

```
                           Data Sources

 Demo Generator      Mock REST API      CSV/Excel/JSON Upload
 (5s Continuous)    (FastAPI:8001)         (POST /upload/*)
        │                  │                      │
        │           APScheduler                   │
        │           (60s Poll)                    │
        └──────────────────┼──────────────────────┘
                           ▼
                FastAPI Ingestion Service
          (Validation, Normalization, Audit)
                           ▼
                    Apache Kafka
         (telecom-alarms, tickets, network,
          security, performance)
                           ▼
                   Kafka Consumers
                           ▼
                        MinIO
      raw/
          kafka/ (alarms, tickets, network, security, performance)
          rest/
          uploads/
      processed/ (parquet/ - Iceberg Ready)
      failed/
      archive/
                           ▼
                    Apache Spark
          (Clean, Validate, Deduplicate, Parquet)
                           ▼
               Ready for Apache Iceberg
```

---

## 📁 MinIO Folder Structure

Raw and processed telemetry landing zone layout in MinIO (`noc-raw-data` bucket):

```
raw/
    kafka/
        alarms/
        tickets/
        network/
        security/
        performance/
    rest/
    uploads/
        csv/
        excel/
        json/
        pdf/
processed/
    parquet/
        alarms/
        tickets/
        network/
        security/
        performance/
        rest/
        uploads_csv/
        uploads_json/
failed/
archive/
```

---

## 🛰️ API Endpoint Reference

### 1. Mock External REST API (`http://localhost:8001`)
- `GET /health`: Health status.
- `GET /api/v1/alarms`: Live simulated Comarch OSS alarm telemetry.
- `GET /api/v1/tickets`: Live simulated ServiceNow incident tickets.
- `GET /api/v1/network-events`: Cell tower and router health KPI events.
- `GET /api/v1/security-events`: Trend Micro & CyberArk security threat events.
- `GET /api/v1/performance`: Device CPU, memory, latency, packet loss metrics.
- `GET /api/v1/sites`: NOC site inventory.
- `GET /api/v1/devices`: Network device inventory.

### 2. Main NOC Ingestion API (`http://localhost:8000`)

#### File Upload & History APIs
- `POST /api/v1/upload/csv`: Upload, parse CSV, store original in MinIO, track in Postgres, publish to Kafka.
- `POST /api/v1/upload/excel`: Upload, parse Excel (.xlsx/.xls), store in MinIO, track in Postgres, publish to Kafka.
- `POST /api/v1/upload/json`: Upload JSON file, store in MinIO, track in Postgres, publish to Kafka.
- `POST /api/v1/upload/pdf`: Upload PDF document, store in MinIO, track metadata in Postgres.
- `GET /api/v1/uploads`: Fetch file upload history list from PostgreSQL.
- `GET /api/v1/uploads/{id}`: Get upload record by ID.
- `DELETE /api/v1/uploads/{id}`: Delete upload record from history.

#### Dashboard Analytics APIs
- `GET /dashboard/summary`: High-level ingestion statistics, active streams, and system health.
- `GET /dashboard/alarms`: Active alarm breakdown by severity (CRITICAL, MAJOR, MINOR, WARNING, INFO) and vendor.
- `GET /dashboard/tickets`: Incident ticket metrics by priority (P1-P4) and state.
- `GET /dashboard/uploads`: Summary file upload statistics across formats.
- `GET /dashboard/jobs`: Summary telemetry of Spark processing job executions.
- `GET /dashboard/performance`: Aggregate latency, packet loss, and throughput health metrics.

#### Job Monitoring APIs
- `GET /jobs`: Execution history of Spark batch ETL jobs.
- `GET /jobs/{id}`: Detailed status of a specific Spark processing job execution.
- `POST /jobs/run`: Trigger execution of Spark batch processing job on demand.
- `GET /processing/status`: Real-time operational status of Kafka Consumers, REST Schedulers, and Spark engine.

## ☁️ AWS EC2 Deployment & Validation

For step-by-step instructions on deploying and validating this platform on your AWS EC2 instance (including Kafka, Spark, MinIO, Postgres, and Docker Compose validation), refer to the [EC2 Deployment Guide](file:///Users/adilnawaz/Music/python-learning/noc-ingestion/DEPLOYMENT_EC2.md).

