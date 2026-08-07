# Telecom NOC Enterprise Ingestion & Analytics Platform (`noc-ingestion`)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688.svg)](https://fastapi.tiangolo.com/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-red.svg)](https://min.io/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Multi_Topic-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.1_PySpark-orange.svg)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-Open_Table_Format-blue.svg)](https://iceberg.apache.org/)
[![Apache Trino](https://img.shields.io/badge/Apache_Trino-SQL_Query_Engine-red.svg)](https://trino.io/)
[![Grafana](https://img.shields.io/badge/Grafana-10.4_Dashboards-orange.svg)](https://grafana.com/)

Enterprise-grade multi-source telemetry data ingestion and analytics platform for a **Telecom Network Operations Center (NOC) Dashboard Platform**. Built with Clean Architecture, non-blocking async IO, automated REST connector scheduling, multi-topic Kafka streaming, file upload processing, PostgreSQL audit tracking, PySpark ETL processing writing to Apache Iceberg tables, Apache Trino SQL query engine, and Grafana dashboards.

---

## 🚀 Complete Platform Architecture

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
      processed/
      iceberg-warehouse/
                           ▼
                    Apache Spark
          (Clean, Validate, Deduplicate)
                           ▼
                  Apache Iceberg Tables
      (alarms, tickets, network_events,
       security_events, performance_metrics)
                           ▼
                  Apache Trino (SQL Engine)
           (Port 8082: SHOW TABLES, SELECT SQL)
                           ▼
                  Grafana Dashboards
           (Port 3000: 5 Starter Dashboards)
```

---

## 📁 MinIO Folder & Iceberg Catalog Layout

Raw telemetry and Iceberg open table storage layout in MinIO (`noc-raw-data` bucket):

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
├── processed/
│   └── parquet/
└── iceberg-warehouse/
    └── noc/
        ├── alarms/
        ├── tickets/
        ├── network_events/
        ├── security_events/
        └── performance_metrics/
```

---

## 📊 Grafana Starter Dashboards

The system comes pre-configured with 5 Grafana dashboards querying Apache Iceberg tables via Trino:
1. 📊 **NOC Alarms Analytics Dashboard**: Active Alarms by Severity & Vendor distribution.
2. 🎫 **NOC Trouble Tickets Dashboard**: Incident Priority (P1-P4) & Resolution State metrics.
3. 📡 **NOC Network Events Dashboard**: Cell tower outages & network health metrics by region.
4. 🛡️ **NOC Security Threat Events Dashboard**: Threat levels & security event actions.
5. 📈 **NOC Performance Metrics Dashboard**: Device CPU, Memory, Latency, & Packet Loss telemetry.

---

## ☁️ AWS EC2 Deployment & Validation Guide

For step-by-step instructions on deploying and validating this platform on your AWS EC2 instance (including Docker Compose, Spark Iceberg ETL execution, Trino SQL verification, and Grafana dashboard checks), refer to the [EC2 Deployment Guide](file:///Users/adilnawaz/Music/python-learning/noc-ingestion/DEPLOYMENT_EC2.md).
