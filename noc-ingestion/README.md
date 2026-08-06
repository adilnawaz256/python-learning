# Telecom NOC Data Ingestion Layer (`noc-ingestion`)

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688.svg)](https://fastapi.tiangolo.com/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-red.svg)](https://min.io/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7.0-black.svg)](https://kafka.apache.org/)
[![Demo Mode](https://img.shields.io/badge/Demo_Mode-Supported-green.svg)](#-demo-mode--data-generator)

Production-Ready Data Ingestion Layer for a **Telecom Network Operations Center (NOC) Dashboard Platform**. Built following Clean Architecture, SOLID design principles, and fully non-blocking asynchronous processing.

---

## 🚀 Architectural Overview

The **NOC Ingestion Service** ingests telemetry, alarms, tickets, security metrics, and log files from heterogeneous telecom data sources, validates their structure, and persists raw data into **MinIO Object Storage** using structured partition keys.

```
                         +-----------------------------------+
                         |      Telecom Data Sources         |
                         +-----------------------------------+
                         |  - Comarch OSS (Alarm Events)     |
                         |  - ServiceNow / CyberArk REST     |
                         |  - NOC Files (CSV/Excel/JSON/PDF) |
                         +-----------------+-----------------+
                                           |
                                           v
                         +-----------------------------------+
                         |      noc-ingestion Service        |
                         |        (FastAPI 3.12 Engine)      |
                         +-----------------+-----------------+
                                           |
         +---------------------------------+---------------------------------+
         |                                 |                                 |
         v                                 v                                 v
+-----------------+               +-----------------+               +-----------------+
| Kafka Consumer  |               | REST Collector  |               | File Upload     |
| (aiokafka)      |               | (httpx)         |               | (pandas/pypdf)  |
+--------+--------+               +--------+--------+               +--------+--------+
         |                                 |                                 |
         +---------------------------------+---------------------------------+
                                           |
                                           v
                         +-----------------------------------+
                         |    MinIO Object Storage Client    |
                         |   raw/{category}/YYYY/MM/DD/file  |
                         +-----------------------------------+
```

---

## 🎮 DEMO MODE & DATA GENERATOR

The service includes a built-in **Demo Data Generator** that simulates all external telecom source systems without requiring live external integrations.

### Features in Demo Mode (`DEMO_MODE=true`):
1. **Kafka Demo Producer**: Publishes realistic Comarch OSS alarm events every **5 seconds** directly into the ingestion pipeline and MinIO storage.
2. **Demo REST APIs**: Exposes endpoints returning 50-100 realistic mock records per request.
3. **Demo File Generator**: Automatically creates and updates sample files inside `sample-data/` every **60 seconds**:
   - `sample-data/alarms.csv`
   - `sample-data/tickets.csv`
   - `sample-data/network_kpi.xlsx`
   - `sample-data/alarm.json`

### Switching Between Modes:
Simply change `DEMO_MODE` in `.env`:
- `DEMO_MODE=true`: Enables background demo producer, demo file generator, and `/demo/*` endpoints.
- `DEMO_MODE=false`: Production mode connecting exclusively to live Kafka brokers and real external REST endpoints.

---

## 📁 Project Structure

```
noc-ingestion/
├── app/
│   ├── api/
│   │   ├── routes/
│   │   │   ├── demo.py           # [DEMO] GET /demo/alarms, /tickets, /network-health, /security-events
│   │   │   ├── health.py         # GET /health, GET /metrics
│   │   │   └── ingestion.py      # POST /upload, POST /simulate/kafka, POST /simulate/rest
│   │   └── deps.py               # Dependency Injection providers
│   ├── config/
│   │   └── config.py             # Pydantic BaseSettings & Environment management (DEMO_MODE toggle)
│   ├── core/
│   │   ├── logger.py             # Structured JSON logger
│   │   └── exceptions.py         # Domain exception hierarchy
│   ├── database/
│   │   ├── session.py            # SQLAlchemy async session manager
│   │   └── repository.py         # Repository pattern for audit logs
│   ├── demo/                     # Demo Data Generator Module
│   │   ├── kafka_producer.py     # Demo Kafka Producer (emits every 5s)
│   │   ├── file_generator.py      # Auto-populates sample-data/ files
│   │   ├── rest_generator.py      # Dynamic mock telecom record generator
│   │   └── scheduler.py           # Lifespan background task orchestrator
│   ├── models/
│   │   └── audit.py              # Ingestion audit log database schema
│   ├── schemas/
│   │   ├── kafka_schema.py       # Pydantic models for Comarch OSS, Alarms, Tickets
│   │   ├── rest_schema.py        # Pydantic models for REST API requests/responses
│   │   ├── file_schema.py        # Metadata validation schemas for CSV/Excel/JSON/PDF
│   │   └── response_schema.py    # Standardized API response format
│   ├── services/
│   │   ├── kafka/
│   │   │   ├── consumer.py       # Async Kafka Consumer with retry logic
│   │   │   └── simulator.py      # Telecom NOC event simulator
│   │   ├── rest/
│   │   │   └── collector.py      # REST Collector with auth, pagination & backoff retries
│   │   ├── file/
│   │   │   └── processor.py      # File inspector, validator & MinIO uploader
│   │   └── storage/
│   │       └── minio/
│   │           └── client.py     # Singleton MinIO client with auto-bucket creation
│   └── main.py                   # FastAPI Application Factory & Lifespan manager
├── sample-data/                  # Auto-generated demo sample files
│   ├── alarms.csv
│   ├── tickets.csv
│   ├── network_kpi.xlsx
│   └── alarm.json
├── tests/                        # Pytest unit & integration test suite
├── Dockerfile                    # Production Docker container build
├── docker-compose.yml            # NOC Service + MinIO + Kafka + Postgres orchestration
├── requirements.txt              # Production dependencies
├── README.md                     # Technical documentation
└── .env.example                  # Environment configuration template
```

---

## 🛠️ Configuration (.env)

| Environment Variable | Default Value | Description |
| :--- | :--- | :--- |
| `DEMO_MODE` | `true` | Enable built-in Demo Data Generator & Mock APIs |
| `DEMO_INTERVAL_KAFKA_SECONDS` | `5` | Kafka event generation frequency |
| `DEMO_INTERVAL_FILE_SECONDS` | `60` | Sample files regeneration frequency |
| `APP_NAME` | `noc-ingestion` | Application Name |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO Server Host and Port |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO Root Access Key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO Root Secret Key |
| `MINIO_BUCKET` | `noc-raw-data` | Target MinIO Bucket |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka Broker Endpoint |
| `KAFKA_TOPIC` | `telecom-events` | Target Telecom Kafka Topic |

---

## ⚡ Running Locally

### 1. Setup Environment
```bash
git clone <repo-url>
cd noc-ingestion

# Create virtual environment
python -m venv venv

# Activate environment (Windows)
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Application
```bash
uvicorn app.main:app --reload --port 8000
```
Interactive OpenAPI Swagger docs will be available at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## 🛰️ API Endpoints Summary

### Demo REST APIs (`DEMO_MODE=true`)
- `GET /demo/alarms`: Returns 50-100 Comarch OSS alarm records.
- `GET /demo/tickets`: Returns 50-100 ServiceNow ticket records.
- `GET /demo/network-health`: Returns 50-100 network KPI health metrics across cell towers.
- `GET /demo/security-events`: Returns 50-100 Trend Micro & CyberArk threat logs.

### Health & Operations
- `GET /health`: Component health status (MinIO, Kafka, Demo Mode).
- `GET /metrics`: Ingestion volume counters and system throughput metrics.

### Ingestion Pipeline Endpoints
- `POST /upload`: Multipart file upload (`.csv`, `.xlsx`, `.xls`, `.json`, `.pdf`).
- `POST /simulate/kafka`: Triggers ingestion simulation of Comarch OSS, Alarms, and Tickets.
- `POST /simulate/rest`: Connects to external REST APIs, handles pagination/retries, and stores JSON in MinIO.
