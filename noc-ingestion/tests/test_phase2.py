import pytest
from fastapi.testclient import TestClient
from app.main import app
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mock-external-api")))
from generator import MockDataGenerator
from app.services.rest.connector import RESTConnectorScheduler
from app.services.kafka.consumer import KafkaConsumerService

client = TestClient(app)


def test_mock_data_generator():
    alarms = MockDataGenerator.generate_alarms(count=5)
    assert len(alarms) == 5
    assert "alarm_id" in alarms[0]

    tickets = MockDataGenerator.generate_tickets(count=5)
    assert len(tickets) == 5

    net_events = MockDataGenerator.generate_network_events(count=5)
    assert len(net_events) == 5

    sec_events = MockDataGenerator.generate_security_events(count=5)
    assert len(sec_events) == 5

    perf = MockDataGenerator.generate_performance(count=5)
    assert len(perf) == 5

    sites = MockDataGenerator.generate_sites(count=5)
    assert len(sites) == 5

    devices = MockDataGenerator.generate_devices(count=5)
    assert len(devices) == 5


def test_rest_connector_normalization():
    raw_alarm = {
        "alarm_id": "ALM-TEST-123",
        "severity": "CRITICAL",
        "tower_id": "TOWER-DEL-101",
        "region": "IN-NORTH-DELHI",
        "vendor": "Huawei",
    }
    normalized = RESTConnectorScheduler.normalize_record(raw_alarm, "alarm")
    assert normalized["event_id"] == "ALM-TEST-123"
    assert normalized["severity"] == "CRITICAL"
    assert normalized["node_id"] == "TOWER-DEL-101"
    assert normalized["source"] == "REST_ALARM"


def test_kafka_category_resolution():
    svc = KafkaConsumerService()
    assert svc.determine_category("telecom-alarms", {}) == "kafka/alarms"
    assert svc.determine_category("tickets", {}) == "kafka/tickets"
    assert svc.determine_category("network-events", {}) == "kafka/network"
    assert svc.determine_category("security-events", {}) == "kafka/security"
    assert svc.determine_category("performance", {}) == "kafka/performance"
    assert svc.determine_category("unknown", {"event_type": "CellTowerDown"}) == "kafka/network"


def test_dashboard_apis():
    resp = client.get("/dashboard/summary")
    assert resp.status_code == 200
    assert resp.json()["success"] is True

    resp = client.get("/dashboard/alarms")
    assert resp.status_code == 200
    assert "by_severity" in resp.json()["data"]

    resp = client.get("/dashboard/tickets")
    assert resp.status_code == 200

    resp = client.get("/dashboard/uploads")
    assert resp.status_code == 200

    resp = client.get("/dashboard/jobs")
    assert resp.status_code == 200

    resp = client.get("/dashboard/performance")
    assert resp.status_code == 200


def test_job_monitoring_apis():
    resp = client.get("/jobs")
    assert resp.status_code == 200
    assert len(resp.json()["data"]) >= 1

    resp = client.get("/processing/status")
    assert resp.status_code == 200
    assert resp.json()["data"]["overall_status"] == "RUNNING"


def test_file_upload_csv():
    csv_content = b"event_id,severity,vendor\nALM-1,CRITICAL,Huawei\nALM-2,MAJOR,Nokia\n"
    files = {"file": ("test_alarms.csv", csv_content, "text/csv")}
    resp = client.post("/api/v1/upload/csv", files=files)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["filename"] == "test_alarms.csv"
    assert data["records_parsed"] == 2


def test_upload_history_apis():
    resp = client.get("/api/v1/uploads")
    assert resp.status_code == 200
