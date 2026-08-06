import pytest
from app.schemas.kafka_schema import KafkaIngestionMessage, EventSource, AlarmSeverity
from app.services.kafka.consumer import KafkaConsumerService
from app.services.kafka.simulator import KafkaSimulatorService
from app.core.exceptions import SchemaValidationError


@pytest.mark.asyncio
async def test_kafka_simulation_endpoint(async_client, mock_minio_service):
    """Test POST /simulate/kafka endpoint."""
    response = await async_client.post("/simulate/kafka?count=4")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert len(data["data"]) == 4
    for item in data["data"]:
        assert item["status"] == "INGESTED"
        assert "raw/kafka/" in item["minio_path"]


@pytest.mark.asyncio
async def test_kafka_consumer_message_validation(mock_minio_service):
    """Test KafkaConsumerService message parsing and validation."""
    consumer_svc = KafkaConsumerService(minio_service=mock_minio_service)

    valid_payload = {
        "event_id": "evt-9912",
        "source": "Comarch OSS",
        "event_type": "OpticalLinkLoss",
        "timestamp": "2026-08-06T12:00:00Z",
        "severity": "CRITICAL",
        "node_id": "TOWER-DELHI-001",
        "region": "IN-NORTH-1",
        "payload": {"dbm_level": -32.5}
    }

    minio_path = await consumer_svc.process_single_message(valid_payload)
    assert "raw/kafka/" in minio_path
    assert consumer_svc.processed_count == 1


@pytest.mark.asyncio
async def test_kafka_consumer_invalid_json(mock_minio_service):
    """Test invalid Kafka payload raises SchemaValidationError."""
    consumer_svc = KafkaConsumerService(minio_service=mock_minio_service)

    invalid_payload = {
        "event_id": "evt-9912",
        # missing required node_id and source fields
    }

    with pytest.raises(SchemaValidationError):
        await consumer_svc.process_single_message(invalid_payload)
