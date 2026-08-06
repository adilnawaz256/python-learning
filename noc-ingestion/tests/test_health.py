import pytest


@pytest.mark.asyncio
async def test_health_endpoint(async_client):
    """Test GET /health returns 200 OK and expected structure."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "data" in data
    assert data["data"]["status"] in ["healthy", "degraded"]
    assert "components" in data["data"]


@pytest.mark.asyncio
async def test_metrics_endpoint(async_client):
    """Test GET /metrics returns 200 OK with metrics counters."""
    response = await async_client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "uptime_seconds" in data["data"]
    assert "total_kafka_events_processed" in data["data"]
    assert "total_files_uploaded" in data["data"]
