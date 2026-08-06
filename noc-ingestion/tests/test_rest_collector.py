import pytest
import httpx
from app.services.rest.collector import RESTCollectorService
from app.schemas.rest_schema import RESTCollectorRequest, TargetSystem
from app.core.exceptions import RESTCollectorError


@pytest.mark.asyncio
async def test_rest_collector_simulation(async_client, mock_minio_service):
    """Test POST /simulate/rest endpoint."""
    payload = {
        "target_system": "ServiceNow",
        "url": "https://httpbin.org/get",
        "auth_token": "demo-token",
        "max_pages": 1,
        "page_size": 10
    }

    response = await async_client.post("/simulate/rest", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["target_system"] == "ServiceNow"
    assert "raw/rest/" in data["data"]["minio_path"]


@pytest.mark.asyncio
async def test_rest_collector_service_unit(mock_minio_service):
    """Unit test for RESTCollectorService with mock httpx requests."""
    collector = RESTCollectorService(minio_service=mock_minio_service)

    req = RESTCollectorRequest(
        target_system=TargetSystem.TREND_MICRO,
        url="https://httpbin.org/json",
        max_pages=1,
        page_size=5
    )

    result = await collector.collect_and_store(req)
    assert result.status == "SUCCESS"
    assert result.pages_fetched == 1
    assert "raw/rest/" in result.minio_path
