import pytest
import httpx
from app.services.rest.collector import RESTCollectorService
from app.schemas.rest_schema import RESTCollectorRequest, TargetSystem


class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code
        self.text = "OK"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("Error", request=None, response=self)

    def json(self):
        return self._json_data


@pytest.mark.asyncio
async def test_rest_collector_simulation(async_client, mock_minio_service, monkeypatch):
    """Test POST /simulate/rest endpoint."""
    async def mock_get(*args, **kwargs):
        return DummyResponse({"items": [{"id": 1, "status": "ACTIVE"}]})

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    payload = {
        "target_system": "ServiceNow",
        "url": "http://mock-api/tickets",
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
async def test_rest_collector_service_unit(mock_minio_service, monkeypatch):
    """Unit test for RESTCollectorService with mock httpx requests."""
    async def mock_get(*args, **kwargs):
        return DummyResponse({"items": [{"event_id": "SEC-101", "threat": "HIGH"}]})

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)

    collector = RESTCollectorService(minio_service=mock_minio_service)

    req = RESTCollectorRequest(
        target_system=TargetSystem.TREND_MICRO,
        url="http://mock-api/security",
        max_pages=1,
        page_size=5
    )

    result = await collector.collect_and_store(req)
    assert result.status == "SUCCESS"
    assert result.pages_fetched == 1
    assert "raw/rest/" in result.minio_path
