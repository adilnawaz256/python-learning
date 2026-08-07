import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database.session import get_db_session
from app.services.storage.minio.client import MinIOService


async def mock_get_db_session():
    """Mock database session dependency yielding None when PostgreSQL is offline in test mode."""
    yield None


app.dependency_overrides[get_db_session] = mock_get_db_session


@pytest.fixture
def mock_minio_service(monkeypatch):
    """Fixture providing a MinIO service operating in mock mode for test isolation."""
    service = MinIOService()
    service.client = None  # Force mock mode
    return service


@pytest_asyncio.fixture
async def async_client():
    """Async HTTP TestClient fixture for FastAPI endpoints."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
