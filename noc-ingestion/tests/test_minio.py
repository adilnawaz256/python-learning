import pytest
from app.services.storage.minio.client import MinIOService


def test_minio_path_generation():
    """Test MinIO object path structuring raw/{category}/{YYYY}/{MM}/{DD}/{timestamp}_{filename}."""
    service = MinIOService()
    path = service.generate_object_path("csv", "report.csv")
    assert path.startswith("raw/csv/")
    assert "report.csv" in path


@pytest.mark.asyncio
async def test_minio_mock_upload_bytes(mock_minio_service):
    """Test bytes upload in mock mode."""
    data = b"sample network data"
    path = await mock_minio_service.async_upload_bytes(
        data=data,
        category="json",
        filename="event.json",
        content_type="application/json"
    )
    assert "raw/json/" in path
