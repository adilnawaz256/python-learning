import io
import pytest
import pandas as pd
import pypdf
from app.services.file.processor import FileProcessorService
from app.schemas.file_schema import SupportedFileType
from app.core.exceptions import FileValidationError


@pytest.mark.asyncio
async def test_csv_upload(async_client, mock_minio_service):
    """Test CSV upload, parsing, row count extraction, and response schema."""
    csv_content = b"node_id,status,latency_ms\nROUTER-01,UP,12.4\nROUTER-02,DOWN,0.0\n"
    files = {"file": ("network_status.csv", csv_content, "text/csv")}

    response = await async_client.post("/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["original_filename"] == "network_status.csv"
    assert data["data"]["file_type"] == "csv"
    assert data["data"]["row_count"] == 2
    assert "raw/csv/" in data["data"]["minio_path"]


@pytest.mark.asyncio
async def test_json_upload(async_client, mock_minio_service):
    """Test JSON upload validation and ingestion."""
    json_content = b'[{"alarm_id": "A101", "severity": "CRITICAL"}, {"alarm_id": "A102", "severity": "MAJOR"}]'
    files = {"file": ("alarms.json", json_content, "application/json")}

    response = await async_client.post("/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["file_type"] == "json"
    assert data["data"]["row_count"] == 2


@pytest.mark.asyncio
async def test_excel_upload(async_client, mock_minio_service):
    """Test Excel upload (.xlsx) processing."""
    df = pd.DataFrame({"TicketID": ["INC001", "INC002"], "Priority": [1, 2]})
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    excel_bytes = buffer.getvalue()

    files = {"file": ("tickets.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}

    response = await async_client.post("/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["file_type"] == "excel"
    assert data["data"]["row_count"] == 2


@pytest.mark.asyncio
async def test_pdf_upload(async_client, mock_minio_service):
    """Test PDF upload and page count extraction."""
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=100, height=100)
    buffer = io.BytesIO()
    writer.write(buffer)
    pdf_bytes = buffer.getvalue()

    files = {"file": ("report.pdf", pdf_bytes, "application/pdf")}

    response = await async_client.post("/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["file_type"] == "pdf"
    assert data["data"]["page_count"] == 1


@pytest.mark.asyncio
async def test_unsupported_file_upload(async_client):
    """Test uploading invalid file extension returns 422 Unprocessable Entity."""
    files = {"file": ("script.sh", b"echo 'hello'", "text/plain")}

    response = await async_client.post("/upload", files=files)
    assert response.status_code == 422
