from typing import List, Dict, Any, Optional
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_file_processor, verify_api_token
from app.config.config import get_settings
from app.database.session import get_db_session
from app.database.repository import UploadHistoryRepository
from app.core.exceptions import FileValidationError
from app.schemas.file_schema import FileMetadata, SupportedFileType
from app.schemas.response_schema import APIResponse
from app.services.file.processor import FileProcessorService
from app.services.kafka.producer import KafkaProducerService

router = APIRouter(prefix="/api/v1", tags=["File Upload & History APIs"])
settings = get_settings()
_producer = KafkaProducerService()


async def handle_file_upload(
    file: UploadFile,
    expected_type: SupportedFileType,
    processor: FileProcessorService,
    session: Optional[AsyncSession],
) -> Dict[str, Any]:
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required for file upload.",
        )

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Uploaded file '{file.filename}' is empty.",
        )

    try:
        metadata = await processor.process_and_upload(content=content, filename=file.filename)
        parsed_records = processor.parse_file_to_json_records(content, expected_type)
        records_count = len(parsed_records)

        # Publish parsed records to Kafka if present
        topic = settings.KAFKA_TOPIC_NETWORK
        if parsed_records and expected_type in [SupportedFileType.CSV, SupportedFileType.EXCEL, SupportedFileType.JSON]:
            await _producer.start()
            for rec in parsed_records[:500]:  # Cap batch to prevent overflow
                event = {
                    "event_id": f"UPLOAD-{metadata.sha256_hash[:8]}",
                    "source": f"UPLOAD_{expected_type.value.upper()}",
                    "timestamp": metadata.uploaded_at.isoformat(),
                    "severity": "INFO",
                    "filename": file.filename,
                    "payload": rec,
                }
                await _producer.publish(topic, event)

        # Record upload in PostgreSQL history table
        repo = UploadHistoryRepository(session)
        history_entry = await repo.create_upload(
            filename=file.filename,
            file_type=expected_type.value,
            file_size_bytes=len(content),
            minio_path=metadata.minio_path,
            kafka_topic=topic if parsed_records else None,
            records_count=records_count,
            status="SUCCESS",
            metadata_json=metadata.model_dump(mode="json"),
        )

        return {
            "upload_id": history_entry.id if history_entry else None,
            "filename": file.filename,
            "file_type": expected_type.value,
            "file_size_bytes": len(content),
            "minio_path": metadata.minio_path,
            "records_parsed": records_count,
            "kafka_published": bool(parsed_records),
            "kafka_topic": topic if parsed_records else None,
        }
    except FileValidationError as fve:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=fve.message)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload processing failed: {str(e)}",
        )


@router.post("/upload/csv", response_model=APIResponse[Dict[str, Any]])
async def upload_csv(
    file: UploadFile = File(...),
    processor: FileProcessorService = Depends(get_file_processor),
    session: AsyncSession = Depends(get_db_session),
    _token: str = Depends(verify_api_token),
):
    """POST /api/v1/upload/csv - Upload, parse CSV, store original in MinIO, track in Postgres, and publish records to Kafka."""
    res = await handle_file_upload(file, SupportedFileType.CSV, processor, session)
    return APIResponse(success=True, message=f"CSV file '{file.filename}' uploaded and processed successfully.", data=res)


@router.post("/upload/excel", response_model=APIResponse[Dict[str, Any]])
async def upload_excel(
    file: UploadFile = File(...),
    processor: FileProcessorService = Depends(get_file_processor),
    session: AsyncSession = Depends(get_db_session),
    _token: str = Depends(verify_api_token),
):
    """POST /api/v1/upload/excel - Upload, parse Excel, store in MinIO, track in Postgres, publish to Kafka."""
    res = await handle_file_upload(file, SupportedFileType.EXCEL, processor, session)
    return APIResponse(success=True, message=f"Excel file '{file.filename}' uploaded and processed successfully.", data=res)


@router.post("/upload/json", response_model=APIResponse[Dict[str, Any]])
async def upload_json(
    file: UploadFile = File(...),
    processor: FileProcessorService = Depends(get_file_processor),
    session: AsyncSession = Depends(get_db_session),
    _token: str = Depends(verify_api_token),
):
    """POST /api/v1/upload/json - Upload JSON, store in MinIO, track in Postgres, publish to Kafka."""
    res = await handle_file_upload(file, SupportedFileType.JSON, processor, session)
    return APIResponse(success=True, message=f"JSON file '{file.filename}' uploaded and processed successfully.", data=res)


@router.post("/upload/pdf", response_model=APIResponse[Dict[str, Any]])
async def upload_pdf(
    file: UploadFile = File(...),
    processor: FileProcessorService = Depends(get_file_processor),
    session: AsyncSession = Depends(get_db_session),
    _token: str = Depends(verify_api_token),
):
    """POST /api/v1/upload/pdf - Upload PDF document, store in MinIO, track metadata in Postgres."""
    res = await handle_file_upload(file, SupportedFileType.PDF, processor, session)
    return APIResponse(success=True, message=f"PDF file '{file.filename}' uploaded and stored successfully.", data=res)


@router.get("/uploads", response_model=APIResponse[List[Dict[str, Any]]])
async def get_upload_history(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db_session),
):
    """GET /api/v1/uploads - Retrieve file upload history from PostgreSQL."""
    repo = UploadHistoryRepository(session)
    entries = await repo.get_uploads(limit=limit, offset=offset)
    data = [
        {
            "id": item.id,
            "filename": item.filename,
            "file_type": item.file_type,
            "file_size_bytes": item.file_size_bytes,
            "minio_path": item.minio_path,
            "kafka_topic": item.kafka_topic,
            "records_count": item.records_count,
            "status": item.status,
            "created_at": item.created_at.isoformat() if item.created_at else None,
        }
        for item in entries
    ]
    return APIResponse(success=True, message=f"Retrieved {len(data)} upload history records.", data=data)


@router.get("/uploads/{upload_id}", response_model=APIResponse[Dict[str, Any]])
async def get_upload_by_id(
    upload_id: int,
    session: AsyncSession = Depends(get_db_session),
):
    """GET /api/v1/uploads/{id} - Retrieve single upload history entry details by ID."""
    repo = UploadHistoryRepository(session)
    item = await repo.get_upload_by_id(upload_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Upload with ID {upload_id} not found.")

    data = {
        "id": item.id,
        "filename": item.filename,
        "file_type": item.file_type,
        "file_size_bytes": item.file_size_bytes,
        "minio_path": item.minio_path,
        "kafka_topic": item.kafka_topic,
        "records_count": item.records_count,
        "status": item.status,
        "error_message": item.error_message,
        "metadata": item.metadata_json,
        "created_at": item.created_at.isoformat() if item.created_at else None,
    }
    return APIResponse(success=True, message=f"Retrieved upload record {upload_id}.", data=data)


@router.delete("/uploads/{upload_id}", response_model=APIResponse[Dict[str, Any]])
async def delete_upload(
    upload_id: int,
    session: AsyncSession = Depends(get_db_session),
    _token: str = Depends(verify_api_token),
):
    """DELETE /api/v1/uploads/{id} - Remove upload record from PostgreSQL history."""
    repo = UploadHistoryRepository(session)
    item = await repo.get_upload_by_id(upload_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Upload with ID {upload_id} not found.")

    success = await repo.delete_upload(upload_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete upload record.")

    return APIResponse(success=True, message=f"Upload record {upload_id} deleted successfully.", data={"id": upload_id})
