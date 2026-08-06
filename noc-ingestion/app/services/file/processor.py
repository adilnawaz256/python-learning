import io
import json
import pandas as pd
import pypdf
from typing import Tuple, Dict, Any, Optional
from app.core.exceptions import FileValidationError
from app.core.logger import log_event, logger
from app.schemas.file_schema import FileMetadata, SupportedFileType
from app.services.storage.minio.client import MinIOService, get_minio_service
from app.utils.validators import validate_file_extension, calculate_sha256, CONTENT_TYPES


class FileProcessorService:
    """Service responsible for validating files, extracting metadata, and storing in MinIO."""

    def __init__(self, minio_service: Optional[MinIOService] = None):
        self.minio_service = minio_service or get_minio_service()

    def inspect_file(self, content: bytes, file_type: SupportedFileType) -> Tuple[Optional[int], Optional[int], Dict[str, Any]]:
        """Parses file contents to extract row count, page count, or internal schema metrics."""
        row_count: Optional[int] = None
        page_count: Optional[int] = None
        extra_details: Dict[str, Any] = {}

        try:
            if file_type == SupportedFileType.CSV:
                df = pd.read_csv(io.BytesIO(content))
                row_count = len(df)
                extra_details["columns"] = list(df.columns)

            elif file_type == SupportedFileType.EXCEL:
                excel_file = pd.ExcelFile(io.BytesIO(content))
                extra_details["sheet_names"] = excel_file.sheet_names
                df = pd.read_excel(excel_file, sheet_name=0)
                row_count = len(df)
                extra_details["columns"] = list(df.columns)

            elif file_type == SupportedFileType.JSON:
                parsed_json = json.loads(content.decode("utf-8"))
                if isinstance(parsed_json, list):
                    row_count = len(parsed_json)
                    extra_details["structure"] = "array"
                elif isinstance(parsed_json, dict):
                    row_count = 1
                    extra_details["structure"] = "object"
                    extra_details["keys"] = list(parsed_json.keys())

            elif file_type == SupportedFileType.PDF:
                pdf_reader = pypdf.PdfReader(io.BytesIO(content))
                page_count = len(pdf_reader.pages)
                extra_details["encrypted"] = pdf_reader.is_encrypted

        except Exception as e:
            logger.warning(f"Metadata extraction warning for file_type {file_type.value}: {e}")
            raise FileValidationError(f"File validation failed for format '{file_type.value}': {str(e)}")

        return row_count, page_count, extra_details

    async def process_and_upload(self, content: bytes, filename: str) -> FileMetadata:
        """Validates file, generates metadata, and stores into MinIO under raw/{type}/"""
        ext, file_type = validate_file_extension(filename)
        file_size = len(content)

        if file_size == 0:
            raise FileValidationError(f"File '{filename}' is empty (0 bytes).")

        # Extract internal metrics (rows, pages, etc.)
        row_count, page_count, extra_details = self.inspect_file(content, file_type)
        sha256_hash = calculate_sha256(content)

        content_type = CONTENT_TYPES.get(file_type, "application/octet-stream")

        # Category for MinIO object path
        category = file_type.value

        # Async upload to MinIO
        minio_path = await self.minio_service.async_upload_bytes(
            data=content,
            category=category,
            filename=filename,
            content_type=content_type,
            metadata={
                "original_filename": filename,
                "sha256": sha256_hash,
                "file_type": file_type.value,
            },
        )

        metadata = FileMetadata(
            original_filename=filename,
            file_type=file_type,
            file_size_bytes=file_size,
            sha256_hash=sha256_hash,
            minio_path=minio_path,
            row_count=row_count,
            page_count=page_count,
            extra_details=extra_details,
        )

        log_event(
            event_type="Upload Completed",
            status="SUCCESS",
            details={
                "filename": filename,
                "file_type": file_type.value,
                "size_bytes": file_size,
                "minio_path": minio_path,
            },
        )

        return metadata
