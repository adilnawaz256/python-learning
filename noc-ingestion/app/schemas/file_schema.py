from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class SupportedFileType(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    PDF = "pdf"


class FileMetadata(BaseModel):
    """Metadata generated during file upload validation and processing."""
    original_filename: str
    file_type: SupportedFileType
    file_size_bytes: int
    sha256_hash: str
    minio_path: str
    uploaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    row_count: Optional[int] = Field(None, description="Number of rows for CSV/Excel")
    page_count: Optional[int] = Field(None, description="Number of pages for PDF")
    extra_details: Dict[str, Any] = Field(default_factory=dict)
