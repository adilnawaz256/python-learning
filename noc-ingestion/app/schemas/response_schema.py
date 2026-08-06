from typing import Generic, TypeVar, Optional, Any, Dict
from pydantic import BaseModel, Field

T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    """Standardized API response wrapper."""
    success: bool = True
    message: str = "Operation completed successfully"
    data: Optional[T] = None
    error: Optional[Dict[str, Any]] = None


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str = "1.0.0"
    components: Dict[str, str] = Field(..., description="Status of MinIO, Kafka, Postgres")


class MetricsResponse(BaseModel):
    uptime_seconds: float
    total_kafka_events_processed: int
    total_rest_collections: int
    total_files_uploaded: int
    minio_storage_used_bytes: int
