import io
import json
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional, Dict, Any, Union
import anyio
from minio import Minio
from minio.error import S3Error


from app.config.config import get_settings
from app.core.logger import log_event, logger
from app.core.exceptions import StorageError, MinIOConnectionError


class MinIOService:
    """Singleton MinIO Object Storage Client."""

    _instance: Optional["MinIOService"] = None

    def __new__(cls) -> "MinIOService":
        if cls._instance is None:
            cls._instance = super(MinIOService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return

        self.settings = get_settings()
        self.endpoint = self.settings.MINIO_ENDPOINT
        self.access_key = self.settings.MINIO_ACCESS_KEY
        self.secret_key = self.settings.MINIO_SECRET_KEY
        self.secure = self.settings.MINIO_SECURE
        self.default_bucket = self.settings.MINIO_BUCKET

        self.client: Optional[Minio] = None
        self._initialized = True

    def connect(self) -> None:
        """Synchronously initializes MinIO client and creates default bucket if missing."""
        try:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
            self.ensure_bucket_exists(self.default_bucket)
            log_event(
                event_type="MinIO Connected",
                status="SUCCESS",
                details={"endpoint": self.endpoint, "bucket": self.default_bucket},
            )
        except Exception as e:
            log_event(
                event_type="MinIO Connection Error",
                status="FAILED",
                details={"error": str(e), "endpoint": self.endpoint},
                level="ERROR",
            )
            # Create a mock/in-memory fallback state for testing if MinIO is not available
            self.client = None

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Creates bucket if it does not already exist."""
        if not self.client:
            return
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created MinIO bucket: {bucket_name}")
        except S3Error as err:
            logger.error(f"S3 Error during bucket check/create: {err}")
            raise StorageError(f"Failed to ensure bucket {bucket_name}: {err}")

    def generate_object_path(self, category: str, filename_or_prefix: str) -> str:
        """Generates structured key path: raw/{category}/{YYYY}/{MM}/{DD}/{timestamp}_{uuid}_{filename}"""
        now = datetime.now(timezone.utc)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")
        date_path = now.strftime("%Y/%m/%d")
        unique_id = uuid.uuid4().hex[:8]

        # Clean category and filename (strip slashes)
        category_clean = category.lower().strip().strip("/")
        filename_clean = filename_or_prefix.strip("/")

        return f"raw/{category_clean}/{date_path}/{timestamp_str}_{unique_id}_{filename_clean}"

    def upload_bytes(
        self,
        data: bytes,
        category: str,
        filename: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        bucket_name: Optional[str] = None,
    ) -> str:
        """Uploads raw bytes to MinIO and returns the object key path."""
        target_bucket = bucket_name or self.default_bucket
        object_name = self.generate_object_path(category, filename)

        if not self.client:
            # Fallback mock path for unit tests without active MinIO server
            logger.warning(f"MinIO client uninitialized. Mocking upload for {object_name}")
            log_event(
                event_type="MinIO Uploaded",
                status="MOCK_SUCCESS",
                details={"bucket": target_bucket, "object_name": object_name, "bytes": len(data)},
            )
            return f"mock://{target_bucket}/{object_name}"

        try:
            stream = io.BytesIO(data)
            data_length = len(data)

            self.client.put_object(
                bucket_name=target_bucket,
                object_name=object_name,
                data=stream,
                length=data_length,
                content_type=content_type,
                metadata=metadata or {},
            )

            log_event(
                event_type="MinIO Uploaded",
                status="SUCCESS",
                details={
                    "bucket": target_bucket,
                    "object_name": object_name,
                    "size_bytes": data_length,
                    "content_type": content_type,
                },
            )
            return object_name
        except Exception as e:
            log_event(
                event_type="MinIO Upload Error",
                status="FAILED",
                details={"error": str(e), "object_name": object_name},
                level="ERROR",
            )
            raise StorageError(f"Failed to upload object {object_name} to MinIO: {e}")

    async def async_upload_bytes(
        self,
        data: bytes,
        category: str,
        filename: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        bucket_name: Optional[str] = None,
    ) -> str:
        """Async non-blocking wrapper around upload_bytes."""
        return await anyio.to_thread.run_sync(
            self.upload_bytes, data, category, filename, content_type, metadata, bucket_name
        )

    def upload_json(
        self,
        data_dict: Union[Dict[str, Any], list],
        category: str,
        filename_prefix: str = "payload.json",
        bucket_name: Optional[str] = None,
    ) -> str:
        """Converts dict/list to JSON bytes and uploads to MinIO."""
        json_bytes = json.dumps(data_dict, indent=2, default=str).encode("utf-8")
        filename = filename_prefix if filename_prefix.endswith(".json") else f"{filename_prefix}.json"
        return self.upload_bytes(
            data=json_bytes,
            category=category,
            filename=filename,
            content_type="application/json",
            bucket_name=bucket_name,
        )

    async def async_upload_json(
        self,
        data_dict: Union[Dict[str, Any], list],
        category: str,
        filename_prefix: str = "payload.json",
        bucket_name: Optional[str] = None,
    ) -> str:
        """Async non-blocking wrapper around upload_json."""
        return await anyio.to_thread.run_sync(
            self.upload_json, data_dict, category, filename_prefix, bucket_name
        )

    def check_health(self) -> str:
        """Returns connection health status."""
        if not self.client:
            return "degraded (mock mode)"
        try:
            self.client.bucket_exists(self.default_bucket)
            return "healthy"
        except Exception:
            return "unhealthy"


# Global singleton helper getter
@lru_cache()
def get_minio_service() -> MinIOService:
    service = MinIOService()
    service.connect()
    return service
