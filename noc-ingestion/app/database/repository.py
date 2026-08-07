from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, delete
from app.models.audit import IngestionAuditLog
from app.models.upload import UploadHistory


class IngestionAuditRepository:
    """Repository pattern implementation for Ingestion Audit Logs."""

    def __init__(self, session: Optional[AsyncSession] = None):
        self.session = session

    async def log_ingestion(
        self,
        source_type: str,
        source_name: str,
        status: str,
        minio_path: Optional[str] = None,
        payload_size_bytes: int = 0,
        records_count: int = 0,
        processing_time_ms: float = 0.0,
        error_message: Optional[str] = None,
        metadata_json: Optional[Dict[str, Any]] = None,
    ) -> Optional[IngestionAuditLog]:
        """Creates an audit log record."""
        if not self.session:
            return None

        log_entry = IngestionAuditLog(
            source_type=source_type,
            source_name=source_name,
            status=status,
            minio_path=minio_path,
            payload_size_bytes=payload_size_bytes,
            records_count=records_count,
            processing_time_ms=processing_time_ms,
            error_message=error_message,
            metadata_json=metadata_json,
        )
        self.session.add(log_entry)
        try:
            await self.session.commit()
            await self.session.refresh(log_entry)
            return log_entry
        except Exception:
            await self.session.rollback()
            return None

    async def get_recent_logs(self, limit: int = 50) -> List[IngestionAuditLog]:
        """Fetches recent audit log records."""
        if not self.session:
            return []
        query = select(IngestionAuditLog).order_by(IngestionAuditLog.created_at.desc()).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_summary_counts(self) -> Dict[str, int]:
        """Calculates metrics across ingestion categories."""
        if not self.session:
            return {"kafka": 0, "rest": 0, "file": 0}
        
        query = select(
            IngestionAuditLog.source_type,
            func.count(IngestionAuditLog.id)
        ).group_by(IngestionAuditLog.source_type)
        
        result = await self.session.execute(query)
        counts = {row[0]: row[1] for row in result.all()}
        return counts


class UploadHistoryRepository:
    """Repository pattern implementation for Upload History."""

    def __init__(self, session: Optional[AsyncSession] = None):
        self.session = session

    async def create_upload(
        self,
        filename: str,
        file_type: str,
        file_size_bytes: int,
        minio_path: str,
        kafka_topic: Optional[str] = None,
        records_count: int = 0,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        metadata_json: Optional[Dict[str, Any]] = None,
    ) -> Optional[UploadHistory]:
        """Creates an upload history entry."""
        if not self.session:
            return UploadHistory(
                id=1,
                filename=filename,
                file_type=file_type,
                file_size_bytes=file_size_bytes,
                minio_path=minio_path,
                kafka_topic=kafka_topic,
                records_count=records_count,
                status=status,
                error_message=error_message,
                metadata_json=metadata_json,
            )

        entry = UploadHistory(
            filename=filename,
            file_type=file_type,
            file_size_bytes=file_size_bytes,
            minio_path=minio_path,
            kafka_topic=kafka_topic,
            records_count=records_count,
            status=status,
            error_message=error_message,
            metadata_json=metadata_json,
        )
        self.session.add(entry)
        try:
            await self.session.commit()
            await self.session.refresh(entry)
            return entry
        except Exception:
            await self.session.rollback()
            return None

    async def get_uploads(self, limit: int = 50, offset: int = 0) -> List[UploadHistory]:
        """Fetches list of upload history records."""
        if not self.session:
            return []
        query = select(UploadHistory).order_by(UploadHistory.created_at.desc()).offset(offset).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_upload_by_id(self, upload_id: int) -> Optional[UploadHistory]:
        """Fetches single upload history record by ID."""
        if not self.session:
            return None
        query = select(UploadHistory).where(UploadHistory.id == upload_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def delete_upload(self, upload_id: int) -> bool:
        """Deletes upload history record by ID."""
        if not self.session:
            return True
        stmt = delete(UploadHistory).where(UploadHistory.id == upload_id)
        try:
            await self.session.execute(stmt)
            await self.session.commit()
            return True
        except Exception:
            await self.session.rollback()
            return False

