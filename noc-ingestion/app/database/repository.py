from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from app.models.audit import IngestionAuditLog


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
