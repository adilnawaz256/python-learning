from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, JSON, Float, Text
from app.database.session import Base


class IngestionAuditLog(Base):
    """Database model for tracking all ingestion activity (Kafka, REST, File Uploads)."""
    __tablename__ = "ingestion_audit_logs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    source_type = Column(String(50), nullable=False, index=True)  # kafka, rest, csv, excel, pdf, json
    source_name = Column(String(100), nullable=False)  # Comarch OSS, ServiceNow, UploadedFileName.csv
    status = Column(String(50), nullable=False)  # SUCCESS, FAILED, RETRIED
    minio_path = Column(String(500), nullable=True)
    payload_size_bytes = Column(Integer, default=0)
    records_count = Column(Integer, default=0)
    processing_time_ms = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
