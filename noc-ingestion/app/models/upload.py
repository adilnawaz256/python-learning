from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, JSON, Text
from app.database.session import Base


class UploadHistory(Base):
    """Database model for tracking uploaded files and parsed records history."""
    __tablename__ = "upload_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    file_type = Column(String(50), nullable=False)  # csv, excel, json, pdf
    file_size_bytes = Column(Integer, default=0)
    minio_path = Column(String(500), nullable=False)
    kafka_topic = Column(String(100), nullable=True)
    records_count = Column(Integer, default=0)
    status = Column(String(50), nullable=False, default="SUCCESS")  # PENDING, SUCCESS, FAILED
    error_message = Column(Text, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
