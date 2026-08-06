import logging
import sys
import json
from datetime import datetime, timezone
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Custom JSON log formatter for structured Telecom NOC logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func_name": record.funcName,
            "line_no": record.lineno,
        }

        # Attach extra structured attributes passed via logging extra dict
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            log_data.update(record.extra_fields)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logger(name: str = "noc_ingestion", level: str = "INFO") -> logging.Logger:
    """Configures structured logger instance."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)

    logger.propagate = False
    return logger


# Global logger instance
logger = setup_logger()


def log_event(event_type: str, status: str, details: Dict[str, Any], level: str = "INFO") -> None:
    """Helper to record standardized structured events.

    Events:
    - Kafka Connected
    - Kafka Error
    - API Started
    - API Success
    - Upload Completed
    - MinIO Uploaded
    """
    log_payload = {
        "event_type": event_type,
        "status": status,
        **details
    }
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, f"[{event_type}] Status: {status}", extra={"extra_fields": log_payload})
