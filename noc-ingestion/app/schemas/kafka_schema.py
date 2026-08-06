from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class EventSource(str, Enum):
    COMARCH_OSS = "Comarch OSS"
    ALARM_EVENTS = "Alarm Events"
    TICKET_EVENTS = "Ticket Events"


class AlarmSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    WARNING = "WARNING"
    INFO = "INFO"


class KafkaIngestionMessage(BaseModel):
    """Unified Pydantic schema for Telecom NOC Kafka events."""
    event_id: str = Field(..., description="Unique event identifier (UUID or OSS event ID)")
    source: EventSource = Field(..., description="Origin Telecom OSS system")
    event_type: str = Field(..., description="Category of event, e.g. LinkDown, BGPFlap, TicketCreated")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of event occurrence")
    severity: AlarmSeverity = Field(default=AlarmSeverity.INFO, description="Event severity rating")
    node_id: str = Field(..., description="Network Element ID / Tower / Router ID")
    region: Optional[str] = Field(None, description="Geographic region / datacenter zone")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Raw event details & attributes")

    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "evt-88392-oss",
                "source": "Comarch OSS",
                "event_type": "LinkDown",
                "timestamp": "2026-08-06T12:00:00Z",
                "severity": "CRITICAL",
                "node_id": "ROUTER-MUM-CORE-01",
                "region": "AP-SOUTH-1",
                "payload": {
                    "interface": "GigabitEthernet0/0/1",
                    "affected_customers": 450,
                    "error_code": "OPTICAL_POWER_LOW"
                }
            }
        }
