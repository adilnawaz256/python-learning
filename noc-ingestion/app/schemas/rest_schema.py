from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class TargetSystem(str, Enum):
    SERVICENOW = "ServiceNow"
    TREND_MICRO = "Trend Micro"
    CYBERARK = "CyberArk"
    GENERIC = "Generic REST"


class RESTCollectorRequest(BaseModel):
    """Schema for requesting a REST API collection cycle."""
    target_system: TargetSystem = Field(..., description="Target system to poll")
    url: str = Field(..., description="Target REST API URL")
    auth_token: Optional[str] = Field(None, description="Optional Bearer/API token for auth")
    headers: Optional[Dict[str, str]] = Field(default_factory=dict, description="Additional HTTP headers")
    query_params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query string parameters")
    max_pages: int = Field(default=5, ge=1, le=50, description="Max pagination pages to fetch")
    page_size: int = Field(default=100, ge=1, le=1000, description="Results per page")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target_system": "ServiceNow",
                "url": "https://servicenow.telecom.local/api/now/table/incident",
                "auth_token": "secret-bearer-token",
                "query_params": {"sysparm_query": "active=true^priority=1"},
                "max_pages": 3,
                "page_size": 50
            }
        }
    )


class RESTCollectorResult(BaseModel):
    """Result of REST collection operation."""
    target_system: TargetSystem
    url: str
    pages_fetched: int
    records_fetched: int
    minio_path: str
    status: str
    execution_time_seconds: float
