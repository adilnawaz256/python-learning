import random
from typing import List, Dict, Any
from fastapi import APIRouter, Query
from app.demo.rest_generator import DemoDataGenerator
from app.schemas.response_schema import APIResponse

router = APIRouter(prefix="/demo", tags=["Demo REST APIs"])


@router.get("/alarms", response_model=APIResponse[List[Dict[str, Any]]])
async def get_demo_alarms(
    count: int = Query(default=75, ge=10, le=200, description="Number of demo alarm records (default 75)")
):
    """GET /demo/alarms - Returns 50-100 realistic Comarch OSS alarm records."""
    records = DemoDataGenerator.generate_alarms(count=count)
    return APIResponse(
        success=True,
        message=f"Generated {len(records)} demo Comarch OSS alarm records.",
        data=records
    )


@router.get("/tickets", response_model=APIResponse[List[Dict[str, Any]]])
async def get_demo_tickets(
    count: int = Query(default=60, ge=10, le=200, description="Number of demo ITSM ticket records (default 60)")
):
    """GET /demo/tickets - Returns 50-100 realistic ServiceNow incident tickets."""
    records = DemoDataGenerator.generate_tickets(count=count)
    return APIResponse(
        success=True,
        message=f"Generated {len(records)} demo ServiceNow ticket records.",
        data=records
    )


@router.get("/network-health", response_model=APIResponse[List[Dict[str, Any]]])
async def get_demo_network_health(
    count: int = Query(default=80, ge=10, le=200, description="Number of demo network KPI records (default 80)")
):
    """GET /demo/network-health - Returns 50-100 network performance KPI records across cell towers."""
    records = DemoDataGenerator.generate_network_health(count=count)
    return APIResponse(
        success=True,
        message=f"Generated {len(records)} demo network health KPI records.",
        data=records
    )


@router.get("/security-events", response_model=APIResponse[List[Dict[str, Any]]])
async def get_demo_security_events(
    count: int = Query(default=50, ge=10, le=200, description="Number of demo security threat records (default 50)")
):
    """GET /demo/security-events - Returns 50-100 Trend Micro & CyberArk threat logs."""
    records = DemoDataGenerator.generate_security_events(count=count)
    return APIResponse(
        success=True,
        message=f"Generated {len(records)} demo security threat records.",
        data=records
    )
