import time
from fastapi import APIRouter, Depends
from app.api.deps import get_minio, get_kafka_consumer
from app.config.config import get_settings
from app.schemas.response_schema import APIResponse, HealthResponse, MetricsResponse
from app.services.storage.minio.client import MinIOService
from app.services.kafka.consumer import KafkaConsumerService

router = APIRouter(tags=["Health & Metrics"])

START_TIME = time.time()
uploaded_files_counter = 0
rest_collections_counter = 0


def increment_file_counter():
    global uploaded_files_counter
    uploaded_files_counter += 1


def increment_rest_counter():
    global rest_collections_counter
    rest_collections_counter += 1


@router.get("/health", response_model=APIResponse[HealthResponse])
async def health_check(
    minio: MinIOService = Depends(get_minio),
    kafka: KafkaConsumerService = Depends(get_kafka_consumer),
    settings = Depends(get_settings),
):
    """GET /health - Verifies status of core application dependencies."""
    minio_status = minio.check_health()
    kafka_status = "connected" if kafka._is_running else "ready (idle)"

    is_healthy = minio_status != "unhealthy"
    status_str = "healthy" if is_healthy else "degraded"

    data = HealthResponse(
        status=status_str,
        version="1.0.0",
        components={
            "minio": minio_status,
            "kafka": kafka_status,
            "api": "healthy",
            "demo_mode": "enabled" if settings.DEMO_MODE else "disabled",
        },
    )
    return APIResponse(success=is_healthy, message=f"Service status is {status_str}", data=data)


@router.get("/metrics", response_model=APIResponse[MetricsResponse])
async def get_metrics(
    kafka: KafkaConsumerService = Depends(get_kafka_consumer),
):
    """GET /metrics - System performance and throughput metrics."""
    uptime = round(time.time() - START_TIME, 2)
    data = MetricsResponse(
        uptime_seconds=uptime,
        total_kafka_events_processed=kafka.processed_count,
        total_rest_collections=rest_collections_counter,
        total_files_uploaded=uploaded_files_counter,
        minio_storage_used_bytes=0,
    )
    return APIResponse(success=True, message="Metrics retrieved successfully", data=data)
