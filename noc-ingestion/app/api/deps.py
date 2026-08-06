from fastapi import Header, HTTPException, status, Depends

from app.config.config import Settings, get_settings
from app.services.storage.minio.client import MinIOService, get_minio_service
from app.services.file.processor import FileProcessorService
from app.services.rest.collector import RESTCollectorService
from app.services.kafka.consumer import KafkaConsumerService
from app.services.kafka.simulator import KafkaSimulatorService
from app.demo.scheduler import DemoScheduler


# Global shared instances
_global_kafka_consumer: KafkaConsumerService = KafkaConsumerService()
_global_demo_scheduler: DemoScheduler = DemoScheduler(consumer_service=_global_kafka_consumer)


def get_minio() -> MinIOService:
    return get_minio_service()


def get_file_processor(minio_service: MinIOService = Depends(get_minio)) -> FileProcessorService:
    return FileProcessorService(minio_service=minio_service)


def get_rest_collector(minio_service: MinIOService = Depends(get_minio)) -> RESTCollectorService:
    return RESTCollectorService(minio_service=minio_service)


def get_kafka_consumer() -> KafkaConsumerService:
    return _global_kafka_consumer


def get_kafka_simulator(consumer: KafkaConsumerService = Depends(get_kafka_consumer)) -> KafkaSimulatorService:
    return KafkaSimulatorService(consumer_service=consumer)


def get_demo_scheduler() -> DemoScheduler:
    return _global_demo_scheduler


def verify_api_token(
    x_api_token: str = Header(None, alias="X-API-Token"),
    settings: Settings = Depends(get_settings),
) -> str:
    """Optional token authentication validator for REST APIs."""
    if x_api_token and x_api_token != settings.API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-API-Token provided.",
        )
    return x_api_token or ""
