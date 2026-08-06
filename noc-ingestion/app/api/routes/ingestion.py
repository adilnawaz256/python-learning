from typing import List, Dict, Any
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from app.api.deps import (
    get_file_processor,
    get_rest_collector,
    get_kafka_simulator,
    verify_api_token,
)
from app.api.routes.health import increment_file_counter, increment_rest_counter
from app.core.exceptions import FileValidationError, RESTCollectorError, SchemaValidationError
from app.schemas.file_schema import FileMetadata
from app.schemas.rest_schema import RESTCollectorRequest, RESTCollectorResult
from app.schemas.response_schema import APIResponse
from app.services.file.processor import FileProcessorService
from app.services.rest.collector import RESTCollectorService
from app.services.kafka.simulator import KafkaSimulatorService

router = APIRouter(tags=["Ingestion APIs"])


@router.post("/upload", response_model=APIResponse[FileMetadata])
async def upload_file(
    file: UploadFile = File(...),
    processor: FileProcessorService = Depends(get_file_processor),
    _token: str = Depends(verify_api_token),
):
    """POST /upload - Accepts CSV, Excel (.xlsx/.xls), JSON, or PDF file uploads."""
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required for file upload.",
        )

    try:
        content = await file.read()
        metadata = await processor.process_and_upload(content=content, filename=file.filename)
        increment_file_counter()
        return APIResponse(
            success=True,
            message=f"File '{file.filename}' processed and stored into MinIO successfully.",
            data=metadata,
        )
    except FileValidationError as fve:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=fve.message)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during file ingestion: {str(e)}",
        )


@router.post("/simulate/kafka", response_model=APIResponse[List[Dict[str, Any]]])
async def simulate_kafka_ingestion(
    count: int = 3,
    simulator: KafkaSimulatorService = Depends(get_kafka_simulator),
    _token: str = Depends(verify_api_token),
):
    """POST /simulate/kafka - Simulates consuming Kafka Telecom events (Comarch OSS, Alarms, Tickets)."""
    if count < 1 or count > 50:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Simulation count must be between 1 and 50.",
        )

    try:
        results = await simulator.run_simulation(count=count)
        return APIResponse(
            success=True,
            message=f"Simulated {count} Kafka NOC events successfully.",
            data=results,
        )
    except SchemaValidationError as sve:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=sve.message)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Kafka simulation error: {str(e)}",
        )


@router.post("/simulate/rest", response_model=APIResponse[RESTCollectorResult])
async def simulate_rest_collector(
    request_body: RESTCollectorRequest,
    collector: RESTCollectorService = Depends(get_rest_collector),
    _token: str = Depends(verify_api_token),
):
    """POST /simulate/rest - Collects data from external REST APIs (ServiceNow, Trend Micro, CyberArk)."""
    try:
        result = await collector.collect_and_store(request_body)
        increment_rest_counter()
        return APIResponse(
            success=True,
            message=f"Collected {result.records_fetched} records from REST endpoint '{request_body.target_system.value}'.",
            data=result,
        )
    except RESTCollectorError as rce:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=rce.message)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"REST Collector error: {str(e)}",
        )
