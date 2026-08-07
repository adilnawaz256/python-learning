from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.api.deps import (
    get_minio,
    get_kafka_consumer,
    get_demo_scheduler,
    get_rest_connector,
)
from app.api.routes import health, ingestion, demo, uploads, dashboard, jobs
from app.config.config import get_settings
from app.core.exceptions import NocIngestionBaseException
from app.core.logger import setup_logger, log_event
from app.database.session import init_db

logger = setup_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown procedures."""
    log_event("Application Startup", "STARTED", {"app_name": settings.APP_NAME, "env": settings.ENVIRONMENT, "demo_mode": settings.DEMO_MODE})

    # 1. Initialize PostgreSQL tables
    try:
        await init_db()
    except Exception as e:
        logger.warning(f"Database init info: {e}")

    # 2. Init MinIO connection & bucket
    try:
        minio_svc = get_minio()
        minio_svc.connect()
    except Exception as e:
        logger.error(f"Failed to initialize MinIO during startup: {e}")

    # 3. Init Kafka Consumer connection
    kafka_svc = get_kafka_consumer()
    try:
        await kafka_svc.start()
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer during startup: {e}")

    # 4. Init REST Connector Scheduler
    rest_connector = get_rest_connector()
    try:
        await rest_connector.start()
    except Exception as e:
        logger.error(f"Failed to start REST Connector Scheduler: {e}")

    # 5. Init Demo Data Generator Scheduler if DEMO_MODE is enabled
    demo_scheduler = get_demo_scheduler()
    if settings.DEMO_MODE:
        try:
            await demo_scheduler.start()
        except Exception as e:
            logger.error(f"Failed to start Demo Data Scheduler: {e}")

    yield

    # Shutdown
    log_event("Application Shutdown", "STOPPING", {"app_name": settings.APP_NAME})
    if settings.DEMO_MODE:
        await demo_scheduler.stop()
    await rest_connector.stop()
    await kafka_svc.stop()


def create_app() -> FastAPI:
    """FastAPI Application Factory."""
    app = FastAPI(
        title="Enterprise Telecom NOC Ingestion Platform",
        description="Enterprise Multi-Source Telemetry & NOC Ingestion Engine (Kafka, REST, File Uploads, MinIO, Spark, PostgreSQL).",
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global Domain Exception Handler
    @app.exception_handler(NocIngestionBaseException)
    async def domain_exception_handler(request: Request, exc: NocIngestionBaseException):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "success": False,
                "message": exc.message,
                "error": {
                    "type": exc.__class__.__name__,
                    "details": exc.details,
                },
            },
        )

    # Register API Routers
    app.include_router(health.router)
    app.include_router(ingestion.router)
    app.include_router(demo.router)
    app.include_router(uploads.router)
    app.include_router(dashboard.router)
    app.include_router(jobs.router)

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True if settings.ENVIRONMENT == "development" else False,
    )
