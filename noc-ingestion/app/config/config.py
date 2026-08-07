import os
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Application
    APP_NAME: str = "noc-ingestion"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # MinIO Storage
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_BUCKET: str = "noc-raw-data"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "telecom-events"
    KAFKA_TOPIC_ALARMS: str = "telecom-alarms"
    KAFKA_TOPIC_TICKETS: str = "tickets"
    KAFKA_TOPIC_NETWORK: str = "network-events"
    KAFKA_TOPIC_SECURITY: str = "security-events"
    KAFKA_TOPIC_PERFORMANCE: str = "performance"
    KAFKA_GROUP_ID: str = "noc-ingestion-group"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"

    # REST Collector & Connector
    DEFAULT_REST_TIMEOUT_SECONDS: int = 30
    DEFAULT_MAX_RETRIES: int = 3
    MOCK_EXTERNAL_API_URL: str = "http://localhost:8001"
    REST_POLL_INTERVAL_SECONDS: int = 60

    # Database
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "noc_audit"
    DATABASE_URL: Optional[str] = None

    # Security
    API_TOKEN: str = "noc-secret-ingestion-token-2026"

    # Demo Mode Generator Configuration
    DEMO_MODE: bool = True
    DEMO_INTERVAL_KAFKA_SECONDS: int = 5
    DEMO_INTERVAL_FILE_SECONDS: int = 60

    def get_database_url(self) -> str:
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


@lru_cache()
def get_settings() -> Settings:
    """Cached singleton settings instance."""
    return Settings()
