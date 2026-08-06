import json
import asyncio
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.config.config import get_settings
from app.core.exceptions import KafkaConsumerError, SchemaValidationError
from app.core.logger import log_event, logger
from app.schemas.kafka_schema import KafkaIngestionMessage
from app.services.storage.minio.client import MinIOService, get_minio_service


class KafkaConsumerService:
    """Kafka Consumer Service that reads JSON messages, validates schema, and stores raw JSON in MinIO."""

    def __init__(self, minio_service: Optional[MinIOService] = None):
        self.settings = get_settings()
        self.minio_service = minio_service or get_minio_service()
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self.processed_count = 0

    async def start(self) -> None:
        """Starts Kafka consumer background task."""
        if self._is_running:
            return

        try:
            self.consumer = AIOKafkaConsumer(
                self.settings.KAFKA_TOPIC,
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=self.settings.KAFKA_GROUP_ID,
                auto_offset_reset=self.settings.KAFKA_AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            await self.consumer.start()
            self._is_running = True
            log_event(
                event_type="Kafka Connected",
                status="SUCCESS",
                details={
                    "bootstrap_servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
                    "topic": self.settings.KAFKA_TOPIC,
                },
            )
            self._task = asyncio.create_task(self._consume_loop())
        except Exception as e:
            log_event(
                event_type="Kafka Error",
                status="FAILED",
                details={"error": str(e)},
                level="ERROR",
            )
            logger.warning("Kafka consumer could not connect directly (Kafka broker might be offline in local test mode).")

    async def stop(self) -> None:
        """Stops Kafka consumer gracefully."""
        self._is_running = False
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped gracefully.")

    async def process_single_message(self, raw_payload: Dict[str, Any]) -> str:
        """Validates message schema and stores in MinIO."""
        try:
            validated_msg = KafkaIngestionMessage(**raw_payload)
        except ValidationError as ve:
            log_event(
                event_type="Kafka Error",
                status="INVALID_JSON",
                details={"error": ve.errors(), "raw_payload": raw_payload},
                level="ERROR",
            )
            raise SchemaValidationError(f"Invalid Kafka event payload: {ve}")

        source_clean = validated_msg.source.value.lower().replace(" ", "_")
        filename_prefix = f"kafka_{source_clean}_{validated_msg.event_id}"

        # Async upload raw JSON to MinIO under raw/kafka/
        minio_path = await self.minio_service.async_upload_json(
            data_dict=validated_msg.model_dump(mode="json"),
            category="kafka",
            filename_prefix=filename_prefix,
        )

        self.processed_count += 1

        log_event(
            event_type="Kafka Message Processed",
            status="SUCCESS",
            details={
                "event_id": validated_msg.event_id,
                "source": validated_msg.source.value,
                "node_id": validated_msg.node_id,
                "minio_path": minio_path,
            },
        )

        return minio_path

    async def _consume_loop(self) -> None:
        """Main consuming loop with auto retry on failure."""
        if not self.consumer:
            return

        while self._is_running:
            try:
                async for msg in self.consumer:
                    if not self._is_running:
                        break
                    try:
                        await self.process_single_message(msg.value)
                    except Exception as err:
                        logger.error(f"Error processing Kafka message offset {msg.offset}: {err}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event(
                    event_type="Kafka Error",
                    status="RETRYING",
                    details={"error": str(e)},
                    level="ERROR",
                )
                await asyncio.sleep(5.0)  # Wait before reconnecting / retrying
