import json
import asyncio
from typing import Optional, Dict, Any, List
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.config.config import get_settings
from app.core.exceptions import KafkaConsumerError, SchemaValidationError
from app.core.logger import log_event, logger
from app.schemas.kafka_schema import KafkaIngestionMessage
from app.services.storage.minio.client import MinIOService, get_minio_service


class KafkaConsumerService:
    """Kafka Consumer Service reading JSON messages from multiple topics, normalizing, and storing in MinIO."""

    def __init__(self, minio_service: Optional[MinIOService] = None):
        self.settings = get_settings()
        self.minio_service = minio_service or get_minio_service()
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._is_running = False
        self._task: Optional[asyncio.Task] = None
        self.processed_count = 0

    def get_topics(self) -> List[str]:
        return [
            self.settings.KAFKA_TOPIC,
            self.settings.KAFKA_TOPIC_ALARMS,
            self.settings.KAFKA_TOPIC_TICKETS,
            self.settings.KAFKA_TOPIC_NETWORK,
            self.settings.KAFKA_TOPIC_SECURITY,
            self.settings.KAFKA_TOPIC_PERFORMANCE,
        ]

    async def start(self) -> None:
        """Starts Kafka consumer background task subscribing to all topics."""
        if self._is_running:
            return

        topics = self.get_topics()
        try:
            self.consumer = AIOKafkaConsumer(
                *topics,
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
                    "topics": topics,
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
            logger.warning("Kafka consumer running in standalone mode (no external broker).")

    async def stop(self) -> None:
        """Stops Kafka consumer gracefully."""
        self._is_running = False
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped gracefully.")

    def determine_category(self, topic: Optional[str], raw_payload: Dict[str, Any], explicit_category: Optional[str] = None) -> str:
        if explicit_category:
            return f"kafka/{explicit_category}" if not explicit_category.startswith("kafka/") else explicit_category

        if topic == self.settings.KAFKA_TOPIC_ALARMS:
            return "kafka/alarms"
        elif topic == self.settings.KAFKA_TOPIC_TICKETS:
            return "kafka/tickets"
        elif topic == self.settings.KAFKA_TOPIC_NETWORK:
            return "kafka/network"
        elif topic == self.settings.KAFKA_TOPIC_SECURITY:
            return "kafka/security"
        elif topic == self.settings.KAFKA_TOPIC_PERFORMANCE:
            return "kafka/performance"

        # Check payload fields if topic is dynamic or missing
        event_type = str(raw_payload.get("event_type", "")).lower()
        source = str(raw_payload.get("source", "")).lower()

        if "alarm" in event_type or "alarm" in source:
            return "kafka/alarms"
        elif "ticket" in event_type or "ticket" in source or "inc" in str(raw_payload.get("event_id", "")).lower():
            return "kafka/tickets"
        elif "net" in event_type or "network" in source or "cell" in event_type:
            return "kafka/network"
        elif "sec" in event_type or "security" in source or "threat" in event_type:
            return "kafka/security"
        elif "prf" in event_type or "perf" in source or "metric" in event_type:
            return "kafka/performance"

        return "kafka"

    async def process_single_message(
        self,
        raw_payload: Dict[str, Any],
        topic: Optional[str] = None,
        category: Optional[str] = None,
    ) -> str:
        """Validates message schema and stores in MinIO structured path."""
        try:
            validated_msg = KafkaIngestionMessage(**raw_payload)
            msg_dict = validated_msg.model_dump(mode="json")
            event_id = validated_msg.event_id
            source = validated_msg.source.value
        except ValidationError as ve:
            if "source" not in raw_payload or "node_id" not in raw_payload:
                raise SchemaValidationError(f"Invalid Kafka event payload: {ve}")
            msg_dict = raw_payload
            event_id = raw_payload.get("event_id", f"EVT-{self.processed_count}")
            source = raw_payload.get("source", "generic")

        cat = self.determine_category(topic, raw_payload, explicit_category=category)
        source_clean = str(source).lower().replace(" ", "_")
        filename_prefix = f"kafka_{source_clean}_{event_id}"

        # Upload raw JSON to MinIO under raw/kafka/<category>/
        minio_path = await self.minio_service.async_upload_json(
            data_dict=msg_dict,
            category=cat,
            filename_prefix=filename_prefix,
        )

        self.processed_count += 1

        log_event(
            event_type="Kafka Message Processed",
            status="SUCCESS",
            details={
                "event_id": event_id,
                "category": cat,
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
                        await self.process_single_message(msg.value, topic=msg.topic)
                    except Exception as err:
                        logger.error(f"Error processing Kafka message from topic {msg.topic}: {err}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event(
                    event_type="Kafka Error",
                    status="RETRYING",
                    details={"error": str(e)},
                    level="ERROR",
                )
                await asyncio.sleep(5.0)
