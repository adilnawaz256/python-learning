import json
import asyncio
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer
from app.config.config import get_settings
from app.core.logger import log_event, logger


class KafkaProducerService:
    """Reusable Kafka Producer Service supporting multiple topics."""

    def __init__(self):
        self.settings = get_settings()
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_running = False

    async def start(self) -> None:
        """Starts Kafka producer connection."""
        if self._is_running:
            return

        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self.producer.start()
            self._is_running = True
            log_event(
                event_type="Kafka Producer Connected",
                status="SUCCESS",
                details={"bootstrap_servers": self.settings.KAFKA_BOOTSTRAP_SERVERS},
            )
        except Exception as e:
            logger.warning(f"Kafka Producer running in simulated mode: {e}")
            self.producer = None
            self._is_running = True

    async def stop(self) -> None:
        """Stops Kafka producer gracefully."""
        self._is_running = False
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped.")

    async def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """Publishes a JSON payload to a specific Kafka topic."""
        if self.producer:
            try:
                await self.producer.send_and_wait(topic, value=message)
                return True
            except Exception as e:
                logger.error(f"Failed to publish message to topic '{topic}': {e}")
                return False
        else:
            logger.debug(f"[Simulated Kafka Publish] Topic: {topic}, EventID: {message.get('event_id', 'N/A')}")
            return True
