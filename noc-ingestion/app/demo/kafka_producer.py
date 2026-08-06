import json
import asyncio
from datetime import datetime, timezone
from typing import Optional
from aiokafka import AIOKafkaProducer

from app.config.config import get_settings
from app.core.logger import log_event, logger
from app.demo.rest_generator import DemoDataGenerator
from app.services.kafka.consumer import KafkaConsumerService


class DemoKafkaProducer:
    """Demo Kafka Producer that publishes realistic Comarch OSS telecom alarms every 5 seconds."""

    def __init__(self, consumer_service: Optional[KafkaConsumerService] = None):
        self.settings = get_settings()
        self.consumer_service = consumer_service
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts background demo producer publishing loop."""
        if self._is_running:
            return

        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self.producer.start()
            logger.info(f"Demo Kafka Producer connected to {self.settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.warning(f"Demo Kafka Producer running in local simulation pipeline mode (No external Kafka broker required): {e}")
            self.producer = None

        self._is_running = True
        self._task = asyncio.create_task(self._produce_loop())
        logger.info("Demo Kafka Producer background task initialized (publishing every 5 seconds).")

    async def stop(self) -> None:
        """Stops producer background task."""
        self._is_running = False
        if self._task:
            self._task.cancel()
        if self.producer:
            await self.producer.stop()
            logger.info("Demo Kafka Producer stopped gracefully.")

    async def produce_single_demo_event((self)) -> dict:
        """Generates 1 realistic Comarch OSS alarm event and ingests it."""
        alarms = DemoDataGenerator.generate_alarms(count=1)
        alarm = alarms[0]

        # Format as KafkaIngestionMessage schema payload
        kafka_message = {
            "event_id": alarm["alarmId"],
            "source": "Comarch OSS",
            "event_type": alarm["alarmType"],
            "timestamp": alarm["timestamp"],
            "severity": alarm["severity"].upper(),
            "node_id": alarm["towerId"],
            "region": alarm["region"],
            "payload": {
                "ticketId": alarm["ticketId"],
                "siteName": alarm["siteName"],
                "vendor": alarm["vendor"],
                "deviceName": alarm["deviceName"],
                "status": alarm["status"],
                "metrics": alarm["metrics"],
            },
        }

        # 1. Publish to real Kafka broker if connected
        if self.producer:
            try:
                await self.producer.send_and_wait(self.settings.KAFKA_TOPIC, value=kafka_message)
            except Exception as pe:
                logger.error(f"Kafka producer error: {pe}")

        # 2. Feed into ingestion pipeline so raw data lands into MinIO raw/kafka/
        if self.consumer_service:
            try:
                await self.consumer_service.process_single_message(kafka_message)
            except Exception as ie:
                logger.error(f"Demo Kafka ingestion pipeline error: {ie}")

        log_event(
            event_type="Demo Kafka Event Produced",
            status="SUCCESS",
            details={
                "event_id": alarm["alarmId"],
                "towerId": alarm["towerId"],
                "severity": alarm["severity"],
                "vendor": alarm["vendor"],
            },
        )
        return kafka_message

    async def _produce_loop(self) -> None:
        """Loop running every 5 seconds when DEMO_MODE=true."""
        interval = self.settings.DEMO_INTERVAL_KAFKA_SECONDS
        while self._is_running:
            try:
                await self.produce_single_demo_event()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Demo Kafka Producer loop: {e}")
                await asyncio.sleep(interval)
