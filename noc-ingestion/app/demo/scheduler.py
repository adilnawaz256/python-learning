import asyncio
from typing import Optional

from app.config.config import get_settings
from app.core.logger import logger
from app.demo.kafka_producer import DemoKafkaProducer
from app.demo.file_generator import DemoFileGenerator
from app.services.kafka.consumer import KafkaConsumerService


class DemoScheduler:
    """Orchestrates periodic generation of Kafka events and sample files in DEMO_MODE."""

    def __init__(self, consumer_service: Optional[KafkaConsumerService] = None):
        self.settings = get_settings()
        self.kafka_producer = DemoKafkaProducer(consumer_service=consumer_service)
        self.file_generator = DemoFileGenerator()
        self._is_running = False
        self._file_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts demo producer and file generator tasks."""
        if not self.settings.DEMO_MODE:
            logger.info("DEMO_MODE is disabled. Demo scheduler will not start.")
            return

        logger.info("🚀 DEMO_MODE enabled! Initializing Demo Scheduler...")

        # 1. Generate initial batch of sample files immediately on startup
        try:
            self.file_generator.generate_all_files()
        except Exception as e:
            logger.error(f"Error generating initial demo sample files: {e}")

        # 2. Start Kafka Producer (runs loop every 5s)
        await self.kafka_producer.start()

        # 3. Start File Generator loop (runs loop every 60s)
        self._is_running = True
        self._file_task = asyncio.create_task(self._file_generation_loop())

    async def stop(self) -> None:
        """Stops demo scheduler tasks."""
        self._is_running = False
        if self._file_task:
            self._file_task.cancel()
        await self.kafka_producer.stop()
        logger.info("Demo Scheduler stopped.")

    async def _file_generation_loop(self) -> None:
        """Regenerates sample files in sample-data/ every 60 seconds."""
        interval = self.settings.DEMO_INTERVAL_FILE_SECONDS
        while self._is_running:
            try:
                await asyncio.sleep(interval)
                self.file_generator.generate_all_files()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Demo File Generator loop: {e}")
