import httpx
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config.config import get_settings
from app.core.logger import log_event, logger
from app.services.kafka.producer import KafkaProducerService
from app.services.kafka.consumer import KafkaConsumerService


class RESTConnectorScheduler:
    """Automated REST API Connector using APScheduler.
    
    Polls mock/external endpoints every minute:
    - /api/v1/alarms
    - /api/v1/tickets
    - /api/v1/network-events
    - /api/v1/security-events
    - /api/v1/performance
    
    Normalizes every record into standard Kafka payload schema and publishes to appropriate topic.
    """

    def __init__(
        self,
        producer_service: Optional[KafkaProducerService] = None,
        consumer_service: Optional[KafkaConsumerService] = None,
    ):
        self.settings = get_settings()
        self.producer = producer_service or KafkaProducerService()
        self.consumer_service = consumer_service
        self.scheduler = AsyncIOScheduler()
        self._is_running = False

    async def start(self) -> None:
        """Starts APScheduler background job."""
        if self._is_running:
            return

        await self.producer.start()
        interval = self.settings.REST_POLL_INTERVAL_SECONDS

        self.scheduler.add_job(
            self.poll_all_endpoints,
            trigger="interval",
            seconds=interval,
            id="rest_connector_poll_job",
            replace_existing=True,
        )
        self.scheduler.start()
        self._is_running = True
        logger.info(f"🚀 REST Connector Scheduler started (polling every {interval}s).")

        # Perform an initial poll asynchronously on startup
        asyncio.create_task(self.poll_all_endpoints())

    async def stop(self) -> None:
        """Stops APScheduler."""
        if self._is_running:
            self.scheduler.shutdown(wait=False)
            await self.producer.stop()
            self._is_running = False
            logger.info("REST Connector Scheduler stopped.")

    async def poll_all_endpoints(self) -> Dict[str, int]:
        """Polls external REST endpoints, normalizes records, and publishes to Kafka topics."""
        base_url = self.settings.MOCK_EXTERNAL_API_URL.rstrip("/")
        endpoints = [
            ("/api/v1/alarms", self.settings.KAFKA_TOPIC_ALARMS, "alarm"),
            ("/api/v1/tickets", self.settings.KAFKA_TOPIC_TICKETS, "ticket"),
            ("/api/v1/network-events", self.settings.KAFKA_TOPIC_NETWORK, "network"),
            ("/api/v1/security-events", self.settings.KAFKA_TOPIC_SECURITY, "security"),
            ("/api/v1/performance", self.settings.KAFKA_TOPIC_PERFORMANCE, "performance"),
        ]

        results = {}
        async with httpx.AsyncClient(timeout=15.0) as client:
            for ep_path, topic, category in endpoints:
                url = f"{base_url}{ep_path}"
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        body = resp.json()
                        records = body.get("data", [])
                        count = 0
                        for rec in records:
                            normalized = self.normalize_record(rec, category)
                            await self.producer.publish(topic, normalized)
                            # Direct ingest fallback to MinIO landing zone if consumer is available
                            if self.consumer_service:
                                try:
                                    await self.consumer_service.process_single_message(normalized, category=category)
                                except Exception:
                                    pass
                            count += 1
                        results[category] = count
                        log_event(
                            event_type="REST Connector Poll",
                            status="SUCCESS",
                            details={"endpoint": ep_path, "topic": topic, "records_published": count},
                        )
                    else:
                        logger.warning(f"REST Connector poll {ep_path} returned status {resp.status_code}")
                except Exception as e:
                    logger.error(f"REST Connector poll error for {ep_path}: {e}")
                    results[category] = 0

        return results

    @staticmethod
    def normalize_record(raw: Dict[str, Any], category: str) -> Dict[str, Any]:
        """Normalizes external REST payload into unified Kafka schema."""
        now = datetime.now(timezone.utc).isoformat()
        event_id = (
            raw.get("alarm_id")
            or raw.get("ticket_id")
            or raw.get("event_id")
            or raw.get("metric_id")
            or f"EXT-{category.upper()}-{now}"
        )
        severity = (raw.get("severity") or raw.get("threat_level") or raw.get("priority") or "INFO").upper()
        if "CRITICAL" in severity or "P1" in severity:
            norm_severity = "CRITICAL"
        elif "MAJOR" in severity or "HIGH" in severity or "P2" in severity:
            norm_severity = "MAJOR"
        elif "MINOR" in severity or "MEDIUM" in severity or "P3" in severity:
            norm_severity = "MINOR"
        elif "WARNING" in severity:
            norm_severity = "WARNING"
        else:
            norm_severity = "INFO"

        node_id = (
            raw.get("tower_id")
            or raw.get("device_id")
            or raw.get("target_device")
            or raw.get("site_id")
            or "NODE-GENERIC"
        )
        region = raw.get("region") or "SA-RIYADH"

        return {
            "event_id": event_id,
            "source": f"REST_{category.upper()}",
            "event_type": raw.get("alarm_type") or raw.get("category") or raw.get("event_type") or f"REST_{category}",
            "timestamp": raw.get("timestamp") or raw.get("created_at") or now,
            "severity": norm_severity,
            "node_id": node_id,
            "region": region,
            "payload": raw,
        }
