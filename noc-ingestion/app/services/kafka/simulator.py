import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any
from app.schemas.kafka_schema import KafkaIngestionMessage, EventSource, AlarmSeverity
from app.services.kafka.consumer import KafkaConsumerService


class KafkaSimulatorService:
    """Simulator service to generate mock Kafka events from Comarch OSS, Alarm Events, and Ticket Events."""

    def __init__(self, consumer_service: KafkaConsumerService):
        self.consumer_service = consumer_service

    def generate_mock_events(self, count: int = 3) -> List[KafkaIngestionMessage]:
        """Generates random realistic telecom NOC events."""
        sources = [EventSource.COMARCH_OSS, EventSource.ALARM_EVENTS, EventSource.TICKET_EVENTS]
        event_types = ["LinkDown", "BGPFlap", "HighPacketLoss", "FiberCutDetected", "HardwareFailure"]
        nodes = ["ROUTER-DELHI-CORE-01", "TOWER-MUM-4G-902", "SWITCH-BLR-DC-04", "GATEWAY-HYD-5G-11"]
        severities = [AlarmSeverity.CRITICAL, AlarmSeverity.MAJOR, AlarmSeverity.MINOR, AlarmSeverity.WARNING]

        events: List[KafkaIngestionMessage] = []
        for i in range(count):
            evt = KafkaIngestionMessage(
                event_id=f"evt-{uuid.uuid4().hex[:8]}",
                source=sources[i % len(sources)],
                event_type=event_types[i % len(event_types)],
                timestamp=datetime.now(timezone.utc),
                severity=severities[i % len(severities)],
                node_id=nodes[i % len(nodes)],
                region="IN-NORTH-1" if "DELHI" in nodes[i % len(nodes)] else "IN-WEST-1",
                payload={
                    "interface_id": f"Gi0/{i}/1",
                    "affected_subscribers": (i + 1) * 120,
                    "simulated": True,
                    "telecom_domain": "RAN_CORE_TRANSPORT",
                },
            )
            events.append(evt)
        return events

    async def run_simulation(self, count: int = 3) -> List[Dict[str, Any]]:
        """Simulates publishing and consuming mock events through the ingestion pipeline."""
        events = self.generate_mock_events(count)
        results: List[Dict[str, Any]] = []

        for evt in events:
            raw_dict = evt.model_dump(mode="json")
            minio_path = await self.consumer_service.process_single_message(raw_dict)
            results.append({
                "event_id": evt.event_id,
                "source": evt.source.value,
                "event_type": evt.event_type,
                "node_id": evt.node_id,
                "minio_path": minio_path,
                "status": "INGESTED",
            })

        return results
