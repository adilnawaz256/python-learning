import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

VENDORS = ["Huawei", "Nokia", "Ericsson", "Cisco", "ZTE"]
SEVERITIES = ["Critical", "Major", "Minor", "Warning"]
REGIONS = ["IN-NORTH-DELHI", "IN-WEST-MUMBAI", "IN-SOUTH-BANGALORE", "IN-EAST-KOLKATA", "IN-CENTRAL-HYDERABAD"]
ALARM_TYPES = ["LinkDown", "BGPFlap", "HighTemperature", "PowerSupplyFailure", "OpticalPowerLow", "VSWR_High"]
STATUSES = ["ACTIVE", "ACKNOWLEDGED", "CLEARED", "PENDING_DISPATCH"]
TOWER_PREFIXES = ["TOWER-DEL", "TOWER-MUM", "TOWER-BLR", "TOWER-KOL", "TOWER-HYD"]


class DemoDataGenerator:
    """Generates dynamic realistic telecom NOC demo records for API endpoints and file generation."""

    @staticmethod
    def generate_alarms(count: int = 75) -> List[Dict[str, Any]]:
        """Generates 50-100 realistic Comarch OSS style alarm records."""
        alarms = []
        now = datetime.now(timezone.utc)

        for i in range(count):
            region = random.choice(REGIONS)
            prefix = random.choice(TOWER_PREFIXES)
            tower_num = random.randint(100, 999)
            tower_id = f"{prefix}-{tower_num}"
            vendor = random.choice(VENDORS)
            severity = random.choice(SEVERITIES)
            alarm_type = random.choice(ALARM_TYPES)

            alarms.append({
                "alarmId": f"ALM-{now.strftime('%Y%m%d')}-{random.randint(10000, 99999)}",
                "ticketId": f"INC{random.randint(1000000, 9999999)}",
                "towerId": tower_id,
                "siteName": f"Site_{region.split('-')[-1]}_{tower_num}",
                "region": region,
                "vendor": vendor,
                "severity": severity,
                "alarmType": alarm_type,
                "deviceName": f"{vendor.lower()}-{alarm_type.lower()}-{tower_num}",
                "timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
                "status": random.choice(STATUSES),
                "metrics": {
                    "vswr_ratio": round(random.uniform(1.1, 3.5), 2),
                    "power_dbm": round(random.uniform(-45.0, -10.0), 1),
                    "temperature_c": random.randint(25, 75)
                }
            })
        return alarms

    @staticmethod
    def generate_tickets(count: int = 60) -> List[Dict[str, Any]]:
        """Generates 50-100 realistic ServiceNow ITSM ticket records."""
        tickets = []
        now = datetime.now(timezone.utc)
        categories = ["RAN Outage", "Core Router Flap", "Fiber Cut", "Power Fail", "Security Incident"]
        groups = ["L2-RAN-Support", "L3-IP-Core", "Field-Engineering", "SOC-Team"]

        for i in range(count):
            tickets.append({
                "ticketId": f"INC{random.randint(1000000, 9999999)}",
                "sys_id": uuid.uuid4().hex,
                "short_description": f"{random.choice(categories)} on {random.choice(REGIONS)} network segment",
                "priority": random.choice(["P1-Critical", "P2-High", "P3-Moderate", "P4-Low"]),
                "category": random.choice(categories),
                "assigned_group": random.choice(groups),
                "state": random.choice(["New", "In Progress", "On Hold", "Resolved", "Closed"]),
                "created_at": (now - timedelta(hours=random.randint(1, 48))).isoformat(),
                "sla_due": (now + timedelta(hours=random.randint(2, 12))).isoformat(),
                "affected_users": random.randint(50, 5000)
            })
        return tickets

    @staticmethod
    def generate_network_health(count: int = 80) -> List[Dict[str, Any]]:
        """Generates 50-100 network KPI health metrics across towers and routers."""
        health_records = []
        now = datetime.now(timezone.utc)

        for i in range(count):
            region = random.choice(REGIONS)
            tower_id = f"{random.choice(TOWER_PREFIXES)}-{random.randint(100, 999)}"
            throughput = round(random.uniform(150.0, 1200.0), 2)
            packet_loss = round(random.uniform(0.01, 4.5), 3)
            latency = round(random.uniform(8.0, 85.0), 1)
            connected_users = random.randint(120, 3500)

            # Health score computation
            health_score = max(0, min(100, int(100 - (packet_loss * 15) - (latency / 5))))

            health_records.append({
                "towerId": tower_id,
                "siteName": f"CellSite-{tower_id}",
                "region": region,
                "technology": random.choice(["4G_LTE", "5G_NR", "VOLTE", "IP_BACKHAUL"]),
                "throughput_mbps": throughput,
                "packet_loss_pct": packet_loss,
                "latency_ms": latency,
                "connected_users": connected_users,
                "health_score": health_score,
                "timestamp": now.isoformat()
            })
        return health_records

    @staticmethod
    def generate_security_events(count: int = 50) -> List[Dict[str, Any]]:
        """Generates 50-100 Trend Micro / CyberArk security threat events."""
        security_records = []
        now = datetime.now(timezone.utc)
        threat_levels = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        attack_types = ["DDoS Flooding", "Brute Force SSH", "Privileged Escalation", "Malware C2 Communication", "Port Scan"]
        actions = ["BLOCKED", "QUARANTINED", "ALERTED", "SESSION_TERMINATED"]

        for i in range(count):
            security_records.append({
                "event_id": f"SEC-{uuid.uuid4().hex[:10]}",
                "source_system": random.choice(["Trend Micro Vision One", "CyberArk PAM"]),
                "threat_level": random.choice(threat_levels),
                "attack_type": random.choice(attack_types),
                "source_ip": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "destination_ip": f"10.0.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "target_device": f"CORE-GW-{random.randint(1, 20)}",
                "action_taken": random.choice(actions),
                "timestamp": (now - timedelta(minutes=random.randint(1, 300))).isoformat()
            })
        return security_records
