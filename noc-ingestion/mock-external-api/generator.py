import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

VENDORS = ["Huawei", "Nokia", "Ericsson", "Cisco", "ZTE"]
SEVERITIES = ["CRITICAL", "MAJOR", "MINOR", "WARNING", "INFO"]
REGIONS = ["IN-NORTH-DELHI", "IN-WEST-MUMBAI", "IN-SOUTH-BANGALORE", "IN-EAST-KOLKATA", "IN-CENTRAL-HYDERABAD"]
ALARM_TYPES = ["LinkDown", "BGPFlap", "HighTemperature", "PowerSupplyFailure", "OpticalPowerLow", "VSWR_High"]
STATUSES = ["ACTIVE", "ACKNOWLEDGED", "CLEARED", "PENDING_DISPATCH"]
TOWER_PREFIXES = ["TOWER-DEL", "TOWER-MUM", "TOWER-BLR", "TOWER-KOL", "TOWER-HYD"]


class MockDataGenerator:
    """Generates realistic telemetry, alarm, incident, and performance data for external NOC monitoring."""

    @staticmethod
    def generate_alarms(count: int = 50) -> List[Dict[str, Any]]:
        alarms = []
        now = datetime.now(timezone.utc)
        for _ in range(count):
            region = random.choice(REGIONS)
            tower_num = random.randint(100, 999)
            prefix = random.choice(TOWER_PREFIXES)
            vendor = random.choice(VENDORS)
            alarm_type = random.choice(ALARM_TYPES)
            alarms.append({
                "alarm_id": f"ALM-{now.strftime('%Y%m%d')}-{random.randint(10000, 99999)}",
                "ticket_id": f"INC{random.randint(1000000, 9999999)}",
                "tower_id": f"{prefix}-{tower_num}",
                "site_name": f"Site_{region.split('-')[-1]}_{tower_num}",
                "region": region,
                "vendor": vendor,
                "severity": random.choice(SEVERITIES),
                "alarm_type": alarm_type,
                "device_name": f"{vendor.lower()}-{alarm_type.lower()}-{tower_num}",
                "status": random.choice(STATUSES),
                "timestamp": (now - timedelta(minutes=random.randint(0, 60))).isoformat(),
                "metrics": {
                    "vswr_ratio": round(random.uniform(1.1, 3.5), 2),
                    "power_dbm": round(random.uniform(-45.0, -10.0), 1),
                    "temperature_c": random.randint(25, 80)
                }
            })
        return alarms

    @staticmethod
    def generate_tickets(count: int = 40) -> List[Dict[str, Any]]:
        tickets = []
        now = datetime.now(timezone.utc)
        categories = ["RAN Outage", "Core Router Flap", "Fiber Cut", "Power Fail", "Security Incident"]
        groups = ["L2-RAN-Support", "L3-IP-Core", "Field-Engineering", "SOC-Team"]
        for _ in range(count):
            tickets.append({
                "ticket_id": f"INC{random.randint(1000000, 9999999)}",
                "sys_id": uuid.uuid4().hex,
                "short_description": f"{random.choice(categories)} on {random.choice(REGIONS)} segment",
                "priority": random.choice(["P1-Critical", "P2-High", "P3-Moderate", "P4-Low"]),
                "category": random.choice(categories),
                "assigned_group": random.choice(groups),
                "state": random.choice(["New", "In Progress", "On Hold", "Resolved", "Closed"]),
                "created_at": (now - timedelta(hours=random.randint(1, 24))).isoformat(),
                "sla_due": (now + timedelta(hours=random.randint(2, 12))).isoformat(),
                "affected_users": random.randint(50, 5000)
            })
        return tickets

    @staticmethod
    def generate_network_events(count: int = 50) -> List[Dict[str, Any]]:
        events = []
        now = datetime.now(timezone.utc)
        event_types = ["CellTowerDown", "BGPPeeringLost", "FiberCutDetected", "OpticalLinkFailure", "RouterInterfaceFlap"]
        for _ in range(count):
            region = random.choice(REGIONS)
            events.append({
                "event_id": f"NET-{uuid.uuid4().hex[:10]}",
                "event_type": random.choice(event_types),
                "region": region,
                "site_id": f"SITE-{random.randint(100, 999)}",
                "vendor": random.choice(VENDORS),
                "severity": random.choice(SEVERITIES),
                "timestamp": (now - timedelta(minutes=random.randint(0, 30))).isoformat(),
                "details": {
                    "affected_channels": random.randint(1, 16),
                    "link_capacity_gbps": random.choice([10, 40, 100]),
                    "auto_recovered": random.choice([True, False])
                }
            })
        return events

    @staticmethod
    def generate_security_events(count: int = 35) -> List[Dict[str, Any]]:
        events = []
        now = datetime.now(timezone.utc)
        threat_levels = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        attack_types = ["DDoS Flooding", "Brute Force SSH", "Privileged Escalation", "Malware C2 Communication", "Port Scan"]
        actions = ["BLOCKED", "QUARANTINED", "ALERTED", "SESSION_TERMINATED"]

        for _ in range(count):
            events.append({
                "event_id": f"SEC-{uuid.uuid4().hex[:10]}",
                "source_system": random.choice(["Trend Micro Vision One", "CyberArk PAM"]),
                "threat_level": random.choice(threat_levels),
                "attack_type": random.choice(attack_types),
                "source_ip": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "destination_ip": f"10.0.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "target_device": f"CORE-GW-{random.randint(1, 20)}",
                "action_taken": random.choice(actions),
                "timestamp": (now - timedelta(minutes=random.randint(0, 120))).isoformat()
            })
        return events

    @staticmethod
    def generate_performance(count: int = 60) -> List[Dict[str, Any]]:
        metrics = []
        now = datetime.now(timezone.utc)
        for _ in range(count):
            cpu = round(random.uniform(10.0, 99.0), 2)
            mem = round(random.uniform(20.0, 95.0), 2)
            latency = round(random.uniform(5.0, 120.0), 2)
            packet_loss = round(random.uniform(0.0, 5.0), 3)
            metrics.append({
                "metric_id": f"PRF-{uuid.uuid4().hex[:10]}",
                "device_id": f"DEV-{random.choice(VENDORS).lower()}-{random.randint(100, 999)}",
                "region": random.choice(REGIONS),
                "cpu_utilization_pct": cpu,
                "memory_utilization_pct": mem,
                "latency_ms": latency,
                "packet_loss_pct": packet_loss,
                "throughput_mbps": round(random.uniform(100.0, 10000.0), 2),
                "timestamp": now.isoformat()
            })
        return metrics

    @staticmethod
    def generate_sites(count: int = 25) -> List[Dict[str, Any]]:
        sites = []
        for i in range(1, count + 1):
            region = random.choice(REGIONS)
            sites.append({
                "site_id": f"SITE-{i:03d}",
                "site_name": f"NOC_Hub_{region.split('-')[-1]}_{i}",
                "region": region,
                "latitude": round(random.uniform(8.0, 35.0), 6),
                "longitude": round(random.uniform(68.0, 97.0), 6),
                "status": random.choice(["OPERATIONAL", "DEGRADED", "MAINTENANCE"]),
                "power_backup": random.choice(["SOLAR_BATTERY", "DIESEL_GENERATOR", "GRID_PRIMARY"])
            })
        return sites

    @staticmethod
    def generate_devices(count: int = 40) -> List[Dict[str, Any]]:
        devices = []
        for i in range(1, count + 1):
            vendor = random.choice(VENDORS)
            devices.append({
                "device_id": f"DEV-{vendor.lower()}-{i:03d}",
                "device_name": f"{vendor}-CoreSwitch-{i}",
                "vendor": vendor,
                "device_type": random.choice(["Router", "Switch", "BaseStation", "Firewall"]),
                "ip_address": f"10.{random.randint(1, 10)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "firmware_version": f"v{random.randint(1, 10)}.{random.randint(0, 9)}",
                "status": random.choice(["ONLINE", "WARNING", "OFFLINE"])
            })
        return devices
