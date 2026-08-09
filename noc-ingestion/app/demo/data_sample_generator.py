import os
import csv
import json
import random
from datetime import datetime, timedelta, timezone

class DataSampleGenerator:
    """Generates 90 days of realistic daily NOC metrics covering all 64 KPI fields for data_sample."""

    @staticmethod
    def generate_records(days: int = 90):
        records = []
        base_date = datetime.now(timezone.utc) - timedelta(days=days)

        for d in range(days):
            current_date = base_date + timedelta(days=d)
            date_str = current_date.strftime("%Y-%m-%d %H:%M:%S")

            records.append({
                "date": date_str,
                "s1_interface_failures": random.randint(2, 25),
                "rrc_success_rate": round(random.uniform(98.5, 99.9), 2),
                "network_trouble_tickets_open": random.randint(15, 60),
                "customer_trouble_tickets_open": random.randint(5, 25),
                "provisioning_success_rate": round(random.uniform(97.0, 99.8), 2),
                "sla_compliance": round(random.uniform(95.0, 99.5), 2),
                "session_establishment_network_success_rate": round(random.uniform(98.0, 99.9), 2),
                "end_to_end_availability": round(random.uniform(99.1, 99.99), 2),
                "attach_success_rate": round(random.uniform(98.2, 99.9), 2),
                "service_request_success_rate": round(random.uniform(98.0, 99.8), 2),
                "paging_success_rate": round(random.uniform(97.5, 99.7), 2),
                "eps_bearer_setup_success_rate": round(random.uniform(98.1, 99.9), 2),
                "eps_bearer_drop_rate": round(random.uniform(0.05, 0.45), 2),
                "tau_success_rate": round(random.uniform(98.5, 99.9), 2),
                "mme_cpu_utilization": round(random.uniform(35.0, 78.0), 1),
                "alarm_storm_detection": random.choice([0, 0, 0, 1]),
                "sip_5xx_error_rate": round(random.uniform(0.01, 1.2), 2),
                "call_drop_rate_of_ims_mo": round(random.uniform(0.02, 0.35), 2),
                "max_registered_users": random.randint(450000, 850000),
                "mttr": round(random.uniform(25.0, 110.0), 1),
                "mean_alarm_acknowledgment_time": round(random.uniform(5.0, 25.0), 1),
                "online_users": random.randint(300000, 650000),
                "critical_alarms_count": random.randint(3, 18),
                "ran_active_alarms": random.randint(12, 55),
                "suppressed_alarms_count": random.randint(20, 85),
                "sites_85_prb": random.randint(5, 30),
                "transport_links_80_util": random.randint(2, 14),
                "sip_registration_sr": round(random.uniform(98.8, 99.95), 2),
                "mo_session_connection_rate": round(random.uniform(98.5, 99.9), 2),
                "mt_session_connection_rate": round(random.uniform(98.2, 99.8), 2),
                "call_setup_time_ms": round(random.uniform(180.0, 420.0), 1),
                "total_traffic_mo_mt_erl": round(random.uniform(12000.0, 45000.0), 1),
                "iot_ul_message_success_rate": round(random.uniform(98.5, 99.9), 2),
                "iot_dl_message_success_rate": round(random.uniform(98.3, 99.8), 2),
                "iot_rach_success_rate": round(random.uniform(97.8, 99.7), 2),
                "affected_sites": random.randint(1, 12),
                "site_down": random.randint(0, 5),
                "total_number_of_lte_connected_subs": random.randint(250000, 550000),
                "mcptt_setup_time": round(random.uniform(120.0, 280.0), 1),
                "total_number_of_wifi_connected_subs": random.randint(80000, 220000),
                "mc_push_to_talk_success": random.randint(95000, 145000),
                "mc_push_to_talk_attempts": random.randint(96000, 146000),
                "s1_success_rate": round(random.uniform(98.6, 99.9), 2),
                "s1_resets": random.randint(0, 4),
                "volte_drop_rate": round(random.uniform(0.03, 0.35), 2),
                "links_down": random.randint(0, 4),
                "mean_alarm_clearance_time": round(random.uniform(1.5, 8.5), 1),
                "sms_submission_success_rate": round(random.uniform(99.0, 99.99), 2),
                "smsc_cpu_usage": round(random.uniform(20.0, 65.0), 1),
                "transport_latency": round(random.uniform(8.0, 35.0), 1),
                "transport_packet_loss": round(random.uniform(0.01, 0.25), 2),
                "transport_jitter": round(random.uniform(0.5, 4.2), 1),
                "backhaul_availability": round(random.uniform(99.2, 99.99), 2),
                "sms_delivery_success_rate": round(random.uniform(98.8, 99.95), 2),
                "volte_cssr": round(random.uniform(98.5, 99.9), 2),
                "emergency_call_success_rate": round(random.uniform(99.5, 100.0), 2),
                "intra_lte_ho_success_rate": round(random.uniform(98.2, 99.8), 2),
                "incident_aging_timer": round(random.uniform(2.0, 18.0), 1),
                "iot_attach_success_rate": round(random.uniform(98.0, 99.9), 2),
                "iot_device_availability": round(random.uniform(99.0, 99.95), 2),
                "smsc_traffic": random.randint(150000, 450000),
                "core_cpu_75": random.randint(1, 8),
                "ddos_attacks_detected": random.randint(0, 3),
            })

        return records

    @classmethod
    def generate_and_save_files(cls, target_dir: str = "sample-data"):
        os.makedirs(target_dir, exist_ok=True)
        records = cls.generate_records(90)
        
        csv_path = os.path.join(target_dir, "data_sample.csv")
        json_path = os.path.join(target_dir, "data_sample.json")

        if records:
            fieldnames = list(records[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)

        return csv_path, json_path

if __name__ == "__main__":
    c_path, j_path = DataSampleGenerator.generate_and_save_files()
    print(f"Generated data_sample files successfully: {c_path}, {j_path}")
