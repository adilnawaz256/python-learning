import os
import json
import pandas as pd
from typing import Dict, Any

from app.core.logger import log_event
from app.demo.rest_generator import DemoDataGenerator


class DemoFileGenerator:
    """Generates realistic telecom sample files into sample-data/ directory."""

    def __init__(self, target_dir: str = "sample-data"):
        self.target_dir = target_dir

    def ensure_directory(self) -> None:
        """Ensures target directory exists."""
        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir, exist_ok=True)

    def generate_all_files(self) -> Dict[str, str]:
        """Generates alarms.csv, tickets.csv, network_kpi.xlsx, and alarm.json."""
        self.ensure_directory()
        generated_files = {}

        # 1. Generate alarms.csv
        alarms_data = DemoDataGenerator.generate_alarms(count=100)
        df_alarms = pd.DataFrame(alarms_data)
        csv_path = os.path.join(self.target_dir, "alarms.csv")
        df_alarms.to_csv(csv_path, index=False)
        generated_files["alarms.csv"] = csv_path

        # 2. Generate tickets.csv
        tickets_data = DemoDataGenerator.generate_tickets(count=80)
        df_tickets = pd.DataFrame(tickets_data)
        tickets_csv_path = os.path.join(self.target_dir, "tickets.csv")
        df_tickets.to_csv(tickets_csv_path, index=False)
        generated_files["tickets.csv"] = tickets_csv_path

        # 3. Generate network_kpi.xlsx (Multi-sheet Excel)
        kpi_data = DemoDataGenerator.generate_network_health(count=120)
        excel_path = os.path.join(self.target_dir, "network_kpi.xlsx")
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame(kpi_data).to_excel(writer, sheet_name="Tower KPIs", index=False)
            pd.DataFrame(alarms_data[:30]).to_excel(writer, sheet_name="Critical Alarms", index=False)
        generated_files["network_kpi.xlsx"] = excel_path

        # 4. Generate alarm.json
        json_path = os.path.join(self.target_dir, "alarm.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(alarms_data[:50], f, indent=2)
        generated_files["alarm.json"] = json_path

        log_event(
            event_type="Demo Files Generated",
            status="SUCCESS",
            details={
                "directory": self.target_dir,
                "files_created": list(generated_files.keys()),
            },
        )
        return generated_files
