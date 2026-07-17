# config.py
"""
body for variables storage implementation
"""

import os
from dotenv import load_dotenv


# ── Config ────────────────────────────────────────────────────────────────────────────────────────────────────────────
# loading .env data from .env and getting configs from there
load_dotenv()
database_url = os.getenv("TTC_DATABASE_URL")
db_coded_name = os.getenv("TTC_DATABASE_NAME")
scd2_item_table_schema = os.getenv("TTC_DATABASE_SCHEMA_NAME")
fact_item_table_schema = os.getenv("TTC_DATABASE_SCHEMA_NAME")
fact_item_table = os.getenv("TTC_DATABASE_FACT_TABLE_NAME")
scd2_item_table = os.getenv("TTC_DATABASE_SCD2_TABLE_NAME")

url = os.getenv("TEST_URL")

# other configs:
log_dir = "output/logs"

ttc_save_json_dir = "output/json_data_ttc"
ttc_save_graph_dir = "output/graphs_ttc"


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
