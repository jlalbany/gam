"""Configuration for Cloud Function."""
import os

# GCP Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT"))
DATASET_ID = os.environ.get("DATASET_ID", "gam_data")
SECRET_NAME = os.environ.get("SECRET_NAME", "gam_api_config")

# Report Types
REPORT_TYPE_INVENTORY_DAILY = "INVENTORY_DAILY"
REPORT_TYPE_GEO_MONTHLY = "GEO_MONTHLY"

# BigQuery Tables
TABLE_INVENTORY_DAILY = "report_inventory_daily"
TABLE_GEO_MONTHLY = "report_geo_monthly"
