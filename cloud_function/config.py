"""Configuration for GAM reporting automation (REST API)."""
import os

# GCP Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT", "true-oasis-479111-t5"))
NETWORK_CODE = "33047445"
DATASET_ID = os.environ.get("DATASET_ID", "gam_data")
LOCATION = "EU"
TIMEZONE = "Europe/Paris"

# GAM Report IDs (created in GAM UI)
REPORT_IDS = {
    "audience_interest": "6493240984",      # BQ_Interests - Monthly
    "inventory_daily": "6493295630",        # BQ_ReportInventoryDaily - Daily
    "audience_demographics": "6493277740",  # BQ_Demo - Monthly
    "fill_rate": "6493312298",             # BQ_ReportRemplissage - Daily
    "geo_monthly": "6493333640",           # BQ_Report Geographique - Monthly
}

# BigQuery Table Names
TABLES = {
    "audience_interest": "report_audience_interest",
    "inventory_daily": "inventory_daily",
    "audience_demographics": "report_audience_demographics",
    "fill_rate": "report_fill_rate",
    "geo_monthly": "geo_monthly",
}

# Report Type Constants (for Cloud Function routing)
REPORT_TYPE_INVENTORY_DAILY = "inventory_daily"
REPORT_TYPE_GEO_MONTHLY = "geo_monthly"
REPORT_TYPE_AUDIENCE_INTEREST = "audience_interest"
REPORT_TYPE_AUDIENCE_DEMOGRAPHICS = "audience_demographics"
REPORT_TYPE_FILL_RATE = "fill_rate"

# Table Name Constants (for Cloud Function)
TABLE_INVENTORY_DAILY = TABLES["inventory_daily"]
TABLE_GEO_MONTHLY = TABLES["geo_monthly"]
TABLE_AUDIENCE_INTEREST = TABLES["audience_interest"]
TABLE_AUDIENCE_DEMOGRAPHICS = TABLES["audience_demographics"]
TABLE_FILL_RATE = TABLES["fill_rate"]

# Column Mappings: API column name → BigQuery column name
# All columns from GAM reports are preserved
COLUMN_MAPPINGS = {
    "audience_interest": {
        "MONTH_YEAR": "report_date",
        "INTEREST": "interest_category",
        "SESSIONS": "sessions",
        "GOOGLE_ANALYTICS_IMPRESSIONS": "ga_impressions",
        "BOUNCE_RATE": "bounce_rate",
        "ENGAGEMENT_RATE": "engagement_rate",
        "ACTIVE_USERS": "active_users",
        "GOOGLE_ANALYTICS_VIEWS": "ga_views",
        "GOOGLE_ANALYTICS_VIEWS_PER_USER": "ga_views_per_user",
        "AD_VIEWERS": "ad_viewers",
    },
    "inventory_daily": {
        "DATE": "date",
        "AD_UNIT_NAME_TOP_LEVEL": "ad_unit_top_level",
        "AD_UNIT_NAME": "ad_unit_name",
        "AD_UNIT_NAME_LEVEL_1": "ad_unit_level_1",  # Extra column from GAM, will be filtered out
        "AD_UNIT_NAME_LEVEL_2": "ad_unit_level_2",  # Extra column from GAM, will be filtered out
        "ORDER_NAME": "order_name",
        "DEVICE_CATEGORY_NAME": "device_category",
        "RENDERED_CREATIVE_SIZE": "creative_size",
        "IMPRESSIONS": "ad_server_impressions",
        "CLICKS": "ad_server_clicks",
        "AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS": "active_view_measurable_impressions",
        "ACTIVE_VIEW_VIEWABLE_IMPRESSIONS": "active_view_viewable_impressions",
    },
    "audience_demographics": {
        "MONTH_YEAR": "report_date",
        "GENDER_NAME": "gender",
        "AGE_BRACKET_NAME": "age_bracket",
        "SESSIONS": "sessions",
        "GOOGLE_ANALYTICS_IMPRESSIONS": "ga_impressions",
        "BOUNCE_RATE": "bounce_rate",
        "ACTIVE_USERS": "active_users",
        "GOOGLE_ANALYTICS_VIEWS": "ga_views",
        "GOOGLE_ANALYTICS_VIEWS_PER_USER": "ga_views_per_user",
        "AD_VIEWERS": "ad_viewers",
    },
    "fill_rate": {
        "DATE": "date",
        "AD_UNIT_NAME": "ad_unit_name",  # Use AD_UNIT_NAME instead of AD_UNIT_NAME_ALL_LEVEL
        "UNFILLED_IMPRESSIONS": "unfilled_impressions",
        "CODE_SERVED_COUNT": "code_served_count",
        "RESPONSES_SERVED": "responses_served",
        "AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "FILL_RATE": "fill_rate",
        "AD_REQUESTS": "ad_requests",
    },
    "geo_monthly": {
        "COUNTRY_NAME": "country_name",
        "COUNTRY_CODE": "country_code",
        "MONTH_YEAR": "report_date",
        "IMPRESSIONS": "ad_server_impressions",
        "CLICKS": "ad_server_clicks",
        "AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_RATE": "active_view_measurable_rate",
        "AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS": "active_view_viewable_impressions",
    },
}

# Type Conversions
# Specify which columns need type conversion
TYPE_CONVERSIONS = {
    "audience_interest": {
        "report_date": "month_year",  # Special: YYMM → DATE (first of month)
        "bounce_rate": "float64",
        "engagement_rate": "float64",
        "ga_views_per_user": "float64",
        "sessions": "int64",
        "ga_impressions": "int64",
        "active_users": "int64",
        "ga_views": "int64",
        "ad_viewers": "int64",
    },
    "inventory_daily": {
        "date": "date_int",  # Special: YYYYMMDD → DATE
        "ad_server_impressions": "int64",
        "ad_server_clicks": "int64",
        "active_view_measurable_impressions": "int64",
        "active_view_viewable_impressions": "int64",
    },
    "audience_demographics": {
        "report_date": "month_year",  # Special: YYMM → DATE (first of month)
        "gender": "string",  # Ensure string type (GAM returns "Femme", "Homme", "0")
        "age_bracket": "string",  # Ensure string type (GAM returns age ranges or "0")
        "bounce_rate": "float64",
        "ga_views_per_user": "float64",
        "sessions": "int64",
        "ga_impressions": "int64",
        "active_users": "int64",
        "ga_views": "int64",
        "ad_viewers": "int64",
    },
    "fill_rate": {
        "date": "date_int",  # Special: YYYYMMDD → DATE
        "ad_unit_name": "string",  # Ensure string type for ad unit names
        "unfilled_impressions": "int64",
        "code_served_count": "int64",
        "responses_served": "int64",
        "ad_server_impressions": "int64",
        "fill_rate": "float64",
        "ad_requests": "int64",
    },
    "geo_monthly": {
        "report_date": "month_year",  # Special: YYMM → DATE (first of month)
        "country_name": "string",  # Ensure string type (GAM returns 0 for unknown)
        "country_code": "string",  # Ensure string type (GAM returns 0 for unknown)
        "ad_server_impressions": "int64",
        "ad_server_clicks": "int64",
        "active_view_measurable_rate": "float64",
        "active_view_viewable_impressions": "int64",
    },
}

# BigQuery Table Schemas
# All columns from GAM reports are included
SCHEMAS = {
    "audience_interest": [
        {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "interest_category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sessions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "bounce_rate", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "engagement_rate", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "active_users", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_views", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_views_per_user", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ad_viewers", "type": "INT64", "mode": "NULLABLE"},
    ],
    "inventory_daily": [
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "ad_unit_top_level", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ad_unit_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "creative_size", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ad_server_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ad_server_clicks", "type": "INT64", "mode": "NULLABLE"},
        {"name": "active_view_measurable_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "active_view_viewable_impressions", "type": "INT64", "mode": "NULLABLE"},
    ],
    "audience_demographics": [
        {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age_bracket", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sessions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "bounce_rate", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "active_users", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_views", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ga_views_per_user", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ad_viewers", "type": "INT64", "mode": "NULLABLE"},
    ],
    "fill_rate": [
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "ad_unit_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "unfilled_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "code_served_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "responses_served", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ad_server_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "fill_rate", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "ad_requests", "type": "INT64", "mode": "NULLABLE"},
    ],
    "geo_monthly": [
        {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "country_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "country_code", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ad_server_impressions", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ad_server_clicks", "type": "INT64", "mode": "NULLABLE"},
        {"name": "active_view_measurable_rate", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "active_view_viewable_impressions", "type": "INT64", "mode": "NULLABLE"},
    ],
}

# Table Partitioning Configuration
PARTITION_CONFIG = {
    "audience_interest": {
        "field": "report_date",
        "type": "MONTH",
    },
    "inventory_daily": {
        "field": "date",
        "type": "DAY",
    },
    "audience_demographics": {
        "field": "report_date",
        "type": "MONTH",
    },
    "fill_rate": {
        "field": "date",
        "type": "DAY",
    },
    "geo_monthly": {
        "field": "report_date",
        "type": "MONTH",  # Monthly data with MONTH_YEAR dimension
    },
}

# Report Execution Schedule (Cloud Scheduler cron expressions)
SCHEDULES = {
    "inventory_daily": "0 6 * * *",        # Every day at 6 AM Paris time
    "fill_rate": "0 7 * * *",              # Every day at 7 AM Paris time
    "audience_interest": "0 8 1 * *",      # 1st of month at 8 AM Paris time
    "audience_demographics": "0 9 1 * *",  # 1st of month at 9 AM Paris time
    "geo_monthly": "0 10 1 * *",           # 1st of month at 10 AM Paris time
}
