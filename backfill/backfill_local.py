#!/usr/bin/env python3
"""
Backfill script for GAM historical data using local configuration.

Usage:
    python backfill_local.py --project-id YOUR_PROJECT_ID \
        --start-date 2025-01-01 --end-date 2025-11-22
"""
import argparse
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Tuple
from googleads import ad_manager
import pandas as pd
import time
from google.cloud import bigquery

# Configuration
GOOGLEADS_YAML_FILE = "googleads.yaml"


def parse_date(date_string: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_string, "%Y-%m-%d")


def get_month_ranges(start_date: datetime, end_date: datetime) -> list:
    """Generate list of (start, end) tuples for each month in range."""
    ranges = []
    current = start_date

    while current <= end_date:
        month_start = current.replace(day=1)
        next_month = month_start + relativedelta(months=1)
        month_end = next_month - timedelta(days=1)

        if month_start < start_date:
            month_start = start_date
        if month_end > end_date:
            month_end = end_date

        ranges.append((month_start, month_end))
        current = next_month

    return ranges


def get_inventory_daily_report(gam_client, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Get daily inventory report from GAM."""
    report_service = gam_client.GetService("ReportService", version="v202502")

    # Build report job
    report_job = {
        "reportQuery": {
            "dimensions": [
                "DATE",
                "AD_UNIT_ID",
                "AD_UNIT_NAME",
                "ORDER_ID",
                "ORDER_NAME",
                "DEVICE_CATEGORY_NAME",
                "CREATIVE_SIZE",
            ],
            "columns": [
                "AD_SERVER_IMPRESSIONS",
                "AD_SERVER_CLICKS",
            ],
            "dateRangeType": "CUSTOM_DATE",
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day,
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            },
        }
    }

    # Run report job
    report_job = report_service.runReportJob(report_job)
    job_id = report_job['id']

    # Wait for completion
    timeout = 300
    start_time = time.time()
    while time.time() - start_time < timeout:
        report_job_status = report_service.getReportJobStatus(job_id)
        if report_job_status == "COMPLETED":
            break
        elif report_job_status == "FAILED":
            raise Exception(f"Report job {job_id} failed")
        time.sleep(10)

    # Download report
    import io
    import requests
    report_download_options = {
        "exportFormat": "CSV_DUMP",
        "includeReportProperties": False,
        "includeTotalsRow": False,
        "useGzipCompression": False,
    }
    report_url = report_service.getReportDownloadUrlWithOptions(job_id, report_download_options)
    response = requests.get(report_url)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))

    # Transform column names
    column_mapping = {
        "Dimension.DATE": "date",
        "Dimension.AD_UNIT_ID": "ad_unit_id",
        "Dimension.AD_UNIT_NAME": "ad_unit_name",
        "Dimension.ORDER_ID": "order_id",
        "Dimension.ORDER_NAME": "order_name",
        "Dimension.DEVICE_CATEGORY_NAME": "device_category",
        "Dimension.CREATIVE_SIZE": "creative_size",
        "Column.AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "Column.AD_SERVER_CLICKS": "ad_server_clicks",
    }
    df = df.rename(columns=column_mapping)

    # Keep only the columns we want (drop any extra columns from GAM)
    expected_columns = [
        "date", "ad_unit_id", "ad_unit_name", "order_id", "order_name",
        "device_category", "creative_size", "ad_server_impressions", "ad_server_clicks"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Convert types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ad_unit_id"] = pd.to_numeric(df["ad_unit_id"], errors="coerce").astype("Int64")
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").astype("Int64")
    df["ad_server_impressions"] = pd.to_numeric(df["ad_server_impressions"], errors="coerce").astype("Int64")
    df["ad_server_clicks"] = pd.to_numeric(df["ad_server_clicks"], errors="coerce").astype("Int64")

    return df


def get_geo_monthly_report(gam_client, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Get monthly geo report from GAM."""
    report_service = gam_client.GetService("ReportService", version="v202502")

    # Build report job
    report_job = {
        "reportQuery": {
            "dimensions": [
                "COUNTRY_CRITERIA_ID",
                "COUNTRY_NAME",
            ],
            "columns": [
                "AD_SERVER_IMPRESSIONS",
                "AD_SERVER_CLICKS",
            ],
            "dateRangeType": "CUSTOM_DATE",
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day,
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            },
        }
    }

    # Run report job
    report_job = report_service.runReportJob(report_job)
    job_id = report_job['id']

    # Wait for completion
    timeout = 300
    start_time = time.time()
    while time.time() - start_time < timeout:
        report_job_status = report_service.getReportJobStatus(job_id)
        if report_job_status == "COMPLETED":
            break
        elif report_job_status == "FAILED":
            raise Exception(f"Report job {job_id} failed")
        time.sleep(10)

    # Download report
    import io
    import requests
    report_download_options = {
        "exportFormat": "CSV_DUMP",
        "includeReportProperties": False,
        "includeTotalsRow": False,
        "useGzipCompression": False,
    }
    report_url = report_service.getReportDownloadUrlWithOptions(job_id, report_download_options)
    response = requests.get(report_url)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))

    # Transform column names
    column_mapping = {
        "Dimension.COUNTRY_CRITERIA_ID": "country_id",
        "Dimension.COUNTRY_NAME": "country_name",
        "Column.AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "Column.AD_SERVER_CLICKS": "ad_server_clicks",
    }
    df = df.rename(columns=column_mapping)

    # Keep only the columns we want (drop any extra columns from GAM)
    expected_columns = [
        "country_id", "country_name", "ad_server_impressions", "ad_server_clicks"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Convert types
    df["country_id"] = pd.to_numeric(df["country_id"], errors="coerce").astype("Int64")
    df["ad_server_impressions"] = pd.to_numeric(df["ad_server_impressions"], errors="coerce").astype("Int64")
    df["ad_server_clicks"] = pd.to_numeric(df["ad_server_clicks"], errors="coerce").astype("Int64")

    # Add report_date (first day of the month)
    report_date = start_date.replace(day=1)
    df["report_date"] = report_date.date()

    return df


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Backfill GAM historical data into BigQuery")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dataset-id", default="gam_data", help="BigQuery dataset ID")

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        sys.exit(1)

    if start_date > end_date:
        print("Error: start_date must be before end_date")
        sys.exit(1)

    print("=" * 60)
    print("GAM Historical Data Backfill (Local)")
    print("=" * 60)
    print(f"Project ID: {args.project_id}")
    print(f"Dataset: {args.dataset_id}")
    print(f"Period: {start_date.date()} to {end_date.date()}")
    print("=" * 60)

    # Initialize GAM client from local file
    print("\nInitializing GAM client...")
    if not os.path.exists(GOOGLEADS_YAML_FILE):
        print(f"Error: {GOOGLEADS_YAML_FILE} not found")
        sys.exit(1)

    try:
        gam_client = ad_manager.AdManagerClient.LoadFromStorage(GOOGLEADS_YAML_FILE)
        print("OK GAM client initialized")
    except Exception as e:
        print(f"Error initializing GAM client: {e}")
        sys.exit(1)

    # Initialize BigQuery client
    print("Initializing BigQuery client...")
    try:
        bq_client = bigquery.Client(project=args.project_id)
        print("OK BigQuery client initialized")
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        sys.exit(1)

    # Backfill inventory report
    print(f"\n=== Backfilling Inventory Report ===")
    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            print("  Fetching data from GAM API...")
            df = get_inventory_daily_report(gam_client, month_start, month_end)

            if df.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df)} rows")

            # Insert into BigQuery
            print("  Inserting into BigQuery...")
            table_id = f"{args.project_id}.{args.dataset_id}.report_inventory_daily"
            job = bq_client.load_table_from_dataframe(df, table_id)
            job.result()

            print(f"  OK Inserted {len(df)} rows")
            total_rows += len(df)

        except Exception as e:
            print(f"  X Error processing month: {e}")
            raise

    print(f"\n=== Inventory Report Complete ===")
    print(f"Total rows inserted: {total_rows}")
    print("=" * 60)

    # Backfill geo monthly report
    print(f"\n=== Backfilling Geo Monthly Report ===")
    total_geo_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            print("  Fetching data from GAM API...")
            df_geo = get_geo_monthly_report(gam_client, month_start, month_end)

            if df_geo.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df_geo)} rows")

            # Insert into BigQuery
            print("  Inserting into BigQuery...")
            table_id_geo = f"{args.project_id}.{args.dataset_id}.report_geo_monthly"
            job_geo = bq_client.load_table_from_dataframe(df_geo, table_id_geo)
            job_geo.result()

            print(f"  OK Inserted {len(df_geo)} rows")
            total_geo_rows += len(df_geo)

        except Exception as e:
            print(f"  X Error processing month: {e}")
            raise

    print(f"\n=== Geo Monthly Report Complete ===")
    print(f"Total rows inserted: {total_geo_rows}")
    print("=" * 60)


if __name__ == "__main__":
    main()
