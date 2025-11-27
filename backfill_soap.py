#!/usr/bin/env python3
"""
Backfill script for GAM historical data using SOAP API.
Uses SOAP API for flexibility in creating reports with custom date ranges.

Usage:
    python backfill_soap.py --start-date 2025-01-01 --end-date 2025-11-26
"""
import argparse
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
from google.cloud import bigquery
from googleads import ad_manager

# Add cloud_function to path for config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "cloud_function"))
from config import PROJECT_ID, DATASET_ID, SCHEMAS

# Configuration
GOOGLEADS_YAML_FILE = "backfill/googleads.yaml"


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


def run_report(gam_client, dimensions: list, columns: list, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Run a GAM report using SOAP API and return DataFrame."""
    report_service = gam_client.GetService("ReportService", version="v202502")

    # Build report job
    report_job = {
        "reportQuery": {
            "dimensions": dimensions,
            "columns": columns,
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
            "adUnitView": "HIERARCHICAL",  # Enable ad unit hierarchy columns
        }
    }

    # Run report job
    report_job = report_service.runReportJob(report_job)
    job_id = report_job['id']
    print(f"    Report job ID: {job_id}")

    # Wait for completion
    timeout = 600  # 10 minutes
    start_time = time.time()
    while time.time() - start_time < timeout:
        report_job_status = report_service.getReportJobStatus(job_id)
        if report_job_status == "COMPLETED":
            break
        elif report_job_status == "FAILED":
            raise Exception(f"Report job {job_id} failed")
        time.sleep(10)
        print(f"    Status: {report_job_status}...")

    if time.time() - start_time >= timeout:
        raise Exception(f"Report job {job_id} timed out after {timeout}s")

    print(f"    Report completed in {int(time.time() - start_time)}s")

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

    # DEBUG: Print actual columns returned
    print(f"    [DEBUG] Columns returned: {list(df.columns)}")

    return df


def get_inventory_daily_report(gam_client, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Get daily inventory report matching REST API schema."""
    dimensions = [
        "DATE",
        "AD_UNIT_NAME",   # With adUnitView=HIERARCHICAL, returns "Ad unit 1", "Ad unit 2", etc.
        "ORDER_NAME",
        "DEVICE_CATEGORY_NAME",
        "CREATIVE_SIZE",
    ]
    columns = [
        "AD_SERVER_IMPRESSIONS",
        "AD_SERVER_CLICKS",
        "AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS",
        "AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",
    ]

    df = run_report(gam_client, dimensions, columns, start_date, end_date)

    # Transform to match REST API schema
    # With adUnitView=HIERARCHICAL, AD_UNIT_NAME becomes "Ad unit 1", "Ad unit 2", etc.
    column_mapping = {
        "Dimension.DATE": "date",
        "Dimension.ORDER_NAME": "order_name",
        "Dimension.DEVICE_CATEGORY_NAME": "device_category",
        "Dimension.CREATIVE_SIZE": "creative_size",
        "Column.AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "Column.AD_SERVER_CLICKS": "ad_server_clicks",
        "Column.AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS": "active_view_measurable_impressions",
        "Column.AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS": "active_view_viewable_impressions",
    }

    # Rename hierarchical ad unit columns to temporary names
    # HIERARCHICAL mode returns "Ad unit 1", "Ad unit 2", etc.
    if "Ad unit 1" in df.columns:
        df = df.rename(columns={"Ad unit 1": "ad_unit_top_level"})
    if "Ad unit 2" in df.columns:
        df = df.rename(columns={"Ad unit 2": "ad_unit_name"})

    # Rename other columns
    df = df.rename(columns=column_mapping)

    # DEBUG: Print unique values
    if "ad_unit_name" in df.columns:
        print(f"    [DEBUG] Sample ad_unit_name values: {df['ad_unit_name'].unique()[:10]}")
    if "ad_unit_top_level" in df.columns:
        print(f"    [DEBUG] Sample ad_unit_top_level values: {df['ad_unit_top_level'].unique()[:10]}")

    # Keep only expected columns in correct order
    expected_columns = [
        "date", "ad_unit_top_level", "ad_unit_name", "order_name",
        "device_category", "creative_size", "ad_server_impressions", "ad_server_clicks",
        "active_view_measurable_impressions", "active_view_viewable_impressions"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Convert types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ad_server_impressions"] = pd.to_numeric(df["ad_server_impressions"], errors="coerce").astype("Int64")
    df["ad_server_clicks"] = pd.to_numeric(df["ad_server_clicks"], errors="coerce").astype("Int64")
    df["active_view_measurable_impressions"] = pd.to_numeric(df["active_view_measurable_impressions"], errors="coerce").astype("Int64")
    df["active_view_viewable_impressions"] = pd.to_numeric(df["active_view_viewable_impressions"], errors="coerce").astype("Int64")

    return df


def get_fill_rate_report(gam_client, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Get daily fill rate report matching REST API schema."""
    dimensions = [
        "DATE",
        "AD_UNIT_NAME",
    ]
    columns = [
        "UNFILLED_IMPRESSIONS",
        "CODE_SERVED_COUNT",
        "RESPONSES_SERVED",
        "AD_SERVER_IMPRESSIONS",
        "FILL_RATE",
        "AD_REQUESTS",
    ]

    df = run_report(gam_client, dimensions, columns, start_date, end_date)

    # Debug: print actual columns returned
    print(f"    Columns returned by GAM: {list(df.columns)}")

    # Transform to match REST API schema
    column_mapping = {
        "Dimension.DATE": "date",
        "Dimension.AD_UNIT_NAME": "ad_unit_name",
        "Column.UNFILLED_IMPRESSIONS": "unfilled_impressions",
        "Column.CODE_SERVED_COUNT": "code_served_count",
        "Column.RESPONSES_SERVED": "responses_served",
        "Column.AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "Column.FILL_RATE": "fill_rate",
        "Column.AD_REQUESTS": "ad_requests",
    }
    df = df.rename(columns=column_mapping)

    # Keep only expected columns that exist
    expected_columns = [
        "date", "ad_unit_name", "unfilled_impressions", "code_served_count",
        "responses_served", "ad_server_impressions", "fill_rate", "ad_requests"
    ]
    available_columns = [col for col in expected_columns if col in df.columns]
    df = df[available_columns]

    # Convert types for available columns
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "unfilled_impressions" in df.columns:
        df["unfilled_impressions"] = pd.to_numeric(df["unfilled_impressions"], errors="coerce").astype("Int64")
    if "code_served_count" in df.columns:
        df["code_served_count"] = pd.to_numeric(df["code_served_count"], errors="coerce").astype("Int64")
    if "responses_served" in df.columns:
        df["responses_served"] = pd.to_numeric(df["responses_served"], errors="coerce").astype("Int64")
    if "ad_server_impressions" in df.columns:
        df["ad_server_impressions"] = pd.to_numeric(df["ad_server_impressions"], errors="coerce").astype("Int64")
    if "fill_rate" in df.columns:
        df["fill_rate"] = pd.to_numeric(df["fill_rate"], errors="coerce").astype("Float64")
    if "ad_requests" in df.columns:
        df["ad_requests"] = pd.to_numeric(df["ad_requests"], errors="coerce").astype("Int64")

    return df


def get_geo_monthly_report(gam_client, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Get monthly geo report matching REST API schema."""
    dimensions = [
        "COUNTRY_NAME",
        "COUNTRY_CRITERIA_ID",  # Country code
    ]
    columns = [
        "AD_SERVER_IMPRESSIONS",
        "AD_SERVER_CLICKS",
        "AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_RATE",
        "AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",
    ]

    df = run_report(gam_client, dimensions, columns, start_date, end_date)

    # Transform to match REST API schema
    column_mapping = {
        "Dimension.COUNTRY_NAME": "country_name",
        "Dimension.COUNTRY_CRITERIA_ID": "country_code",
        "Column.AD_SERVER_IMPRESSIONS": "ad_server_impressions",
        "Column.AD_SERVER_CLICKS": "ad_server_clicks",
        "Column.AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_RATE": "active_view_measurable_rate",
        "Column.AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS": "active_view_viewable_impressions",
    }
    df = df.rename(columns=column_mapping)

    # Keep only expected columns
    expected_columns = [
        "country_name", "country_code", "ad_server_impressions", "ad_server_clicks",
        "active_view_measurable_rate", "active_view_viewable_impressions"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Add report_date (first day of the month)
    report_date = start_date.replace(day=1)
    df["report_date"] = report_date.date()

    # Filter out rows with NULL country_name (GAM returns aggregate rows with NULL)
    df = df[df["country_name"].notna()]

    # Convert types
    df["country_code"] = df["country_code"].astype(str)  # Convert to string for BigQuery
    df["ad_server_impressions"] = pd.to_numeric(df["ad_server_impressions"], errors="coerce").astype("Int64")
    df["ad_server_clicks"] = pd.to_numeric(df["ad_server_clicks"], errors="coerce").astype("Int64")
    df["active_view_measurable_rate"] = pd.to_numeric(df["active_view_measurable_rate"], errors="coerce").astype("Float64")
    df["active_view_viewable_impressions"] = pd.to_numeric(df["active_view_viewable_impressions"], errors="coerce").astype("Int64")

    return df


def ensure_table_exists(bq_client, table_name: str):
    """Ensure BigQuery table exists with correct schema."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Map table names to schema keys
    schema_key_map = {
        "inventory_daily": "inventory_daily",
        "fill_rate_daily": "fill_rate",
        "geo_monthly": "geo_monthly",
    }

    try:
        bq_client.get_table(table_id)
        print(f"  Table {table_name} already exists")
    except Exception:
        print(f"  Creating table {table_name}...")
        schema_key = schema_key_map.get(table_name, table_name)
        schema = SCHEMAS[schema_key]
        table = bigquery.Table(table_id, schema=schema)

        # Set partitioning
        if "daily" in table_name:
            table.time_partitioning = bigquery.TimePartitioning(field="date")
        elif "monthly" in table_name:
            table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="report_date")

        table = bq_client.create_table(table)
        print(f"  Created table {table_name}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Backfill GAM historical data using SOAP API")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--reports", nargs="+", default=["inventory_daily", "fill_rate", "geo_monthly"],
                       help="Reports to backfill (default: inventory_daily fill_rate geo_monthly)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - don't write to BigQuery")

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date) if args.end_date else datetime.now()
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        sys.exit(1)

    if start_date > end_date:
        print("Error: start_date must be before end_date")
        sys.exit(1)

    print("=" * 80)
    print("GAM Historical Data Backfill (SOAP API)")
    print("=" * 80)
    print(f"Project ID: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Period: {start_date.date()} to {end_date.date()}")
    print(f"Reports: {', '.join(args.reports)}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'PRODUCTION'}")
    print("=" * 80)

    # Initialize GAM client from local file
    print("\nInitializing GAM SOAP client...")
    if not os.path.exists(GOOGLEADS_YAML_FILE):
        print(f"Error: {GOOGLEADS_YAML_FILE} not found")
        sys.exit(1)

    try:
        gam_client = ad_manager.AdManagerClient.LoadFromStorage(GOOGLEADS_YAML_FILE)
        print("OK GAM SOAP client initialized")
    except Exception as e:
        print(f"Error initializing GAM client: {e}")
        sys.exit(1)

    # Initialize BigQuery client
    print("Initializing BigQuery client...")
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        print("OK BigQuery client initialized")
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        sys.exit(1)

    # Backfill each report
    for report_name in args.reports:
        if report_name == "inventory_daily":
            backfill_inventory_daily(gam_client, bq_client, start_date, end_date, args.dry_run)
        elif report_name == "fill_rate":
            backfill_fill_rate(gam_client, bq_client, start_date, end_date, args.dry_run)
        elif report_name == "geo_monthly":
            backfill_geo_monthly(gam_client, bq_client, start_date, end_date, args.dry_run)
        else:
            print(f"\nWarning: Unknown report '{report_name}', skipping")


def backfill_inventory_daily(gam_client, bq_client, start_date: datetime, end_date: datetime, dry_run: bool):
    """Backfill inventory daily report."""
    print(f"\n{'=' * 80}")
    print("Backfilling Inventory Daily Report")
    print('=' * 80)

    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            print("  Fetching data from GAM SOAP API...")
            df = get_inventory_daily_report(gam_client, month_start, month_end)

            if df.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df)} rows")

            if not dry_run:
                ensure_table_exists(bq_client, "inventory_daily")

                # Delete existing data for this period to avoid duplicates
                table_id = f"{PROJECT_ID}.{DATASET_ID}.inventory_daily"
                delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE DATE(date) >= '{month_start.date()}'
                    AND DATE(date) <= '{month_end.date()}'
                """
                print("  Deleting existing data for this period...")
                delete_job = bq_client.query(delete_query)
                delete_job.result()

                # Insert into BigQuery
                print("  Inserting into BigQuery...")
                job = bq_client.load_table_from_dataframe(df, table_id)
                job.result()

                print(f"  OK Inserted {len(df)} rows")
            else:
                print(f"  [DRY RUN] Would insert {len(df)} rows")
                print(f"  Sample:\n{df.head(3)}")

            total_rows += len(df)

        except Exception as e:
            print(f"  X Error processing month: {e}")
            import traceback
            traceback.print_exc()
            raise

    print(f"\n{'=' * 80}")
    print(f"Inventory Daily Complete: {total_rows} total rows")
    print('=' * 80)


def backfill_fill_rate(gam_client, bq_client, start_date: datetime, end_date: datetime, dry_run: bool):
    """Backfill fill rate report."""
    print(f"\n{'=' * 80}")
    print("Backfilling Fill Rate Report")
    print('=' * 80)

    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            print("  Fetching data from GAM SOAP API...")
            df = get_fill_rate_report(gam_client, month_start, month_end)

            if df.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df)} rows")

            if not dry_run:
                ensure_table_exists(bq_client, "fill_rate_daily")

                # Delete existing data for this period
                table_id = f"{PROJECT_ID}.{DATASET_ID}.fill_rate_daily"
                delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE DATE(date) >= '{month_start.date()}'
                    AND DATE(date) <= '{month_end.date()}'
                """
                print("  Deleting existing data for this period...")
                delete_job = bq_client.query(delete_query)
                delete_job.result()

                # Insert into BigQuery
                print("  Inserting into BigQuery...")
                job = bq_client.load_table_from_dataframe(df, table_id)
                job.result()

                print(f"  OK Inserted {len(df)} rows")
            else:
                print(f"  [DRY RUN] Would insert {len(df)} rows")
                print(f"  Sample:\n{df.head(3)}")

            total_rows += len(df)

        except Exception as e:
            print(f"  X Error processing month: {e}")
            import traceback
            traceback.print_exc()
            raise

    print(f"\n{'=' * 80}")
    print(f"Fill Rate Complete: {total_rows} total rows")
    print('=' * 80)


def backfill_geo_monthly(gam_client, bq_client, start_date: datetime, end_date: datetime, dry_run: bool):
    """Backfill geo monthly report."""
    print(f"\n{'=' * 80}")
    print("Backfilling Geo Monthly Report")
    print('=' * 80)

    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            print("  Fetching data from GAM SOAP API...")
            df_geo = get_geo_monthly_report(gam_client, month_start, month_end)

            if df_geo.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df_geo)} rows")

            if not dry_run:
                ensure_table_exists(bq_client, "geo_monthly")

                # Delete existing data for this period
                table_id = f"{PROJECT_ID}.{DATASET_ID}.geo_monthly"
                report_date = month_start.replace(day=1).date()
                delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE DATE(report_date) = '{report_date}'
                """
                print("  Deleting existing data for this period...")
                delete_job = bq_client.query(delete_query)
                delete_job.result()

                # Insert into BigQuery
                print("  Inserting into BigQuery...")
                job_geo = bq_client.load_table_from_dataframe(df_geo, table_id)
                job_geo.result()

                print(f"  OK Inserted {len(df_geo)} rows")
            else:
                print(f"  [DRY RUN] Would insert {len(df_geo)} rows")
                print(f"  Sample:\n{df_geo.head(3)}")

            total_rows += len(df_geo)

        except Exception as e:
            print(f"  X Error processing month: {e}")
            import traceback
            traceback.print_exc()
            raise

    print(f"\n{'=' * 80}")
    print(f"Geo Monthly Complete: {total_rows} total rows")
    print('=' * 80)


if __name__ == "__main__":
    main()
