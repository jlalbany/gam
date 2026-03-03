#!/usr/bin/env python3
"""
Backfill script for GAM historical data.

This script retrieves historical GAM data and loads it into BigQuery.
It processes data month by month to avoid memory issues and API limitations.

Usage:
    python backfill_gam_reports.py --project-id YOUR_PROJECT_ID \
        --start-date 2023-01-01 --end-date 2024-02-29

Requirements:
    - GCP credentials configured (via gcloud or GOOGLE_APPLICATION_CREDENTIALS)
    - Access to Secret Manager (gam_api_config secret)
    - BigQuery write permissions
"""
import argparse
import sys
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Tuple

# Add parent directory to path for imports
sys.path.insert(0, '../cloud_function')

from utils.gam_client import GAMReportClient
from utils.bigquery_client import BigQueryClient


def parse_date(date_string: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_string, "%Y-%m-%d")


def get_month_ranges(start_date: datetime, end_date: datetime) -> list[Tuple[datetime, datetime]]:
    """
    Generate list of (start, end) tuples for each month in range.

    Args:
        start_date: Overall start date
        end_date: Overall end date

    Returns:
        List of (month_start, month_end) tuples
    """
    ranges = []
    current = start_date

    while current <= end_date:
        # Start of current month
        month_start = current.replace(day=1)

        # End of current month (last day)
        next_month = month_start + relativedelta(months=1)
        month_end = next_month - timedelta(days=1)

        # Clip to overall range
        if month_start < start_date:
            month_start = start_date
        if month_end > end_date:
            month_end = end_date

        ranges.append((month_start, month_end))

        # Move to next month
        current = next_month

    return ranges


def backfill_inventory_report(
    gam_client: GAMReportClient,
    bq_client: BigQueryClient,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """
    Backfill inventory daily report month by month.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance
        start_date: Start date
        end_date: End date
    """
    print(f"\n=== Backfilling Inventory Report ===")
    print(f"Period: {start_date.date()} to {end_date.date()}")

    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        try:
            # Fetch data from GAM
            print("  Fetching data from GAM API...")
            df = gam_client.get_inventory_daily_report(
                date_range_type="CUSTOM_DATE",
                start_date=month_start,
                end_date=month_end,
            )

            if df.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df)} rows")

            # Insert into BigQuery
            print("  Inserting into BigQuery...")
            rows_inserted = bq_client.insert_dataframe(
                df=df,
                table_id="report_inventory_daily",
                write_disposition="WRITE_APPEND",
            )

            print(f"  ✓ Inserted {rows_inserted} rows")
            total_rows += rows_inserted

        except Exception as e:
            print(f"  ✗ Error processing month: {e}")
            raise

    print(f"\n=== Inventory Report Complete ===")
    print(f"Total rows inserted: {total_rows}")


def backfill_geo_report(
    gam_client: GAMReportClient,
    bq_client: BigQueryClient,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """
    Backfill geo monthly report month by month.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance
        start_date: Start date
        end_date: End date
    """
    print(f"\n=== Backfilling Geo Monthly Report ===")
    print(f"Period: {start_date.date()} to {end_date.date()}")

    month_ranges = get_month_ranges(start_date, end_date)
    total_rows = 0

    for i, (month_start, month_end) in enumerate(month_ranges, 1):
        print(f"\n[{i}/{len(month_ranges)}] Processing {month_start.strftime('%B %Y')}")
        print(f"  Date range: {month_start.date()} to {month_end.date()}")

        # Calculate report_date (first day of the month)
        report_date = month_start.replace(day=1)

        try:
            # Fetch data from GAM
            print("  Fetching data from GAM API...")
            df = gam_client.get_geo_monthly_report(
                date_range_type="CUSTOM_DATE",
                start_date=month_start,
                end_date=month_end,
                report_date=report_date,
            )

            if df.empty:
                print("  No data returned for this period")
                continue

            print(f"  Retrieved {len(df)} rows")

            # Insert into BigQuery
            print("  Inserting into BigQuery...")
            rows_inserted = bq_client.insert_dataframe(
                df=df,
                table_id="report_geo_monthly",
                write_disposition="WRITE_APPEND",
            )

            print(f"  ✓ Inserted {rows_inserted} rows")
            total_rows += rows_inserted

        except Exception as e:
            print(f"  ✗ Error processing month: {e}")
            raise

    print(f"\n=== Geo Monthly Report Complete ===")
    print(f"Total rows inserted: {total_rows}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill GAM historical data into BigQuery"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--secret-name",
        default="gam_api_config",
        help="Secret Manager secret name (default: gam_api_config)",
    )
    parser.add_argument(
        "--dataset-id",
        default="gam_data",
        help="BigQuery dataset ID (default: gam_data)",
    )
    parser.add_argument(
        "--reports",
        nargs="+",
        choices=["inventory", "geo", "all"],
        default=["all"],
        help="Which reports to backfill (default: all)",
    )

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
    print("GAM Historical Data Backfill")
    print("=" * 60)
    print(f"Project ID: {args.project_id}")
    print(f"Dataset: {args.dataset_id}")
    print(f"Period: {start_date.date()} to {end_date.date()}")
    print(f"Reports: {', '.join(args.reports)}")
    print("=" * 60)

    # Initialize clients
    print("\nInitializing clients...")
    try:
        gam_client = GAMReportClient(
            project_id=args.project_id,
            secret_name=args.secret_name,
        )
        bq_client = BigQueryClient(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
        )
        print("✓ Clients initialized successfully")
    except Exception as e:
        print(f"✗ Error initializing clients: {e}")
        sys.exit(1)

    # Determine which reports to run
    reports_to_run = set(args.reports)
    if "all" in reports_to_run:
        reports_to_run = {"inventory", "geo"}

    try:
        # Run backfill for each report
        if "inventory" in reports_to_run:
            backfill_inventory_report(gam_client, bq_client, start_date, end_date)

        if "geo" in reports_to_run:
            backfill_geo_report(gam_client, bq_client, start_date, end_date)

        print("\n" + "=" * 60)
        print("✓ Backfill completed successfully!")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ Backfill failed: {e}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
