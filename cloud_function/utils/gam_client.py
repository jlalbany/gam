"""Google Ad Manager API client wrapper."""
import io
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml
from googleads import ad_manager
from google.cloud import secretmanager


class GAMReportClient:
    """Client for Google Ad Manager Reporting API."""

    def __init__(self, project_id: str, secret_name: str = "gam_api_config"):
        """
        Initialize GAM client with credentials from Secret Manager.

        Args:
            project_id: GCP project ID
            secret_name: Name of the secret containing GAM credentials
        """
        self.project_id = project_id
        self.secret_name = secret_name
        self.client = self._initialize_client()

    def _initialize_client(self):
        """
        Initialize AdManager client from Secret Manager credentials.

        Returns:
            Initialized AdManagerClient
        """
        import os
        import json

        # Fetch credentials from Secret Manager
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_path})
        credentials_yaml = response.payload.data.decode("UTF-8")

        # Parse YAML and create client
        credentials_dict = yaml.safe_load(credentials_yaml)

        # Extract service account key if present and create key file
        sa_key_path = None
        if 'service_account_key' in credentials_dict:
            sa_key_data = credentials_dict['service_account_key']

            # If it's a string, parse it as JSON
            if isinstance(sa_key_data, str):
                sa_key_data = json.loads(sa_key_data)

            # Create temporary file for service account key
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as sa_key_file:
                json.dump(sa_key_data, sa_key_file)
                sa_key_path = sa_key_file.name

            # Update path in config
            if 'ad_manager' in credentials_dict:
                credentials_dict['ad_manager']['path_to_private_key_file'] = sa_key_path

            # Remove the embedded key from dict
            del credentials_dict['service_account_key']

        # For Cloud Functions, remove empty path_to_private_key_file
        if 'ad_manager' in credentials_dict:
            if 'path_to_private_key_file' in credentials_dict['ad_manager']:
                if not credentials_dict['ad_manager']['path_to_private_key_file']:
                    del credentials_dict['ad_manager']['path_to_private_key_file']

        # Create temporary file for googleads YAML config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            yaml.dump(credentials_dict, temp_file)
            temp_path = temp_file.name

        try:
            client = ad_manager.AdManagerClient.LoadFromStorage(temp_path)
            return client
        finally:
            # Clean up temp files
            os.unlink(temp_path)
            if sa_key_path and os.path.exists(sa_key_path):
                os.unlink(sa_key_path)

    def create_report_job(
        self,
        dimensions: List[str],
        columns: List[str],
        date_range_type: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """
        Create a report job in GAM.

        Args:
            dimensions: List of dimension names
            columns: List of column (metric) names
            date_range_type: Type of date range (YESTERDAY, LAST_MONTH, CUSTOM_DATE, etc.)
            start_date: Start date for CUSTOM_DATE range
            end_date: End date for CUSTOM_DATE range

        Returns:
            Report job ID
        """
        report_service = self.client.GetService("ReportService", version="v202502")

        # Build report job
        report_job = {
            "reportQuery": {
                "dimensions": dimensions,
                "columns": columns,
                "dateRangeType": date_range_type,
            }
        }

        # Add custom date range if specified
        if date_range_type == "CUSTOM_DATE" and start_date and end_date:
            report_job["reportQuery"]["startDate"] = {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day,
            }
            report_job["reportQuery"]["endDate"] = {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day,
            }

        # Run report job
        report_job = report_service.runReportJob(report_job)
        return report_job['id']

    def wait_for_report(self, report_job_id: int, timeout: int = 300) -> bool:
        """
        Wait for report job to complete.

        Args:
            report_job_id: Report job ID
            timeout: Maximum time to wait in seconds

        Returns:
            True if completed successfully, False otherwise
        """
        report_service = self.client.GetService("ReportService", version="v202502")

        start_time = time.time()
        while time.time() - start_time < timeout:
            report_job_status = report_service.getReportJobStatus(report_job_id)

            if report_job_status == "COMPLETED":
                return True
            elif report_job_status == "FAILED":
                raise Exception(f"Report job {report_job_id} failed")

            time.sleep(10)  # Wait 10 seconds before checking again

        raise TimeoutError(f"Report job {report_job_id} timed out after {timeout} seconds")

    def download_report(self, report_job_id: int, export_format: str = "CSV_DUMP") -> pd.DataFrame:
        """
        Download report data and return as DataFrame.

        Args:
            report_job_id: Report job ID
            export_format: Export format (CSV_DUMP recommended)

        Returns:
            DataFrame containing report data
        """
        report_service = self.client.GetService("ReportService", version="v202502")

        # Get report download URL
        report_download_options = {
            "exportFormat": export_format,
            "includeReportProperties": False,
            "includeTotalsRow": False,
            "useGzipCompression": False,
        }

        report_url = report_service.getReportDownloadUrlWithOptions(
            report_job_id, report_download_options
        )

        # Download and parse CSV
        import requests
        response = requests.get(report_url)
        response.raise_for_status()

        # Parse CSV into DataFrame
        df = pd.read_csv(io.StringIO(response.text))

        return df

    def get_inventory_daily_report(
        self,
        date_range_type: str = "YESTERDAY",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get daily inventory report.

        Args:
            date_range_type: Type of date range
            start_date: Start date for CUSTOM_DATE
            end_date: End date for CUSTOM_DATE

        Returns:
            DataFrame with inventory data
        """
        dimensions = [
            "DATE",
            "AD_UNIT_ID",
            "AD_UNIT_NAME",
            "ORDER_ID",
            "ORDER_NAME",
            "DEVICE_CATEGORY_NAME",
            "CREATIVE_SIZE",
        ]

        columns = [
            "AD_SERVER_IMPRESSIONS",
            "AD_SERVER_CLICKS",
        ]

        # Create and run report
        job_id = self.create_report_job(dimensions, columns, date_range_type, start_date, end_date)
        self.wait_for_report(job_id)
        df = self.download_report(job_id)

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

    def get_geo_monthly_report(
        self,
        date_range_type: str = "LAST_MONTH",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        report_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get monthly geo report.

        Args:
            date_range_type: Type of date range
            start_date: Start date for CUSTOM_DATE
            end_date: End date for CUSTOM_DATE
            report_date: Date to use as report_date (defaults to calculated from LAST_MONTH)

        Returns:
            DataFrame with geo data
        """
        dimensions = [
            "COUNTRY_CRITERIA_ID",
            "COUNTRY_NAME",
        ]

        columns = [
            "AD_SERVER_IMPRESSIONS",
            "AD_SERVER_CLICKS",
        ]

        # Create and run report
        job_id = self.create_report_job(dimensions, columns, date_range_type, start_date, end_date)
        self.wait_for_report(job_id)
        df = self.download_report(job_id)

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

        # Calculate report_date
        if report_date is None:
            # Default: Calculate from LAST_MONTH logic
            # Subtract 1 day from today to get into previous month, then force to 1st
            today = datetime.now()
            previous_month = today.replace(day=1) - timedelta(days=1)
            report_date = previous_month.replace(day=1)

        df["report_date"] = report_date.date()

        return df
