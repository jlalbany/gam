"""Google Ad Manager REST API client wrapper."""
import os
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import pandas as pd

from google.ads import admanager_v1
from google.ads.admanager_v1.types import ReportDefinition
from google.type import date_pb2
from google.cloud import secretmanager

from config import (
    REPORT_IDS,
    COLUMN_MAPPINGS,
    TYPE_CONVERSIONS,
)


class GAMRestClient:
    """Client for Google Ad Manager REST API."""

    def __init__(self, project_id: str, network_code: str):
        """
        Initialize GAM REST client.

        Args:
            project_id: GCP project ID
            network_code: GAM network code
        """
        self.project_id = project_id
        self.network_code = network_code

        # Initialize Report Service client
        # Uses Application Default Credentials (ADC):
        # - In Cloud Functions: Uses the service account attached to the function
        # - Locally: Uses GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default login
        self.report_client = admanager_v1.ReportServiceClient()
        self.parent = f"networks/{network_code}"

    @staticmethod
    def convert_month_year(value: int) -> date:
        """
        Convert GAM MONTH_YEAR format to first day of month.

        GAM uses format: (year - 2010) * 100 + month_0indexed
        Where month_0indexed is 0=January, 1=February, ..., 11=December

        Args:
            value: Integer in GAM format (e.g., 1510 for November 2025)

        Returns:
            Date object for first day of that month

        Example:
            1510 → 2025-11-01 (15 years since 2010 + month 10 (0-indexed) = November)
            1509 → 2025-10-01 (15 years since 2010 + month 9 (0-indexed) = October)
        """
        year_offset = value // 100  # 1510 // 100 = 15
        month_0indexed = value % 100  # 1510 % 100 = 10

        # GAM format: years since 2010, months are 0-indexed
        year = 2010 + year_offset
        month = month_0indexed + 1  # Convert 0-indexed to 1-indexed (10 → 11 for November)

        return date(year, month, 1)

    @staticmethod
    def convert_date_int(value: int) -> date:
        """
        Convert YYYYMMDD format to date.

        Args:
            value: Integer in YYYYMMDD format (e.g., 20251125)

        Returns:
            Date object

        Example:
            20251125 → 2025-11-25
        """
        year = value // 10000
        month = (value % 10000) // 100
        day = value % 100
        return date(year, month, day)

    def list_reports(self) -> List[Dict]:
        """
        List all saved reports in the network.

        Returns:
            List of report dictionaries with id, name, and visibility
        """
        request = admanager_v1.ListReportsRequest(parent=self.parent)

        reports = []
        for report in self.report_client.list_reports(request=request):
            report_id = report.name.split('/')[-1]
            reports.append({
                'id': report_id,
                'name': report.display_name,
                'visibility': report.visibility,
                'full_name': report.name,
            })

        return reports

    def run_report_by_id(
        self,
        report_id: str,
    ) -> pd.DataFrame:
        """
        Run an existing report by its ID.

        Note: The report uses the date range configured in GAM UI.
        For dynamic dates, use reports with relative date ranges (Yesterday, Last month, etc.)

        Args:
            report_id: The ID of the report (e.g., "12345678")

        Returns:
            DataFrame with report data
        """
        report_name = f"{self.parent}/reports/{report_id}"

        # Run the report
        run_request = admanager_v1.RunReportRequest(name=report_name)
        operation = self.report_client.run_report(request=run_request)

        # Wait for the report to complete
        print(f"Running report {report_id}...")
        run_result = operation.result(timeout=600)  # 10 minutes timeout

        # The result contains the report result path (e.g., "networks/.../reports/.../results/...")
        result_name = run_result.report_result

        # Fetch report data
        return self._fetch_report_data(result_name, report_id)

    def create_and_run_report(
        self,
        dimensions: List[str],
        metrics: List[str],
        start_date: datetime,
        end_date: datetime,
        report_type: str = "HISTORICAL",
        time_zone: str = "Europe/Paris",
    ) -> pd.DataFrame:
        """
        Create and run a report, then fetch the results.

        Args:
            dimensions: List of dimension names (e.g., ["DATE", "AD_UNIT_NAME"])
            metrics: List of metric names (e.g., ["TOTAL_IMPRESSIONS", "TOTAL_CLICKS"])
            start_date: Start date for the report
            end_date: End date for the report
            report_type: Type of report (HISTORICAL, REACH, etc.)
            time_zone: Time zone for the report

        Returns:
            DataFrame with report data
        """
        # Create report definition
        report_definition = ReportDefinition(
            report_type=report_type,
            dimensions=dimensions,
            metrics=metrics,
            date_range=ReportDefinition.DateRange(
                fixed=ReportDefinition.DateRange.FixedDateRange(
                    start_date=date_pb2.Date(
                        year=start_date.year,
                        month=start_date.month,
                        day=start_date.day,
                    ),
                    end_date=date_pb2.Date(
                        year=end_date.year,
                        month=end_date.month,
                        day=end_date.day,
                    ),
                )
            ),
            time_zone=time_zone,
        )

        # Create report request
        create_request = admanager_v1.CreateReportRequest(
            parent=self.parent,
            report=admanager_v1.Report(
                display_name=f"Report {datetime.now().isoformat()}",
                report_definition=report_definition,
                visibility="HIDDEN",  # Don't save in UI
            ),
        )

        # Create the report
        report = self.report_client.create_report(request=create_request)
        report_name = report.name

        # Run the report
        run_request = admanager_v1.RunReportRequest(name=report_name)
        operation = self.report_client.run_report(request=run_request)

        # Wait for the report to complete
        print(f"Waiting for report {report_name} to complete...")
        result = operation.result(timeout=600)  # 10 minutes timeout

        # Fetch report data
        return self._fetch_report_data(report_name)

    def _fetch_report_data(self, result_name: str, report_id: str) -> pd.DataFrame:
        """
        Fetch report data and convert to DataFrame.

        Args:
            result_name: Full result name path (networks/.../reports/.../results/...)
            report_id: Report ID to get column definitions

        Returns:
            DataFrame with report data
        """
        # Get report definition to understand column structure
        report_name = f"{self.parent}/reports/{report_id}"
        get_request = admanager_v1.GetReportRequest(name=report_name)
        report = self.report_client.get_report(request=get_request)

        # Extract dimension and metric names from the enum values
        # The API returns enums like <Dimension.DATE: 1>, we need the string name
        dimension_names = [dim.name for dim in report.report_definition.dimensions]
        metric_names = [metric.name for metric in report.report_definition.metrics]

        # Combine to create ordered column names
        headers = dimension_names + metric_names

        # Fetch report rows using the result path
        fetch_request = admanager_v1.FetchReportResultRowsRequest(
            name=result_name,
        )

        rows_data = []

        # Stream the report pages
        for page in self.report_client.fetch_report_result_rows(request=fetch_request).pages:
            # Process rows in this page
            for row in page.rows:
                row_dict = {}

                # Extract dimension values
                for i, dim_name in enumerate(dimension_names):
                    if i < len(row.dimension_values):
                        dim_value = row.dimension_values[i]
                        # ReportValue has multiple fields but only one is set
                        # Check in order: string > int
                        if dim_value.string_value:  # Non-empty string
                            row_dict[dim_name] = dim_value.string_value
                        else:
                            row_dict[dim_name] = dim_value.int_value

                # Extract metric values from primary_values
                if len(row.metric_value_groups) > 0:
                    primary_values = row.metric_value_groups[0].primary_values
                    for i, metric_name in enumerate(metric_names):
                        if i < len(primary_values):
                            metric_value = primary_values[i]
                            # ReportValue has int_value and double_value
                            # Only one is set to non-default value
                            # Check double first to preserve decimal precision
                            if metric_value.double_value != 0.0:
                                row_dict[metric_name] = metric_value.double_value
                            else:
                                row_dict[metric_name] = metric_value.int_value

                rows_data.append(row_dict)

        # Convert to DataFrame
        df = pd.DataFrame(rows_data)
        return df

    def _transform_dataframe(
        self,
        df: pd.DataFrame,
        column_mapping: Dict[str, str],
        type_conversions: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Transform DataFrame with column renaming and type conversions.

        Args:
            df: Input DataFrame
            column_mapping: Dictionary mapping API column names to BigQuery names
            type_conversions: Dictionary mapping column names to their types
                            Supported types: 'date', 'date_int', 'month_year', 'int64', 'float64'

        Returns:
            Transformed DataFrame
        """
        # Rename columns
        df = df.rename(columns=column_mapping)

        # Apply type conversions if specified
        if type_conversions:
            for col, col_type in type_conversions.items():
                if col not in df.columns:
                    continue

                if col_type == "date":
                    df[col] = pd.to_datetime(df[col]).dt.date
                elif col_type == "date_int":
                    # Convert YYYYMMDD int format to date
                    df[col] = df[col].apply(self.convert_date_int)
                elif col_type == "month_year":
                    # Convert YYMM int format to first day of month
                    df[col] = df[col].apply(self.convert_month_year)
                elif col_type == "int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif col_type == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
                elif col_type == "string":
                    # Explicitly convert to string to avoid type conflicts
                    df[col] = df[col].astype(str)

        return df

    # Report-specific methods using pre-configured report IDs from config

    def get_audience_interest_report(self) -> pd.DataFrame:
        """
        Get audience interest report (monthly).

        Uses the report configured in GAM UI with ID from config.REPORT_IDS.
        The report should be configured with relative date range (e.g., "Last month").

        Returns:
            DataFrame with audience interest data, columns mapped to BigQuery schema
        """
        report_id = REPORT_IDS["audience_interest"]
        df = self.run_report_by_id(report_id)

        return self._transform_dataframe(
            df,
            COLUMN_MAPPINGS["audience_interest"],
            TYPE_CONVERSIONS["audience_interest"]
        )

    def get_inventory_daily_report(self) -> pd.DataFrame:
        """
        Get daily inventory report with Active View metrics.

        Uses the report configured in GAM UI with ID from config.REPORT_IDS.
        The report should be configured with relative date range (e.g., "Yesterday").

        Returns:
            DataFrame with inventory data, columns mapped to BigQuery schema
        """
        report_id = REPORT_IDS["inventory_daily"]
        df = self.run_report_by_id(report_id)

        df = self._transform_dataframe(
            df,
            COLUMN_MAPPINGS["inventory_daily"],
            TYPE_CONVERSIONS["inventory_daily"]
        )

        # Keep only the mapped columns (remove extra AD_UNIT_LEVEL_1, AD_UNIT_LEVEL_2, etc.)
        expected_columns = [
            "date", "ad_unit_top_level", "ad_unit_name", "order_name",
            "device_category", "creative_size", "ad_server_impressions", "ad_server_clicks",
            "active_view_measurable_impressions", "active_view_viewable_impressions"
        ]
        df = df[[col for col in expected_columns if col in df.columns]]

        return df

    def get_audience_demographics_report(self) -> pd.DataFrame:
        """
        Get audience demographics report (monthly).

        Uses the report configured in GAM UI with ID from config.REPORT_IDS.
        The report should be configured with relative date range (e.g., "Last month").

        Returns:
            DataFrame with audience demographics data, columns mapped to BigQuery schema
        """
        report_id = REPORT_IDS["audience_demographics"]
        df = self.run_report_by_id(report_id)

        return self._transform_dataframe(
            df,
            COLUMN_MAPPINGS["audience_demographics"],
            TYPE_CONVERSIONS["audience_demographics"]
        )

    def get_fill_rate_report(self) -> pd.DataFrame:
        """
        Get daily fill rate report.

        Uses the report configured in GAM UI with ID from config.REPORT_IDS.
        The report should be configured with relative date range (e.g., "Yesterday").

        Returns:
            DataFrame with fill rate data, columns mapped to BigQuery schema
        """
        report_id = REPORT_IDS["fill_rate"]
        df = self.run_report_by_id(report_id)

        df = self._transform_dataframe(
            df,
            COLUMN_MAPPINGS["fill_rate"],
            TYPE_CONVERSIONS["fill_rate"]
        )

        # Keep only the mapped columns (remove extra AD_UNIT_NAME_LEVEL_1, etc.)
        expected_columns = [
            "date", "ad_unit_name", "unfilled_impressions", "code_served_count",
            "responses_served", "ad_server_impressions", "fill_rate", "ad_requests"
        ]
        df = df[[col for col in expected_columns if col in df.columns]]

        return df

    def get_geo_monthly_report(self) -> pd.DataFrame:
        """
        Get monthly geographic report.

        Uses the report configured in GAM UI with ID from config.REPORT_IDS.
        The report should be configured with relative date range (e.g., "Last month").

        Returns:
            DataFrame with geographic data, columns mapped to BigQuery schema
        """
        report_id = REPORT_IDS["geo_monthly"]
        df = self.run_report_by_id(report_id)

        return self._transform_dataframe(
            df,
            COLUMN_MAPPINGS["geo_monthly"],
            TYPE_CONVERSIONS["geo_monthly"]
        )
