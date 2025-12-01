"""
Cloud Function for automated GAM reporting.

This function is triggered by Cloud Scheduler and routes to the appropriate
report handler based on the request payload.
"""
import json
from datetime import datetime
from typing import Any, Dict

import functions_framework
from flask import Request

from config import (
    PROJECT_ID,
    DATASET_ID,
    NETWORK_CODE,
    REPORT_TYPE_INVENTORY_DAILY,
    REPORT_TYPE_GEO_MONTHLY,
    REPORT_TYPE_FILL_RATE,
    REPORT_TYPE_AUDIENCE_INTEREST,
    REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
    TABLE_INVENTORY_DAILY,
    TABLE_GEO_MONTHLY,
    TABLE_FILL_RATE,
    TABLE_AUDIENCE_INTEREST,
    TABLE_AUDIENCE_DEMOGRAPHICS,
)
from utils.gam_rest_client import GAMRestClient
from utils.bigquery_client import BigQueryClient
from utils.logger import StructuredLogger


def process_inventory_daily(
    gam_client: GAMRestClient,
    bq_client: BigQueryClient,
) -> Dict[str, Any]:
    """
    Process daily inventory report.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance

    Returns:
        Result dictionary with status and rows_inserted
    """
    StructuredLogger.info(
        "Starting inventory daily report processing",
        report_type=REPORT_TYPE_INVENTORY_DAILY,
    )

    try:
        # Fetch data from GAM (YESTERDAY)
        StructuredLogger.info(
            "Fetching data from GAM API",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            date_range="YESTERDAY",
        )

        df = gam_client.get_inventory_daily_report()

        if df.empty:
            StructuredLogger.warning(
                "No data returned from GAM API",
                report_type=REPORT_TYPE_INVENTORY_DAILY,
                rows_inserted=0,
                status="SUCCESS",
            )
            return {"status": "SUCCESS", "rows_inserted": 0, "message": "No data available"}

        StructuredLogger.info(
            f"Retrieved {len(df)} rows from GAM",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            rows_fetched=len(df),
        )

        # Delete existing data for yesterday to avoid duplicates
        from datetime import datetime, timedelta
        yesterday = (datetime.now() - timedelta(days=1)).date()
        StructuredLogger.info(
            f"Deleting existing data for {yesterday}",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            date=str(yesterday),
        )
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_INVENTORY_DAILY}`
            WHERE DATE(date) = '{yesterday}'
        """
        from google.cloud import bigquery as bq
        bq_delete_client = bq.Client(project=PROJECT_ID)
        delete_job = bq_delete_client.query(delete_query)
        delete_job.result()

        # Insert into BigQuery
        StructuredLogger.info(
            "Inserting data into BigQuery",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            table=TABLE_INVENTORY_DAILY,
        )

        rows_inserted = bq_client.insert_dataframe(
            df=df,
            table_id=TABLE_INVENTORY_DAILY,
            write_disposition="WRITE_APPEND",
        )

        StructuredLogger.info(
            "Inventory daily report completed successfully",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            rows_inserted=rows_inserted,
            status="SUCCESS",
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "report_type": REPORT_TYPE_INVENTORY_DAILY,
        }

    except Exception as e:
        StructuredLogger.error(
            f"Error processing inventory daily report: {str(e)}",
            report_type=REPORT_TYPE_INVENTORY_DAILY,
            status="FAILED",
            error=str(e),
        )
        raise


def process_geo_monthly(
    gam_client: GAMRestClient,
    bq_client: BigQueryClient,
) -> Dict[str, Any]:
    """
    Process monthly geo report.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance

    Returns:
        Result dictionary with status and rows_inserted
    """
    StructuredLogger.info(
        "Starting geo monthly report processing",
        report_type=REPORT_TYPE_GEO_MONTHLY,
    )

    try:
        # Fetch data from GAM (LAST_MONTH)
        StructuredLogger.info(
            "Fetching data from GAM API",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            date_range="LAST_MONTH",
        )

        df = gam_client.get_geo_monthly_report()

        if df.empty:
            StructuredLogger.warning(
                "No data returned from GAM API",
                report_type=REPORT_TYPE_GEO_MONTHLY,
                rows_inserted=0,
                status="SUCCESS",
            )
            return {"status": "SUCCESS", "rows_inserted": 0, "message": "No data available"}

        StructuredLogger.info(
            f"Retrieved {len(df)} rows from GAM",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            rows_fetched=len(df),
            report_date=df["report_date"].iloc[0].isoformat(),
        )

        # Delete existing data for last month to avoid duplicates
        from datetime import datetime, timedelta
        report_date_to_delete = df["report_date"].iloc[0]
        StructuredLogger.info(
            f"Deleting existing data for {report_date_to_delete}",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            report_date=str(report_date_to_delete),
        )
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_GEO_MONTHLY}`
            WHERE DATE(report_date) = '{report_date_to_delete}'
        """
        from google.cloud import bigquery as bq
        bq_delete_client = bq.Client(project=PROJECT_ID)
        delete_job = bq_delete_client.query(delete_query)
        delete_job.result()

        # Insert into BigQuery
        StructuredLogger.info(
            "Inserting data into BigQuery",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            table=TABLE_GEO_MONTHLY,
        )

        rows_inserted = bq_client.insert_dataframe(
            df=df,
            table_id=TABLE_GEO_MONTHLY,
            write_disposition="WRITE_APPEND",
        )

        StructuredLogger.info(
            "Geo monthly report completed successfully",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            rows_inserted=rows_inserted,
            status="SUCCESS",
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "report_type": REPORT_TYPE_GEO_MONTHLY,
        }

    except Exception as e:
        StructuredLogger.error(
            f"Error processing geo monthly report: {str(e)}",
            report_type=REPORT_TYPE_GEO_MONTHLY,
            status="FAILED",
            error=str(e),
        )
        raise


def process_fill_rate_daily(
    gam_client: GAMRestClient,
    bq_client: BigQueryClient,
) -> Dict[str, Any]:
    """
    Process daily fill rate report.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance

    Returns:
        Result dictionary with status and rows_inserted
    """
    StructuredLogger.info(
        "Starting fill rate daily report processing",
        report_type=REPORT_TYPE_FILL_RATE,
    )

    try:
        # Fetch data from GAM (YESTERDAY)
        StructuredLogger.info(
            "Fetching data from GAM API",
            report_type=REPORT_TYPE_FILL_RATE,
            date_range="YESTERDAY",
        )

        df = gam_client.get_fill_rate_report()

        if df.empty:
            StructuredLogger.warning(
                "No data returned from GAM API",
                report_type=REPORT_TYPE_FILL_RATE,
                rows_inserted=0,
                status="SUCCESS",
            )
            return {"status": "SUCCESS", "rows_inserted": 0, "message": "No data available"}

        StructuredLogger.info(
            f"Retrieved {len(df)} rows from GAM",
            report_type=REPORT_TYPE_FILL_RATE,
            rows_fetched=len(df),
        )

        # Delete existing data for yesterday to avoid duplicates
        from datetime import datetime, timedelta
        yesterday = (datetime.now() - timedelta(days=1)).date()
        StructuredLogger.info(
            f"Deleting existing data for {yesterday}",
            report_type=REPORT_TYPE_FILL_RATE,
            date=str(yesterday),
        )
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_FILL_RATE}`
            WHERE DATE(date) = '{yesterday}'
        """
        from google.cloud import bigquery as bq
        bq_delete_client = bq.Client(project=PROJECT_ID)
        delete_job = bq_delete_client.query(delete_query)
        delete_job.result()

        # Insert into BigQuery
        StructuredLogger.info(
            "Inserting data into BigQuery",
            report_type=REPORT_TYPE_FILL_RATE,
            table=TABLE_FILL_RATE,
        )

        rows_inserted = bq_client.insert_dataframe(
            df=df,
            table_id=TABLE_FILL_RATE,
            write_disposition="WRITE_APPEND",
        )

        StructuredLogger.info(
            "Fill rate daily report completed successfully",
            report_type=REPORT_TYPE_FILL_RATE,
            rows_inserted=rows_inserted,
            status="SUCCESS",
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "report_type": REPORT_TYPE_FILL_RATE,
        }

    except Exception as e:
        StructuredLogger.error(
            f"Error processing fill rate daily report: {str(e)}",
            report_type=REPORT_TYPE_FILL_RATE,
            status="FAILED",
            error=str(e),
        )
        raise


def process_audience_interest(
    gam_client: GAMRestClient,
    bq_client: BigQueryClient,
) -> Dict[str, Any]:
    """
    Process monthly audience interest report.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance

    Returns:
        Result dictionary with status and rows_inserted
    """
    StructuredLogger.info(
        "Starting audience interest report processing",
        report_type=REPORT_TYPE_AUDIENCE_INTEREST,
    )

    try:
        # Fetch data from GAM (LAST_MONTH)
        StructuredLogger.info(
            "Fetching data from GAM API",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            date_range="LAST_MONTH",
        )

        df = gam_client.get_audience_interest_report()

        if df.empty:
            StructuredLogger.warning(
                "No data returned from GAM API",
                report_type=REPORT_TYPE_AUDIENCE_INTEREST,
                rows_inserted=0,
                status="SUCCESS",
            )
            return {"status": "SUCCESS", "rows_inserted": 0, "message": "No data available"}

        StructuredLogger.info(
            f"Retrieved {len(df)} rows from GAM",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            rows_fetched=len(df),
        )

        # Delete existing data for last month to avoid duplicates
        report_date_to_delete = df["report_date"].iloc[0]
        StructuredLogger.info(
            f"Deleting existing data for {report_date_to_delete}",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            report_date=str(report_date_to_delete),
        )
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_AUDIENCE_INTEREST}`
            WHERE DATE(report_date) = '{report_date_to_delete}'
        """
        from google.cloud import bigquery as bq
        bq_delete_client = bq.Client(project=PROJECT_ID)
        delete_job = bq_delete_client.query(delete_query)
        delete_job.result()

        # Insert into BigQuery
        StructuredLogger.info(
            "Inserting data into BigQuery",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            table=TABLE_AUDIENCE_INTEREST,
        )

        rows_inserted = bq_client.insert_dataframe(
            df=df,
            table_id=TABLE_AUDIENCE_INTEREST,
            write_disposition="WRITE_APPEND",
        )

        StructuredLogger.info(
            "Audience interest report completed successfully",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            rows_inserted=rows_inserted,
            status="SUCCESS",
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "report_type": REPORT_TYPE_AUDIENCE_INTEREST,
        }

    except Exception as e:
        StructuredLogger.error(
            f"Error processing audience interest report: {str(e)}",
            report_type=REPORT_TYPE_AUDIENCE_INTEREST,
            status="FAILED",
            error=str(e),
        )
        raise


def process_audience_demographics(
    gam_client: GAMRestClient,
    bq_client: BigQueryClient,
) -> Dict[str, Any]:
    """
    Process monthly audience demographics report.

    Args:
        gam_client: GAM client instance
        bq_client: BigQuery client instance

    Returns:
        Result dictionary with status and rows_inserted
    """
    StructuredLogger.info(
        "Starting audience demographics report processing",
        report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
    )

    try:
        # Fetch data from GAM (LAST_MONTH)
        StructuredLogger.info(
            "Fetching data from GAM API",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            date_range="LAST_MONTH",
        )

        df = gam_client.get_audience_demographics_report()

        if df.empty:
            StructuredLogger.warning(
                "No data returned from GAM API",
                report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
                rows_inserted=0,
                status="SUCCESS",
            )
            return {"status": "SUCCESS", "rows_inserted": 0, "message": "No data available"}

        StructuredLogger.info(
            f"Retrieved {len(df)} rows from GAM",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            rows_fetched=len(df),
        )

        # Delete existing data for last month to avoid duplicates
        report_date_to_delete = df["report_date"].iloc[0]
        StructuredLogger.info(
            f"Deleting existing data for {report_date_to_delete}",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            report_date=str(report_date_to_delete),
        )
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_AUDIENCE_DEMOGRAPHICS}`
            WHERE DATE(report_date) = '{report_date_to_delete}'
        """
        from google.cloud import bigquery as bq
        bq_delete_client = bq.Client(project=PROJECT_ID)
        delete_job = bq_delete_client.query(delete_query)
        delete_job.result()

        # Insert into BigQuery
        StructuredLogger.info(
            "Inserting data into BigQuery",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            table=TABLE_AUDIENCE_DEMOGRAPHICS,
        )

        rows_inserted = bq_client.insert_dataframe(
            df=df,
            table_id=TABLE_AUDIENCE_DEMOGRAPHICS,
            write_disposition="WRITE_APPEND",
        )

        StructuredLogger.info(
            "Audience demographics report completed successfully",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            rows_inserted=rows_inserted,
            status="SUCCESS",
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "report_type": REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
        }

    except Exception as e:
        StructuredLogger.error(
            f"Error processing audience demographics report: {str(e)}",
            report_type=REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
            status="FAILED",
            error=str(e),
        )
        raise


@functions_framework.http
def main(request: Request) -> tuple:
    """
    Cloud Function entry point.

    Expects JSON payload with 'report_type' field:
    - {"report_type": "INVENTORY_DAILY"}
    - {"report_type": "GEO_MONTHLY"}

    Args:
        request: Flask request object

    Returns:
        Tuple of (response_dict, status_code)
    """
    try:
        # Parse request
        request_json = request.get_json(silent=True)

        if not request_json or "report_type" not in request_json:
            StructuredLogger.error(
                "Missing report_type in request payload",
                status="FAILED",
            )
            return {"error": "Missing report_type in request payload"}, 400

        report_type = request_json["report_type"]

        StructuredLogger.info(
            "Cloud Function triggered",
            report_type=report_type,
            request_id=request.headers.get("X-Cloud-Trace-Context", "unknown"),
        )

        # Validate report type
        valid_types = [
            REPORT_TYPE_INVENTORY_DAILY,
            REPORT_TYPE_GEO_MONTHLY,
            REPORT_TYPE_FILL_RATE,
            REPORT_TYPE_AUDIENCE_INTEREST,
            REPORT_TYPE_AUDIENCE_DEMOGRAPHICS,
        ]
        if report_type not in valid_types:
            StructuredLogger.error(
                f"Invalid report_type: {report_type}",
                status="FAILED",
            )
            return {"error": f"Invalid report_type: {report_type}"}, 400

        # Initialize clients
        StructuredLogger.info("Initializing clients", report_type=report_type)
        gam_client = GAMRestClient(project_id=PROJECT_ID, network_code=NETWORK_CODE)
        bq_client = BigQueryClient(project_id=PROJECT_ID, dataset_id=DATASET_ID)

        # Route to appropriate handler
        if report_type == REPORT_TYPE_INVENTORY_DAILY:
            result = process_inventory_daily(gam_client, bq_client)
        elif report_type == REPORT_TYPE_GEO_MONTHLY:
            result = process_geo_monthly(gam_client, bq_client)
        elif report_type == REPORT_TYPE_FILL_RATE:
            result = process_fill_rate_daily(gam_client, bq_client)
        elif report_type == REPORT_TYPE_AUDIENCE_INTEREST:
            result = process_audience_interest(gam_client, bq_client)
        elif report_type == REPORT_TYPE_AUDIENCE_DEMOGRAPHICS:
            result = process_audience_demographics(gam_client, bq_client)

        return result, 200

    except Exception as e:
        StructuredLogger.error(
            f"Unhandled exception in Cloud Function: {str(e)}",
            status="FAILED",
            error=str(e),
        )
        return {"error": str(e), "status": "FAILED"}, 500
