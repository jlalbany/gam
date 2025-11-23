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
    SECRET_NAME,
    REPORT_TYPE_INVENTORY_DAILY,
    REPORT_TYPE_GEO_MONTHLY,
    TABLE_INVENTORY_DAILY,
    TABLE_GEO_MONTHLY,
)
from utils.gam_client import GAMReportClient
from utils.bigquery_client import BigQueryClient
from utils.logger import StructuredLogger


def process_inventory_daily(
    gam_client: GAMReportClient,
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

        df = gam_client.get_inventory_daily_report(date_range_type="YESTERDAY")

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
    gam_client: GAMReportClient,
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

        df = gam_client.get_geo_monthly_report(date_range_type="LAST_MONTH")

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
        if report_type not in [REPORT_TYPE_INVENTORY_DAILY, REPORT_TYPE_GEO_MONTHLY]:
            StructuredLogger.error(
                f"Invalid report_type: {report_type}",
                status="FAILED",
            )
            return {"error": f"Invalid report_type: {report_type}"}, 400

        # Initialize clients
        StructuredLogger.info("Initializing clients", report_type=report_type)
        gam_client = GAMReportClient(project_id=PROJECT_ID, secret_name=SECRET_NAME)
        bq_client = BigQueryClient(project_id=PROJECT_ID, dataset_id=DATASET_ID)

        # Route to appropriate handler
        if report_type == REPORT_TYPE_INVENTORY_DAILY:
            result = process_inventory_daily(gam_client, bq_client)
        elif report_type == REPORT_TYPE_GEO_MONTHLY:
            result = process_geo_monthly(gam_client, bq_client)

        return result, 200

    except Exception as e:
        StructuredLogger.error(
            f"Unhandled exception in Cloud Function: {str(e)}",
            status="FAILED",
            error=str(e),
        )
        return {"error": str(e), "status": "FAILED"}, 500
