"""Structured logging utility for Cloud Functions."""
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredLogger:
    """Structured JSON logger for Google Cloud Logging."""

    @staticmethod
    def log(
        severity: str,
        message: str,
        report_type: Optional[str] = None,
        rows_inserted: Optional[int] = None,
        status: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Log a structured JSON message to stdout.

        Args:
            severity: Log severity (INFO, WARNING, ERROR, DEBUG)
            message: Log message
            report_type: Type of report being processed
            rows_inserted: Number of rows inserted to BigQuery
            status: Status of the operation (SUCCESS, FAILED, etc.)
            **kwargs: Additional fields to include in the log
        """
        log_entry: Dict[str, Any] = {
            "severity": severity,
            "message": message,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        if report_type:
            log_entry["report_type"] = report_type
        if rows_inserted is not None:
            log_entry["rows_inserted"] = rows_inserted
        if status:
            log_entry["status"] = status

        # Add any additional fields
        log_entry.update(kwargs)

        print(json.dumps(log_entry), file=sys.stdout, flush=True)

    @staticmethod
    def info(message: str, **kwargs: Any) -> None:
        """Log INFO level message."""
        StructuredLogger.log("INFO", message, **kwargs)

    @staticmethod
    def warning(message: str, **kwargs: Any) -> None:
        """Log WARNING level message."""
        StructuredLogger.log("WARNING", message, **kwargs)

    @staticmethod
    def error(message: str, **kwargs: Any) -> None:
        """Log ERROR level message."""
        StructuredLogger.log("ERROR", message, **kwargs)

    @staticmethod
    def debug(message: str, **kwargs: Any) -> None:
        """Log DEBUG level message."""
        StructuredLogger.log("DEBUG", message, **kwargs)
