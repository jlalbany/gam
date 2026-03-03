"""BigQuery client wrapper for GAM data."""
import pandas as pd
from google.cloud import bigquery
from typing import Optional


class BigQueryClient:
    """Client for BigQuery operations."""

    def __init__(self, project_id: str, dataset_id: str = "gam_data"):
        """
        Initialize BigQuery client.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """
        Insert DataFrame into BigQuery table.

        Args:
            df: DataFrame to insert
            table_id: Table name (without dataset/project prefix)
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, etc.)

        Returns:
            Number of rows inserted
        """
        if df.empty:
            return 0

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            # Schema auto-detection is disabled; we rely on existing table schema
            autodetect=False,
        )

        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )

        job.result()  # Wait for job to complete

        return len(df)

    def create_dataset(self, location: str = "EU") -> None:
        """
        Create dataset if it doesn't exist.

        Args:
            location: Dataset location (EU, US, etc.)
        """
        dataset_ref = f"{self.project_id}.{self.dataset_id}"

        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            # Dataset doesn't exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.client.create_dataset(dataset)

    def create_table_from_schema(
        self,
        table_id: str,
        schema: list,
        partition_field: Optional[str] = None,
        partition_type: str = "DAY",
    ) -> None:
        """
        Create table from schema definition.

        Args:
            table_id: Table name
            schema: List of SchemaField objects or dict schema
            partition_field: Field name to partition on
            partition_type: Partition type (DAY, MONTH, etc.)
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        try:
            self.client.get_table(table_ref)
            # Table exists, skip creation
            return
        except Exception:
            pass

        # Convert dict schema to SchemaField objects if needed
        if schema and isinstance(schema[0], dict):
            schema_fields = [
                bigquery.SchemaField(
                    name=field["name"],
                    field_type=field["type"],
                    mode=field.get("mode", "NULLABLE"),
                    description=field.get("description", ""),
                )
                for field in schema
            ]
        else:
            schema_fields = schema

        table = bigquery.Table(table_ref, schema=schema_fields)

        # Add partitioning if specified
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=getattr(bigquery.TimePartitioningType, partition_type),
                field=partition_field,
            )

        self.client.create_table(table)

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Query results as DataFrame
        """
        query_job = self.client.query(sql)
        return query_job.to_dataframe()
