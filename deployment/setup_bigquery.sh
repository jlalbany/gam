#!/bin/bash
# Setup BigQuery dataset and tables

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-your-project-id}"
DATASET_ID="gam_data"
LOCATION="EU"  # Change to US if needed

echo "=========================================="
echo "BigQuery Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "Location: $LOCATION"
echo "=========================================="

# Create dataset
echo ""
echo "Creating dataset..."
bq --project_id="$PROJECT_ID" mk \
  --dataset \
  --location="$LOCATION" \
  --description="GAM reporting data" \
  "$PROJECT_ID:$DATASET_ID" || echo "Dataset already exists"

# Create inventory_daily table
echo ""
echo "Creating report_inventory_daily table..."
bq --project_id="$PROJECT_ID" mk \
  --table \
  --time_partitioning_field=date \
  --time_partitioning_type=DAY \
  --description="Daily inventory report from GAM" \
  "$PROJECT_ID:$DATASET_ID.report_inventory_daily" \
  ../bigquery_schemas/report_inventory_daily.json || echo "Table already exists"

# Create geo_monthly table
echo ""
echo "Creating report_geo_monthly table..."
bq --project_id="$PROJECT_ID" mk \
  --table \
  --time_partitioning_field=report_date \
  --time_partitioning_type=DAY \
  --description="Monthly geo report from GAM" \
  "$PROJECT_ID:$DATASET_ID.report_geo_monthly" \
  ../bigquery_schemas/report_geo_monthly.json || echo "Table already exists"

echo ""
echo "=========================================="
echo "✓ BigQuery setup complete!"
echo "=========================================="
