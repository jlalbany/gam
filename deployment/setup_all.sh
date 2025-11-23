#!/bin/bash
# Complete setup script - runs all setup steps in order

set -e

echo "=========================================="
echo "GAM Reporting Automation - Complete Setup"
echo "=========================================="

# Check if PROJECT_ID is set
if [ -z "$GCP_PROJECT_ID" ]; then
  echo "Error: GCP_PROJECT_ID environment variable not set"
  echo "Usage: export GCP_PROJECT_ID=your-project-id && ./setup_all.sh"
  exit 1
fi

echo "This script will:"
echo "1. Create BigQuery dataset and tables"
echo "2. Deploy Cloud Function"
echo "3. Create Cloud Scheduler jobs"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  exit 0
fi

# Step 1: Setup BigQuery
echo ""
echo "=========================================="
echo "Step 1/3: Setting up BigQuery"
echo "=========================================="
./setup_bigquery.sh

# Step 2: Deploy Cloud Function
echo ""
echo "=========================================="
echo "Step 2/3: Deploying Cloud Function"
echo "=========================================="
./deploy.sh

# Step 3: Create Schedulers
echo ""
echo "=========================================="
echo "Step 3/3: Creating Cloud Scheduler jobs"
echo "=========================================="
./create_schedulers.sh

echo ""
echo "=========================================="
echo "✓ Complete setup finished successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run backfill for historical data:"
echo "   cd ../backfill"
echo "   python backfill_gam_reports.py --project-id $GCP_PROJECT_ID --start-date YYYY-MM-DD --end-date YYYY-MM-DD"
echo ""
echo "2. Monitor Cloud Function logs:"
echo "   gcloud functions logs read gam-reporting-automation --region=$REGION"
echo ""
echo "3. Test schedulers manually:"
echo "   gcloud scheduler jobs run trigger-gam-inventory-daily --location=$REGION"
echo "=========================================="
