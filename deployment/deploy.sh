#!/bin/bash
# Deploy Cloud Function for GAM reporting

set -e

# Validate required environment variables
if [ -z "$GCP_PROJECT_ID" ]; then
  echo "Error: GCP_PROJECT_ID environment variable is not set"
  echo "Usage: export GCP_PROJECT_ID=your-project-id"
  exit 1
fi

# Configuration
PROJECT_ID="$GCP_PROJECT_ID"
REGION="${REGION:-europe-west1}"
FUNCTION_NAME="gam-reporting-automation"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-gam-reporter@${PROJECT_ID}.iam.gserviceaccount.com}"
RUNTIME="python311"
ENTRY_POINT="main"
MEMORY="1024MB"
TIMEOUT="540s"
DATASET_ID="gam_data"
SECRET_NAME="gam_api_config"

echo "=========================================="
echo "Cloud Function Deployment"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Function: $FUNCTION_NAME"
echo "Service Account: $SERVICE_ACCOUNT"
echo "=========================================="

# Change to cloud_function directory
cd ../cloud_function

# Deploy Cloud Function
echo ""
echo "Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime="$RUNTIME" \
  --region="$REGION" \
  --source=. \
  --entry-point="$ENTRY_POINT" \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="$SERVICE_ACCOUNT" \
  --memory="$MEMORY" \
  --timeout="$TIMEOUT" \
  --set-env-vars="GCP_PROJECT=$PROJECT_ID,DATASET_ID=$DATASET_ID,SECRET_NAME=$SECRET_NAME" \
  --project="$PROJECT_ID"

# Get function URL
FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --gen2 \
  --format="value(serviceConfig.uri)")

echo ""
echo "=========================================="
echo "✓ Deployment complete!"
echo "=========================================="
echo "Function URL: $FUNCTION_URL"
echo ""
echo "Next steps:"
echo "1. Setup Cloud Scheduler jobs (run create_schedulers.sh)"
echo "2. Test the function manually"
echo "=========================================="
