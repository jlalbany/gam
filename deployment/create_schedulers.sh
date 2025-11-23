#!/bin/bash
# Create Cloud Scheduler jobs for automated GAM reporting

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

echo "=========================================="
echo "Cloud Scheduler Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "=========================================="

# Get function URL
FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --gen2 \
  --format="value(serviceConfig.uri)")

if [ -z "$FUNCTION_URL" ]; then
  echo "Error: Could not find Cloud Function URL"
  echo "Make sure the function is deployed first (run deploy.sh)"
  exit 1
fi

echo "Function URL: $FUNCTION_URL"

# Create or update daily inventory scheduler
echo ""
echo "Creating daily inventory scheduler..."
gcloud scheduler jobs create http trigger-gam-inventory-daily \
  --location="$REGION" \
  --schedule="0 6 * * *" \
  --time-zone="Europe/Paris" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --message-body='{"report_type":"INVENTORY_DAILY"}' \
  --oidc-service-account-email="$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" \
  --description="Daily GAM inventory report (runs at 6am)" \
  || gcloud scheduler jobs update http trigger-gam-inventory-daily \
     --location="$REGION" \
     --schedule="0 6 * * *" \
     --time-zone="Europe/Paris" \
     --uri="$FUNCTION_URL" \
     --http-method=POST \
     --message-body='{"report_type":"INVENTORY_DAILY"}' \
     --oidc-service-account-email="$SERVICE_ACCOUNT" \
     --project="$PROJECT_ID" \
     --description="Daily GAM inventory report (runs at 6am)"

# Create or update monthly geo scheduler
echo ""
echo "Creating monthly geo scheduler..."
gcloud scheduler jobs create http trigger-gam-geo-monthly \
  --location="$REGION" \
  --schedule="0 7 1 * *" \
  --time-zone="Europe/Paris" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --message-body='{"report_type":"GEO_MONTHLY"}' \
  --oidc-service-account-email="$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" \
  --description="Monthly GAM geo report (runs on 1st of month at 7am)" \
  || gcloud scheduler jobs update http trigger-gam-geo-monthly \
     --location="$REGION" \
     --schedule="0 7 1 * *" \
     --time-zone="Europe/Paris" \
     --uri="$FUNCTION_URL" \
     --http-method=POST \
     --message-body='{"report_type":"GEO_MONTHLY"}' \
     --oidc-service-account-email="$SERVICE_ACCOUNT" \
     --project="$PROJECT_ID" \
     --description="Monthly GAM geo report (runs on 1st of month at 7am)"

echo ""
echo "=========================================="
echo "✓ Schedulers created successfully!"
echo "=========================================="
echo ""
echo "Created jobs:"
echo "1. trigger-gam-inventory-daily (0 6 * * *)"
echo "2. trigger-gam-geo-monthly (0 7 1 * *)"
echo ""
echo "To test manually:"
echo "gcloud scheduler jobs run trigger-gam-inventory-daily --location=$REGION"
echo "gcloud scheduler jobs run trigger-gam-geo-monthly --location=$REGION"
echo "=========================================="
