#!/bin/bash
# Deploy Lambda function with updated code

set -e  # Exit on error

# Determine environment (default to staging)
ENV=${1:-staging}

if [ "$ENV" == "production" ] || [ "$ENV" == "prod" ]; then
    FUNCTION_NAME="register-start-viewership-data-processing"
    echo "🚀 Deploying to PRODUCTION: $FUNCTION_NAME"
    read -p "Are you sure you want to deploy to PRODUCTION? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "❌ Deployment cancelled"
        exit 1
    fi
else
    FUNCTION_NAME="register-start-viewership-data-processing-staging"
    echo "🚀 Deploying to STAGING: $FUNCTION_NAME"
fi

echo ""
echo "📦 Creating deployment package..."

# Create a temporary directory for the deployment package
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

# Copy Lambda code
cp index.js "$TEMP_DIR/"
cp snowflake-helpers.js "$TEMP_DIR/"
cp package.json "$TEMP_DIR/"

# Install dependencies (production only, no dev dependencies)
cd "$TEMP_DIR"
npm install --production

# Create zip file
echo "📦 Creating zip file..."
zip -r lambda-deployment.zip . > /dev/null

# Deploy to Lambda
echo "☁️  Uploading to Lambda function: $FUNCTION_NAME"
aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://lambda-deployment.zip" \
    --region us-east-1

# Cleanup
cd -
rm -rf "$TEMP_DIR"

echo ""
echo "✅ Deployment successful!"
echo ""
echo "Function: $FUNCTION_NAME"
echo "Region: us-east-1"
echo ""
