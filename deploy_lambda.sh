#!/bin/bash

# Lambda Deployment Script for Generic Platform Architecture
# This script packages and deploys the Lambda function

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "Lambda Deployment"
echo "========================================="
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI is not installed${NC}"
    echo "Please install AWS CLI: https://aws.amazon.com/cli/"
    exit 1
fi

# Change to lambda directory
cd lambda

echo -e "${YELLOW}Installing dependencies...${NC}"
npm install

echo -e "${YELLOW}Packaging Lambda function...${NC}"
zip -r lambda-deployment.zip index.js snowflake-helpers.js package.json node_modules/ -q

echo -e "${GREEN}✓ Package created: lambda-deployment.zip${NC}\n"

# Get Lambda function name from user
echo -e "${YELLOW}Enter your Lambda function name:${NC}"
read -p "Function name: " FUNCTION_NAME

if [ -z "$FUNCTION_NAME" ]; then
    echo -e "${RED}Error: Function name is required${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Deploying to AWS Lambda...${NC}"

if aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file fileb://lambda-deployment.zip \
    --no-cli-pager; then
    echo -e "${GREEN}✓ Lambda deployed successfully!${NC}"
else
    echo -e "${RED}✗ Lambda deployment failed${NC}"
    echo ""
    echo "You can also deploy manually:"
    echo "1. Upload lambda-deployment.zip to AWS Lambda Console"
    echo "2. Or use: aws lambda update-function-code --function-name <name> --zip-file fileb://lambda-deployment.zip"
    exit 1
fi

# Clean up
echo ""
echo -e "${YELLOW}Cleaning up...${NC}"
rm lambda-deployment.zip
echo -e "${GREEN}✓ Cleanup complete${NC}"

echo ""
echo "========================================="
echo -e "${GREEN}✓ Deployment Complete!${NC}"
echo "========================================="
echo ""
echo "Deployed Lambda: $FUNCTION_NAME"
echo ""
echo "Next steps:"
echo "  1. Verify Lambda environment variables are set"
echo "  2. Test end-to-end with Streamlit app"
echo "  3. Monitor CloudWatch logs"
echo "  4. Check DEPLOYMENT_CHECKLIST.md for testing guide"
echo ""
