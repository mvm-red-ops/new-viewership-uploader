#!/bin/bash

# ==============================================================================
# SQL Deployment Script - Environment-Based
# ==============================================================================
# Usage:
#   ./deploy.sh staging DEPLOY_ALL_GENERIC_PROCEDURES.sql
#   ./deploy.sh prod CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
#   ./deploy.sh staging DEPLOY_ALL_GENERIC_PROCEDURES.sql --execute
# ==============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check arguments
if [ $# -lt 2 ]; then
    echo -e "${RED}Usage: $0 <environment> <sql-file> [--execute]${NC}"
    echo ""
    echo "Arguments:"
    echo "  environment: staging | prod"
    echo "  sql-file: Path to SQL template file (relative to sql/ directory)"
    echo "  --execute: (Optional) Execute the SQL in Snowflake after generating"
    echo ""
    echo "Examples:"
    echo "  $0 staging DEPLOY_ALL_GENERIC_PROCEDURES.sql"
    echo "  $0 prod create_platform_viewership.sql"
    echo "  $0 staging DEPLOY_ALL_GENERIC_PROCEDURES.sql --execute"
    exit 1
fi

ENVIRONMENT=$1
SQL_FILE=$2
EXECUTE_FLAG=$3

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_DIR="${SCRIPT_DIR}/config"
TEMPLATES_DIR="${SCRIPT_DIR}/templates"
OUTPUT_DIR="${SCRIPT_DIR}/generated"

# Validate environment
if [ "$ENVIRONMENT" != "staging" ] && [ "$ENVIRONMENT" != "prod" ]; then
    echo -e "${RED}Error: Environment must be 'staging' or 'prod'${NC}"
    exit 1
fi

# Load environment config
CONFIG_FILE="${CONFIG_DIR}/${ENVIRONMENT}.env"
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Config file not found: $CONFIG_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}Loading configuration: $CONFIG_FILE${NC}"
source "$CONFIG_FILE"

# Check if template file exists
TEMPLATE_FILE="${TEMPLATES_DIR}/${SQL_FILE}"
if [ ! -f "$TEMPLATE_FILE" ]; then
    echo -e "${RED}Error: Template file not found: $TEMPLATE_FILE${NC}"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Generate output filename
OUTPUT_FILE="${OUTPUT_DIR}/${ENVIRONMENT}_${SQL_FILE}"

echo -e "${GREEN}Processing template: $TEMPLATE_FILE${NC}"
echo -e "${GREEN}Environment: $ENVIRONMENT${NC}"
echo -e "${GREEN}Configuration:${NC}"
echo -e "  UPLOAD_DB: ${YELLOW}$UPLOAD_DB${NC}"
echo -e "  STAGING_DB: ${YELLOW}$STAGING_DB${NC}"
echo -e "  ASSETS_DB: ${YELLOW}$ASSETS_DB${NC}"
echo -e "  EPISODE_DETAILS_TABLE: ${YELLOW}$EPISODE_DETAILS_TABLE${NC}"
echo -e "  METADATA_DB: ${YELLOW}$METADATA_DB${NC}"
echo ""

# Replace placeholders in SQL file
sed -e "s/{{UPLOAD_DB}}/$UPLOAD_DB/g" \
    -e "s/{{STAGING_DB}}/$STAGING_DB/g" \
    -e "s/{{ASSETS_DB}}/$ASSETS_DB/g" \
    -e "s/{{EPISODE_DETAILS_TABLE}}/$EPISODE_DETAILS_TABLE/g" \
    -e "s/{{METADATA_DB}}/$METADATA_DB/g" \
    "$TEMPLATE_FILE" > "$OUTPUT_FILE"

echo -e "${GREEN}✓ Generated SQL file: $OUTPUT_FILE${NC}"
echo ""

# Optionally execute in Snowflake
if [ "$EXECUTE_FLAG" == "--execute" ]; then
    echo -e "${YELLOW}Executing SQL in Snowflake...${NC}"
    echo -e "${YELLOW}Note: This requires snowsql to be configured${NC}"
    echo ""

    # Check if snowsql is available
    if ! command -v snowsql &> /dev/null; then
        echo -e "${RED}Error: snowsql command not found. Install Snowflake CLI to use --execute${NC}"
        echo -e "${YELLOW}You can still manually run: $OUTPUT_FILE${NC}"
        exit 1
    fi

    # Execute the generated SQL
    snowsql -f "$OUTPUT_FILE"

    echo ""
    echo -e "${GREEN}✓ SQL executed successfully${NC}"
else
    echo -e "${YELLOW}To execute this SQL in Snowflake:${NC}"
    echo -e "  1. Manual: Copy/paste from ${OUTPUT_FILE}"
    echo -e "  2. SnowSQL: snowsql -f ${OUTPUT_FILE}"
    echo -e "  3. Re-run with: $0 $ENVIRONMENT $SQL_FILE --execute"
fi

echo ""
echo -e "${GREEN}Deployment preparation complete!${NC}"
