#!/bin/bash

# Snowflake Deployment Script for Generic Platform Architecture
# This script deploys all necessary SQL files to Snowflake

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if snowsql is installed
if ! command -v snowsql &> /dev/null; then
    echo -e "${RED}Error: snowsql is not installed${NC}"
    echo "Please install snowsql: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    exit 1
fi

# Function to execute SQL file
execute_sql() {
    local file=$1
    local description=$2

    echo -e "${YELLOW}Deploying: ${description}${NC}"
    echo "  File: $file"

    if snowsql -f "$file"; then
        echo -e "${GREEN}✓ Success${NC}\n"
    else
        echo -e "${RED}✗ Failed${NC}\n"
        exit 1
    fi
}

echo "========================================="
echo "Snowflake Generic Platform Deployment"
echo "========================================="
echo ""

# Phase 1: Configuration and Tables
echo -e "${GREEN}=== Phase 1: Configuration and Tables ===${NC}\n"

execute_sql "sql/create_platform_config.sql" \
    "Platform configuration table"

execute_sql "sql/create_test_staging_platform_viewership.sql" \
    "Generic platform_viewership table in test_staging"

# Phase 2: Helper Procedures
echo -e "${GREEN}=== Phase 2: Helper Procedures ===${NC}\n"

execute_sql "snowflake/stored_procedures/generic/set_phase_generic.sql" \
    "set_phase_generic helper"

execute_sql "snowflake/stored_procedures/generic/move_sanitized_data_to_staging_generic.sql" \
    "move_sanitized_data_to_staging_generic helper"

# Phase 3: Main Generic Procedures
echo -e "${GREEN}=== Phase 3: Main Generic Procedures ===${NC}\n"

execute_sql "snowflake/stored_procedures/generic/generic_sanitization.sql" \
    "Generic sanitization procedure"

execute_sql "snowflake/stored_procedures/generic/move_viewership_to_staging.sql" \
    "Generic move_viewership_to_staging procedure"

execute_sql "snowflake/stored_procedures/generic/normalize_data_in_staging.sql" \
    "Generic normalize_data_in_staging procedure"

# Phase 4: Platform-Specific Helpers (Pluto)
echo -e "${GREEN}=== Phase 4: Platform-Specific Helpers (Pluto) ===${NC}\n"

execute_sql "snowflake/stored_procedures/generic/helpers/set_territory_pluto_generic.sql" \
    "Pluto territory mapping"

execute_sql "snowflake/stored_procedures/generic/helpers/set_channel_deal_parent_pluto_generic.sql" \
    "Pluto channel and deal parent"

execute_sql "snowflake/stored_procedures/generic/helpers/set_ymd_pluto_generic.sql" \
    "Pluto year-month-day formatter"

execute_sql "snowflake/stored_procedures/generic/helpers/set_quarter_pluto_generic.sql" \
    "Pluto quarter calculator"

execute_sql "snowflake/stored_procedures/generic/helpers/set_hours_pluto_generic.sql" \
    "Pluto hours converter"

# Summary
echo ""
echo "========================================="
echo -e "${GREEN}✓ Deployment Complete!${NC}"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Deploy Lambda code (cd lambda && npm install && zip deployment)"
echo "  2. Test with Pluto platform using Streamlit app"
echo "  3. Monitor CloudWatch logs during test"
echo "  4. Check DEPLOYMENT_CHECKLIST.md for full testing guide"
echo ""
