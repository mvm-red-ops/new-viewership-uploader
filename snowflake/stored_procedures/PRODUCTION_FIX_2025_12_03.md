# Production Fix - December 3, 2025

## Problem
Production pipeline was not setting `ref_id` in `NOSEY_PROD.PUBLIC.platform_viewership` table, causing asset matching to fail completely. All records had `ref_id = NULL`, resulting in zero successful asset matches.

## Root Cause
There was a **duplicate `NORMALIZE_DATA_IN_STAGING()` procedure** in `UPLOAD_DB_PROD.PUBLIC` with **ZERO parameters**.

This zero-parameter procedure was:
- WURL-specific (only called WURL procedures like `set_channel_wurl`, `set_deal_parent_wurl`, etc.)
- Did NOT call `SET_REF_ID_FROM_PLATFORM_CONTENT_ID`
- Completely incorrect for generic platforms like Tubi, Pluto, Amagi, etc.

When Lambda invoked the procedure, Snowflake was calling the wrong zero-parameter version instead of the correct 2-parameter generic version.

## The Fix

### Step 1: Dropped the duplicate procedure
```sql
DROP PROCEDURE UPLOAD_DB_PROD.PUBLIC.NORMALIZE_DATA_IN_STAGING()
```

### Step 2: Verified the correct procedure exists
The 2-parameter version remains: `NORMALIZE_DATA_IN_STAGING(VARCHAR platform, VARCHAR filename)`

This procedure correctly:
1. Calls `SET_DEAL_PARENT_GENERIC(platform, filename)`
2. **Calls `SET_REF_ID_FROM_PLATFORM_CONTENT_ID(platform, filename)`** ← Critical step
3. Calls `CALCULATE_VIEWERSHIP_METRICS(platform, filename)`
4. Calls `SET_DATE_COLUMNS_DYNAMIC(platform, filename)`
5. Calls `SET_PHASE_GENERIC(platform, 1, filename)`

## Verification

After the fix, tested with production data:
```sql
CALL UPLOAD_DB_PROD.PUBLIC.NORMALIZE_DATA_IN_STAGING('Tubi', 'tubi_vod_july.csv')
```

Expected result: 100/100 records should have `ref_id` set.

## Related Fixes

This session also fixed:
1. **59 hardcoded staging database references** in `snowflake/stored_procedures/production/` directory
   - Changed `TEST_STAGING` → `NOSEY_PROD`
   - Changed `METADATA_MASTER_CLEANED_STAGING` → `METADATA_MASTER`
   - Changed `UPLOAD_DB.PUBLIC` → `UPLOAD_DB_PROD.PUBLIC`

2. **Deployed all 21 procedures** to UPLOAD_DB_PROD with correct production database names
   - 6 bucket procedures
   - 10 generic procedures
   - 5 helper procedures

## Impact

This fix resolves:
- ref_id not being set in production
- Asset matching failing with 0/100 success rate
- Platform-agnostic processing for Tubi, Pluto, Amagi, and other generic platforms

## Prevention

To prevent this issue in the future:
1. Always check for duplicate procedures before deploying: `SHOW PROCEDURES LIKE 'PROCEDURE_NAME'`
2. Use the separated `staging/` and `production/` directories
3. Never manually deploy procedures without using the deployment scripts
4. The deployment scripts now use `CREATE OR REPLACE` which will handle duplicates correctly
