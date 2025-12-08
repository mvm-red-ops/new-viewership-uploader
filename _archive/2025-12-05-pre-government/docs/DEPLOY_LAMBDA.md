# Lambda Deployment - Revenue-by-Episode Fix

## ⚠️ CRITICAL: Snowflake Procedures Must Be Deployed First

**BEFORE deploying the lambda, you MUST deploy all Snowflake procedures to the target environment:**

```bash
# For staging:
python3 sql/deploy/deploy.py --env staging

# For production:
python3 sql/deploy/deploy.py --env prod
```

**Why this is critical:**
- The lambda calls stored procedures that use template variables like `{{UPLOAD_DB}}`, `{{STAGING_DB}}`, `{{METADATA_DB}}`
- These templates are ONLY replaced during deployment via the deploy script
- Individual procedure files in `snowflake/stored_procedures/` are NOT automatically deployed
- Editing procedures directly in Snowflake will be overwritten on next deploy
- Hardcoding database names (like `TEST_STAGING` or `UPLOAD_DB`) will break in production

**The deployment script (`sql/deploy/deploy.py`):**
1. Reads template files from `sql/templates/` (NOT individual procedure files)
2. Replaces `{{UPLOAD_DB}}` with `UPLOAD_DB` (staging) or `UPLOAD_DB_PROD` (prod)
3. Replaces `{{STAGING_DB}}` with `TEST_STAGING` (staging) or `NOSEY_PROD` (prod)
4. Replaces `{{METADATA_DB}}` with `METADATA_MASTER_CLEANED_STAGING` or `METADATA_MASTER`
5. Deploys all procedures in the correct order with proper dependencies

**Key procedures that must exist before lambda deployment:**
- ✅ `NORMALIZE_DATA_IN_STAGING` - orchestrates Phase 1
- ✅ `SET_DEAL_PARENT_GENERIC` - sets deal metadata
- ✅ `SET_REF_ID_FROM_PLATFORM_CONTENT_ID` - matches content IDs to ref_ids
- ✅ `SET_DATE_COLUMNS_DYNAMIC` - sets date columns
- ✅ `CALCULATE_VIEWERSHIP_METRICS` - calculates TOT_MOV/TOT_HOV
- ✅ `ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC` - orchestrates Phase 2
- ✅ `HANDLE_FINAL_INSERT_DYNAMIC_GENERIC` - orchestrates Phase 3

## Changes Made

### File: `aws/lambda-master/register-data-processing-lambda/snowflake-helpers.js`

**Line 246:** Changed stored procedure call to use generic version

**Before:**
```javascript
const storedProcedureStatement = `CALL ${databaseName}.public.handle_final_insert_dynamic('${platform}', '${type}', '${filename}');`;
```

**After:**
```javascript
const storedProcedureStatement = `CALL ${databaseName}.public.handle_final_insert_dynamic_generic('${platform}', '${type}', '${filename}');`;
```

## Why This Change?

- The `moveToFinalTable` function is called by the **Streamlit processing path** (new architecture)
- Streamlit uses `platform_viewership` table (generic architecture) instead of platform-specific tables
- The `_generic` stored procedure we just deployed expects to be called from this path
- The `_generic` procedure now passes TYPE to validation, enabling proper revenue validation

## Deployment Steps

### Option 1: AWS Console (Manual)
1. Go to AWS Lambda Console
2. Find lambda: `register-data-processing-lambda` (or similar name)
3. Update the code for `snowflake-helpers.js`
4. Save and deploy

### Option 2: AWS CLI (if configured)
```bash
cd ../../aws/lambda-master/register-data-processing-lambda

# Zip the lambda
zip -r function.zip . -x "*.git*" "node_modules/*"

# Deploy (replace with actual lambda name and region)
aws lambda update-function-code \
    --function-name register-data-processing-lambda \
    --zip-file fileb://function.zip \
    --region us-east-1
```

### Option 3: Terraform/IaC (if you use it)
Update your infrastructure code and apply.

## Testing After Deployment

1. Clean up test data:
```sql
DELETE FROM upload_db.public.platform_viewership WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';
DELETE FROM test_staging.public.platform_viewership WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';
DELETE FROM staging_assets.public.episode_details_test_staging WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';
DELETE FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs WHERE filename = 'tubi_vod_july.csv';
```

2. Upload Tubi revenue file through Streamlit

3. Check lambda logs for:
   - Should see: `CALL UPLOAD_DB.public.handle_final_insert_dynamic_generic('Tubi', 'Revenue', 'tubi_vod_july.csv')`
   - Validation should pass (no more "Missing tot_mov" errors)

4. Verify data in final table:
```sql
SELECT COUNT(*), label,
       COUNT(revenue) as has_revenue,
       COUNT(tot_mov) as has_tot_mov
FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY label;
-- Expected: label='Revenue', has_revenue=100, has_tot_mov=0
```

## What This Doesn't Affect

The old S3-based upload path (`jobType !== "Streamlit"`) still calls the old procedures and is unchanged. This only affects:
- ✅ Streamlit uploads (new architecture)
- ✅ Revenue-by-episode files
- ✅ All files using the new viewership uploader tool

## Rollback Plan

If something breaks, change line 246 back to:
```javascript
const storedProcedureStatement = `CALL ${databaseName}.public.handle_final_insert_dynamic('${platform}', '${type}', '${filename}');`;
```

And redeploy.
