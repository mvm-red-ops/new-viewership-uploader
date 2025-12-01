# Lambda Deployment - Revenue-by-Episode Fix

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
