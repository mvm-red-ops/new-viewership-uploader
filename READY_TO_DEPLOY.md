# Ready to Deploy - Streamlit Revenue Upload Fix

## Status: Ready for Deployment ✅

All code changes are complete and the stored procedure is deployed to Snowflake. Only the Lambda needs to be deployed to AWS.

## What's Done ✅

1. **Stored Procedure Deployed** ✅
   - `UPLOAD_DB.PUBLIC.MOVE_STREAMLIT_DATA_TO_STAGING(VARCHAR, VARCHAR)` is live in Snowflake
   - Dynamically fetches columns from INFORMATION_SCHEMA
   - Handles Streamlit-specific data movement

2. **Lambda Code Updated** ✅
   - `snowflake-helpers.js` updated to call stored procedure instead of hardcoded SQL
   - Deployment package created: `function.zip` in `/Users/tayloryoung/work/nosey/aws/lambda-master/register-data-processing-lambda/`

3. **Templates Fixed** ✅
   - Tubi VOD template has correct Partner/Channel format
   - Freevee VOD template has correct Partner/Channel format

4. **Validation Fixed** ✅
   - Validation procedure now accepts TYPE parameter
   - Correctly checks revenue vs tot_mov based on data type

## What Needs to Be Done

### 1. Deploy Lambda to AWS

```bash
cd /Users/tayloryoung/work/nosey/aws/lambda-master/register-data-processing-lambda

# Deployment package is already created: function.zip

# Deploy via AWS CLI (recommended):
aws lambda update-function-code \
    --function-name register-data-processing-lambda \
    --zip-file fileb://function.zip \
    --region us-east-1

# OR deploy manually via AWS Console:
# 1. Go to AWS Lambda Console
# 2. Find lambda: register-data-processing-lambda
# 3. Upload function.zip
# 4. Click "Deploy"
```

### 2. Clean Up Test Data

```sql
DELETE FROM upload_db.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM test_staging.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
WHERE filename = 'tubi_vod_july.csv';
```

### 3. Test Upload

Upload `tubi_vod_july.csv` via Streamlit:
- Platform: Tubi
- Partner: DEFAULT
- Year: 2025, Quarter: Q3, Month: July
- Click "Load Data"

### 4. Verify Success

Check lambda logs for:
```
Calling stored procedure: CALL upload_db.public.move_streamlit_data_to_staging('Tubi', 'tubi_vod_july.csv')
✓ Data moved to staging successfully
```

Check final table:
```sql
SELECT COUNT(*), label, COUNT(revenue) as has_revenue
FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY label;
-- Expected: label='Revenue', count > 0
```

## Timeline of Fixes

1. **Fix #1:** Template hardcoded values now save as `{"hardcoded_value": "value"}`
2. **Fix #2:** Validation procedure accepts TYPE parameter and validates correctly
3. **Fix #3:** Lambda calls `handle_final_insert_dynamic_generic` (not old version)
4. **Fix #4:** Lambda uses stored procedure with dynamic column mapping (not hardcoded SQL)

## Files Changed

### Snowflake
- ✅ `snowflake/stored_procedures/generic/move_streamlit_data_to_staging.sql` (NEW)
- ✅ `sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql` (UPDATED)
- ✅ `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` (UPDATED)
- ✅ `snowflake/stored_procedures/4.table_migrations/handle_final_insert_dynamic_generic.sql` (UPDATED)

### Lambda (Needs AWS Deployment)
- ⏳ `aws/lambda-master/register-data-processing-lambda/snowflake-helpers.js` (UPDATED)
- ⏳ `aws/lambda-master/register-data-processing-lambda/function.zip` (READY)

### Application
- ✅ `app.py` (UPDATED - template save logic)
- ✅ Database: Tubi & Freevee templates updated

## Next Command

```bash
# From aws/lambda-master/register-data-processing-lambda:
aws lambda update-function-code \
    --function-name register-data-processing-lambda \
    --zip-file fileb://function.zip \
    --region us-east-1
```

Or upload via AWS Console if you prefer.

## Documentation

- `DEPLOY_LAMBDA_FINAL.md` - Complete deployment guide with all details
- `REVENUE_BY_EPISODE_FIXES.md` - Original fixes for validation and templates
- `DEPLOY_LAMBDA.md` - Previous deployment notes (now superseded)
