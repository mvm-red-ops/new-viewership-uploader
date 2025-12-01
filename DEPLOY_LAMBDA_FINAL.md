# Lambda Deployment - Streamlit Data Movement Fix

## Summary
Fixed the Streamlit upload pipeline to use dynamic column mapping instead of hardcoded column lists.

## What Was Wrong?

The lambda's `moveDataToStaging` function had hardcoded SQL with explicit column lists:
- This broke when column schemas changed
- Got "59 columns expected but got 50" errors
- Missing columns: DEAL_PARENT, CHANNEL_ID, TERRITORY_ID, INTERNAL_SERIES, ID, FULL_DATE, DAY, WEEK, TOT_COMPLETIONS

## What's Fixed?

### 1. New Stored Procedure Created ✅
**File:** `snowflake/stored_procedures/generic/move_streamlit_data_to_staging.sql`

This procedure:
- Dynamically queries `INFORMATION_SCHEMA.COLUMNS` to get all columns
- Constructs INSERT...SELECT statement with discovered columns
- Filters out LOAD_TIMESTAMP (auto-generated in target)
- Handles Streamlit-specific filtering (no phase, processed=NULL/FALSE)

**Status:** ✅ Deployed to Snowflake (UPLOAD_DB.PUBLIC.MOVE_STREAMLIT_DATA_TO_STAGING)

### 2. Lambda Updated ✅
**File:** `aws/lambda-master/register-data-processing-lambda/snowflake-helpers.js`

**Line 300-313:** Changed `moveDataToStaging` function from hardcoded SQL to stored procedure call:

```javascript
async function moveDataToStaging(uploadDatabaseName, viewershipDatabaseFullyQualified, platform, filename) {
    console.log(`Moving data from ${uploadDatabaseName} to staging for Streamlit upload...`);

    // Use stored procedure to dynamically handle column mapping
    const storedProcSQL = `CALL ${uploadDatabaseName}.public.move_streamlit_data_to_staging('${platform}', '${filename}')`;
    console.log("Calling stored procedure:", storedProcSQL);

    await runQueryWithoutBind(storedProcSQL).catch(error => {
        console.error('Error calling stored procedure:', error);
        throw new Error(`Failed to move data to staging: ${error.message || error}`);
    });

    console.log(`✓ Data moved to staging successfully`);
}
```

**Status:** ✅ Code updated, needs deployment to AWS

## Deployment Steps

### Step 1: Deploy Lambda to AWS

Navigate to the lambda directory:
```bash
cd /Users/tayloryoung/work/nosey/aws/lambda-master/register-data-processing-lambda
```

#### Option A: AWS Console (Manual)
1. Go to AWS Lambda Console
2. Find lambda: `register-data-processing-lambda`
3. Go to "Code" tab
4. Update `snowflake-helpers.js` with the new code
5. Click "Deploy"

#### Option B: AWS CLI (Recommended)
```bash
# Create deployment package
zip -r function.zip . -x "*.git*" "node_modules/*" "package-lock.json"

# Deploy (replace with actual lambda name and region)
aws lambda update-function-code \
    --function-name register-data-processing-lambda \
    --zip-file fileb://function.zip \
    --region us-east-1
```

### Step 2: Clean Up Previous Test Data

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

### Step 3: Test End-to-End

1. **Upload via Streamlit:**
   - Go to "Load Data to Platform Viewership" tab
   - Select: Platform=Tubi, Partner=DEFAULT, Year=2025, Quarter=Q3, Month=July
   - Upload `tubi_vod_july.csv`
   - Click "Load Data"

2. **Monitor Lambda Logs:**
   - Should see: `Calling stored procedure: CALL upload_db.public.move_streamlit_data_to_staging('Tubi', 'tubi_vod_july.csv')`
   - Should see: `✓ Data moved to staging successfully`
   - Should see all phases complete successfully

3. **Verify Data:**
```sql
-- Check upload_db has data with Partner/Channel
SELECT partner, channel, COUNT(*)
FROM upload_db.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY partner, channel;
-- Expected: partner='Tubi VOD', channel='VOD', count=100

-- Check staging has all columns populated
SELECT phase, processed,
       COUNT(*) as total,
       COUNT(deal_parent) as has_deal_parent,
       COUNT(week) as has_week,
       COUNT(day) as has_day
FROM test_staging.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY phase, processed;
-- Expected: phase='2', processed=TRUE, all counts should match

-- Check final table has revenue records
SELECT COUNT(*), label,
       COUNT(revenue) as has_revenue,
       COUNT(revenue_amount) as has_revenue_amount
FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY label;
-- Expected: label='Revenue', count=matched_records, has_revenue>0
```

## What This Fixes

✅ **Dynamic column mapping** - No more hardcoded column lists
✅ **Schema-agnostic** - Automatically adapts to table changes
✅ **Follows existing patterns** - Uses stored procedures like legacy S3 path
✅ **Handles Streamlit-specific logic** - Filters by phase/processed correctly

## Previous Fixes (Now Complete)

1. ✅ Template hardcoded values (Partner/Channel) now save correctly
2. ✅ Validation distinguishes Revenue from Viewership data
3. ✅ Lambda calls `handle_final_insert_dynamic_generic` (not old version)
4. ✅ Lambda uses dynamic column mapping (not hardcoded lists)

## Complete Data Flow for Revenue-by-Episode (Streamlit)

```
1. Template Creation
   ↓ Partner="Tubi VOD", Channel="VOD" saved as {"hardcoded_value": "value"}

2. File Upload via Streamlit
   ↓ Data inserted to upload_db.public.platform_viewership
   ↓ Partner/Channel columns populated with hardcoded values
   ↓ Phase=NULL, Processed=NULL

3. Lambda Invoked (jobType="Streamlit", TYPE="Revenue")
   ↓ Initial verification: Checks upload_db has expected record count

4. Phase 0: Move to Staging
   ↓ CALL move_streamlit_data_to_staging('Tubi', 'tubi_vod_july.csv')
   ↓ Dynamically fetches columns from INFORMATION_SCHEMA
   ↓ INSERT INTO test_staging SELECT * FROM upload_db WHERE...
   ↓ Verification: Checks test_staging has expected records at phase='0'
   ↓ Mark upload_db records as processed=TRUE

5. Phase 2: Asset Matching
   ↓ CALL set_internal_series_dynamic('Tubi', 'tubi_vod_july.csv')
   ↓ CALL analyze_and_process_viewership_data('Tubi', 'tubi_vod_july.csv')
   ↓ Sets: ref_id, asset_series, asset_title, content_provider
   ↓ CALL set_phase_generic('Tubi', '2', 'tubi_vod_july.csv')
   ↓ Verification: Checks test_staging has expected records at phase='2'

6. Phase 3: Validation & Final Insert
   ↓ CALL handle_final_insert_dynamic_generic('Tubi', 'Revenue', 'tubi_vod_july.csv')
   ↓ CALL validate_viewership_for_insert('Tubi', 'tubi_vod_july.csv', 'Revenue')
   ↓   → Revenue path: Checks revenue, revenue>0, deal_parent, week, day
   ↓ CALL move_to_final_table_dynamic_generic('Tubi', 'Revenue', 'tubi_vod_july.csv')
   ↓   → INSERT INTO episode_details with label='Revenue'
   ↓ Final verification: Checks episode_details has expected records
   ↓ Mark test_staging records as processed=TRUE
   ↓ Send confirmation email

✅ Complete!
```

## Rollback Plan

If deployment fails, revert lambda's `moveDataToStaging` function to call the old stored procedure:

```javascript
const storedProcSQL = `CALL ${uploadDatabaseName}.public.move_viewership_to_staging('${platform}', '${filename}')`;
```

(Note: The old procedure may not work for Streamlit path, but it's better than nothing)

## Questions?

Contact: Taylor Young
