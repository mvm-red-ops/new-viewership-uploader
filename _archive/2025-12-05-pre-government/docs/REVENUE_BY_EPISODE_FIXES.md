# Revenue-by-Episode Implementation Fixes

## Summary
Fixed the revenue-by-episode upload pipeline to properly handle revenue data throughout the entire process, from template creation to final database insertion.

## Problems Fixed

### 1. Hardcoded Template Values Not Saving Correctly
**Problem:** When creating templates with hardcoded Partner/Channel values (e.g., "Tubi VOD", "VOD"), they were saved as simple strings instead of the proper format, causing them to be treated as column names.

**Fix:** Updated `app.py` to save hardcoded values in dict format:
- `app.py:1698, 1702` - Required columns now save as `{"hardcoded_value": "value"}`
- `app.py:1855` - Optional columns now save as `{"hardcoded_value": "value"}`
- `app.py:2648-2655` - Removed skip logic that prevented template values from being processed

**Templates Updated:**
- ✅ Tubi VOD (Revenue type)
- ✅ Freevee VOD (Viewership type)

### 2. Validation Checking Wrong Metrics for Revenue Data
**Problem:** The validation procedure checked for `tot_mov` (minutes of viewership) and `tot_hov` (hours of viewership) for ALL data types, including Revenue uploads where these columns don't exist.

**Fix:** Updated validation to accept TYPE parameter and validate appropriately:

**Files Changed:**
1. `sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql`
   - Added `data_type VARCHAR DEFAULT 'Viewership'` parameter
   - Split validation into two paths:
     - **Viewership:** Checks `tot_mov`, `tot_hov`, `deal_parent`, `week`, `day`
     - **Revenue:** Checks `revenue`, `revenue > 0`, `deal_parent`, `week`, `day`

2. `snowflake/stored_procedures/4.table_migrations/handle_final_insert_dynamic_generic.sql`
   - Line 120: Now passes TYPE parameter to validation: `[PLATFORM, FILENAME, TYPE]`

3. `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql`
   - Line 1914: Updated to pass TYPE parameter

**Deployed to:** ✅ STAGING environment

## What Already Worked

The following components were already correctly handling revenue:

1. **Final Insert Logic** (`move_to_final_table_dynamic_generic.sql`)
   - Lines 20-48: Inserts viewership records when `type.includes("viewership")`
   - Lines 52-116: Inserts revenue records when `type.includes("revenue")`
   - Correctly checks for `revenue IS NOT NULL AND revenue > 0`

2. **Lambda Handler** (`aws/lambda-master/register-data-processing-lambda/index.js`)
   - Properly passes TYPE parameter through the entire pipeline
   - Supports: `Revenue`, `Viewership`, `Viewership_Revenue`, `Payment`

3. **Pluto Sanitization**
   - Already sanitizes the `revenue` column for numeric data

## Data Flow for Revenue-by-Episode

```
1. Template Creation (Streamlit)
   ↓ Partner/Channel saved as {"hardcoded_value": "value"}

2. File Upload (Streamlit)
   ↓ Data transformed with hardcoded values applied
   ↓ Inserted into upload_db.public.platform_viewership

3. Lambda Invoked (TYPE = "Revenue")
   ↓ Moves data to test_staging.public.platform_viewership
   ↓ Phase 0: Data copied

4. Phase 1: Normalization
   ↓ Territory, Channel, Deal Parent mapping
   ↓ Date columns set (week, day, quarter, year, month)

5. Phase 2: Asset Matching
   ↓ Sets ref_id, asset_series, asset_title, content_provider
   ↓ Unmatched records logged to record_reprocessing_batch_logs

6. Phase 3: Validation & Final Insert
   ↓ Validation checks revenue-specific fields
   ↓ If valid:
       ↓ INSERT into staging_assets.public.episode_details_test_staging
       ↓ Label = 'Revenue'
       ↓ Revenue columns populated:
          - register_name, payment_amount, revenue_amount
          - payment_date, payment_quarter, payment_year, payment_month
          - payment_support_category = 'Revenue'
```

## Testing Instructions

### 1. Clean up previous test data
```sql
-- Run these to clean up the failed Tubi test:
DELETE FROM upload_db.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM test_staging.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
WHERE filename = 'tubi_vod_july.csv';
```

### 2. Re-upload Tubi Revenue Data
1. Go to Streamlit app → "Load Data to Platform Viewership" tab
2. Select:
   - Platform: Tubi
   - Partner: DEFAULT
   - Channel: VOD (optional)
   - Year: 2025
   - Quarter: Q3
   - Month: July
3. Upload `tubi_vod_july.csv`
4. Click "Load Data"

### 3. Verify Success
```sql
-- Check upload_db has records with Partner and Channel populated
SELECT partner, channel, COUNT(*)
FROM upload_db.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY partner, channel;
-- Expected: partner = 'Tubi VOD', channel = 'VOD'

-- Check test_staging processing
SELECT phase, processed, COUNT(*)
FROM test_staging.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY phase, processed;
-- Expected: phase = '2' (or empty if moved to final), processed = TRUE

-- Check final table has revenue records
SELECT COUNT(*), label
FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv'
GROUP BY label;
-- Expected: label = 'Revenue', with revenue columns populated

-- Check unmatched records (if any)
SELECT COUNT(*)
FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
WHERE filename = 'tubi_vod_july.csv';
-- This is normal - unmatched content goes here for later processing
```

## Type Definitions

The system supports these data types:
- **`Viewership`** - Standard viewership data (hours/minutes watched)
- **`Revenue`** - Revenue by episode data
- **`Viewership_Revenue`** - Combined type (e.g., Pluto with both metrics)
- **`Payment`** - Finance/payment manager path (different pipeline, not covered here)

## Files Modified

### App Code
- `app.py` - Template save logic (lines 1698, 1702, 1855, 2648-2655)
- `update_tubi_template.py` - Script to fix Tubi template
- `update_freevee_template.py` - Script to fix Freevee template

### SQL Templates
- `sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql` - Validation logic
- `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` - Deployment template

### Stored Procedures
- `snowflake/stored_procedures/4.table_migrations/handle_final_insert_dynamic_generic.sql`

### Database Updates
- Dictionary table: Fixed column_mappings for Tubi and Freevee templates

## Next Steps

1. ✅ Clean up test data
2. ✅ Re-upload Tubi revenue file
3. ✅ Verify data flows through all stages correctly
4. ✅ Check that revenue records land in episode_details with correct label
5. If successful, consider deploying to PROD environment

## Deployment Notes

**Currently deployed to:** STAGING only

To deploy to PROD:
```bash
# Drop old validation procedure first
python -c "import snowflake.connector; from config import load_snowflake_config; config = load_snowflake_config(); conn = snowflake.connector.connect(**config); conn.cursor().execute('DROP PROCEDURE IF EXISTS UPLOAD_DB_PROD.public.validate_viewership_for_insert(VARCHAR, VARCHAR)'); conn.close()"

# Deploy
python sql/deploy/deploy.py --env prod
```

## Questions?

Contact: Taylor Young or refer to the main README for architecture details.
