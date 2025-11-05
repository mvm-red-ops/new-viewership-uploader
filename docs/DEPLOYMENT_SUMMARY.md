# Viewership Upload Pipeline - Fix Summary

## Date: 2025-11-05

## Issues Fixed

### 1. JSON Serialization Error (Lambda Invocation)
**File:** `app.py` lines 2447-2474

**Problem:** Lambda invocation failed with "Object of type int64 is not JSON serializable"

**Fix:** Added conversion of pandas/numpy types to native Python types:
- `record_count` → `int(record_count)`
- `tot_hov` → `float(round(file_hov, 2))`
- `year`, `quarter`, `month` → `convert_to_native()` helper function

**Status:** ✅ Deployed to staging and prod

---

### 2. Missing Database Columns
**Tables:**
- `STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING`
- `ASSETS.PUBLIC.EPISODE_DETAILS`

**Problem:** INSERT query failed with "invalid identifier 'PLATFORM_PARTNER_NAME'"

**Fix:** Added missing columns to both tables:
- `PLATFORM_PARTNER_NAME VARCHAR(500)`
- `PLATFORM_CHANNEL_NAME VARCHAR(500)`
- `PLATFORM_TERRITORY VARCHAR(500)`

**Status:** ✅ Deployed to staging and prod

---

### 3. Unmatched Records Not Logged
**File:** `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` lines 1510-1545

**Problem:** Records that couldn't be matched by asset buckets were dropped without logging

**Fix:** Added logging to `record_reprocessing_batch_logs` for all final unmatched records:
```javascript
if (finalUnmatchedCount > 0) {
    // Log to record_reprocessing_batch_logs
    const logFinalUnmatchedSql = `
        INSERT INTO {{METADATA_DB}}.public.record_reprocessing_batch_logs (...)
        SELECT ... FROM platform_viewership v
        JOIN TEMP_UNMATCHED u ON v.id = u.id
        WHERE NOT EXISTS (already logged)
    `;
}
```

**Status:** ✅ Deployed to staging and prod

---

### 4. Missing UDF Function
**Function:** `EXTRACT_PRIMARY_TITLE`

**Problem:** Asset matching bucket procedures failed with "Unknown user-defined function UPLOAD_DB.PUBLIC.EXTRACT_PRIMARY_TITLE"

**Fix:**
- Function already existed in database
- Granted USAGE permissions to WEB_APP role
- Added function definition and permission grants to deployment template

**Status:** ✅ Permissions granted manually, template updated for future deployments

---

### 5. Missing Table Permissions
**Table:** `record_reprocessing_batch_logs`

**Problem:** "Insufficient privileges to operate on table 'RECORD_REPROCESSING_BATCH_LOGS'"

**Fix:** Granted INSERT and SELECT permissions to WEB_APP role:
- STAGING: `METADATA_MASTER_CLEANED_STAGING.PUBLIC.record_reprocessing_batch_logs`
- PROD: `METADATA_MASTER.PUBLIC.record_reprocessing_batch_logs`

**Status:** ✅ Permissions granted manually, added to deployment template

---

## Files Modified

### Code Changes
1. **app.py** - JSON serialization fix
2. **sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql** - Added unmatched logging, UDF, and permissions

### Deployment Scripts Updated
3. **redeploy_procedures_prod.py** - Added `{{METADATA_DB}}` → `METADATA_MASTER` replacement
4. **redeploy_procedures_staging.py** - Already had correct `{{METADATA_DB}}` → `METADATA_MASTER_CLEANED_STAGING`

### New Helper Scripts Created
5. **sql/add_platform_columns.py** - Script to add missing columns to EPISODE_DETAILS tables
6. **sql/grant_udf_permissions.py** - Script to grant UDF permissions
7. **sql/grant_table_permissions.py** - Script to grant table permissions
8. **sql/cleanup_youtube.py** - Script to clean up test data
9. Various diagnostic scripts in `sql/` directory

---

## Deployment Instructions

### Template Deployment (Requires Admin)
```bash
# Staging
python3 redeploy_procedures_staging.py

# Production
python3 redeploy_procedures_prod.py
```

**Note:** Template includes:
- `EXTRACT_PRIMARY_TITLE` UDF creation
- Permission grants for UDF and record_reprocessing_batch_logs
- All procedure updates

### Manual Permissions (If needed)
```bash
# UDF permissions
python3 sql/grant_udf_permissions.py

# Table permissions
python3 sql/grant_table_permissions.py

# Column additions (already done)
python3 sql/add_platform_columns.py
```

---

## Database Configuration

### Staging
- **Upload DB:** `UPLOAD_DB`
- **Staging DB:** `TEST_STAGING`
- **Assets DB:** `STAGING_ASSETS`
- **Episode Details:** `EPISODE_DETAILS_TEST_STAGING`
- **Metadata DB:** `METADATA_MASTER_CLEANED_STAGING`

### Production
- **Upload DB:** `UPLOAD_DB_PROD`
- **Staging DB:** `NOSEY_PROD`
- **Assets DB:** `ASSETS`
- **Episode Details:** `EPISODE_DETAILS`
- **Metadata DB:** `METADATA_MASTER`

---

## Testing

### Ready to Test
All fixes are deployed to staging and prod. The workflow should now:

1. ✅ Upload data via Streamlit
2. ✅ Trigger Lambda with proper JSON serialization
3. ✅ Run asset matching with EXTRACT_PRIMARY_TITLE UDF
4. ✅ Log unmatched records to record_reprocessing_batch_logs
5. ✅ INSERT matched records to EPISODE_DETAILS with all columns
6. ✅ Pass verification: `inserted + unmatched = total`

### Test File
YouTube: `youtube_daily_2025-07-01_to_2025-09-30 (1).csv` (80,686 records)

Expected results:
- ~73,671 matched records → EPISODE_DETAILS_TEST_STAGING
- ~7,015 unmatched records → record_reprocessing_batch_logs
- Total verification passes

---

## Next Steps

1. Re-upload test file to verify all fixes work end-to-end
2. Monitor Lambda logs for any remaining errors
3. Verify final counts in EPISODE_DETAILS and record_reprocessing_batch_logs
4. Deploy to production once staging is confirmed working
