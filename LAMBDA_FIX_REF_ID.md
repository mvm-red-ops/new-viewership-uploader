# Lambda Fix: Add SET_REF_ID_FROM_PLATFORM_CONTENT_ID to Streamlit Path

## Problem
The Lambda Streamlit processing path skips `NORMALIZE_DATA_IN_STAGING` and calls individual procedures directly, but **never calls `SET_REF_ID_FROM_PLATFORM_CONTENT_ID`**, causing ref_id to remain NULL and asset matching to fail.

## Solution
Replace the individual procedure calls in the Streamlit path with a single call to `NORMALIZE_DATA_IN_STAGING`.

## Current Lambda Code (BROKEN)
```python
# Streamlit processing path - skips Phase 0 & 1
logger.info("Using Streamlit processing path (skip Phase 0 & 1)")

# Individual procedure calls
call_stored_procedure(cursor, "set_deal_parent_generic", [platform, filename])
call_stored_procedure(cursor, "set_channel_generic", [platform, filename])
call_stored_procedure(cursor, "set_territory_generic", [platform, filename])
call_stored_procedure(cursor, "set_deal_parent_normalized_generic", [platform, filename])
call_stored_procedure(cursor, "send_unmatched_deals_alert", [platform, filename])
call_stored_procedure(cursor, "SET_INTERNAL_SERIES_WITH_EXTRACTION", [platform, filename])
call_stored_procedure(cursor, "set_internal_series_generic", [platform, filename])
call_stored_procedure(cursor, "analyze_and_process_viewership_data_generic", [platform, filename])
call_stored_procedure(cursor, "set_phase_generic", [platform, '2', filename])
# ❌ SET_REF_ID_FROM_PLATFORM_CONTENT_ID is NEVER called
```

## Fixed Lambda Code
```python
# Streamlit processing path - use normalization wrapper
logger.info("Using Streamlit processing path (skip Phase 0)")

# Call wrapper procedure that includes SET_REF_ID_FROM_PLATFORM_CONTENT_ID
call_stored_procedure(cursor, "NORMALIZE_DATA_IN_STAGING_GENERIC", [platform, filename])

# Then continue with asset matching
call_stored_procedure(cursor, "analyze_and_process_viewership_data_generic", [platform, filename])
call_stored_procedure(cursor, "set_phase_generic", [platform, '2', filename])
```

## What NORMALIZE_DATA_IN_STAGING_GENERIC Does
This wrapper procedure (already deployed to `UPLOAD_DB_PROD.PUBLIC`) calls:

1. `SET_DEAL_PARENT_GENERIC(platform, filename)` - sets deal_parent, partner, channel, territory
2. **`SET_REF_ID_FROM_PLATFORM_CONTENT_ID(platform, filename)`** - THE MISSING STEP!
3. `CALCULATE_VIEWERSHIP_METRICS(platform, filename)` - calculates TOT_MOV from TOT_HOV
4. `SET_DATE_COLUMNS_DYNAMIC(platform, filename)` - sets week, day, quarter, year, month
5. `SET_PHASE_GENERIC(platform, '1', filename)` - marks as normalized

## Benefits
1. ✅ Fixes ref_id not being set in production
2. ✅ Single source of truth for normalization steps
3. ✅ Less Lambda code to maintain
4. ✅ Easier to add/remove normalization steps in the future

## File Location
The procedure is in: `snowflake/stored_procedures/production/generic/normalize_data_in_staging_generic.sql`

Already deployed to: `UPLOAD_DB_PROD.PUBLIC.NORMALIZE_DATA_IN_STAGING_GENERIC(VARCHAR, VARCHAR)`

## Testing
After deploying the Lambda change, test with:
```python
CALL UPLOAD_DB_PROD.PUBLIC.NORMALIZE_DATA_IN_STAGING_GENERIC('Tubi', 'tubi_vod_july.csv')
```

Then verify ref_id is set:
```sql
SELECT COUNT(*)
FROM NOSEY_PROD.PUBLIC.platform_viewership
WHERE filename = 'tubi_vod_july.csv' AND ref_id IS NOT NULL
```

Should return 100/100 records with ref_id set.
