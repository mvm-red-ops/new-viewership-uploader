# Governance Meeting - December 9, 2025

**Attendees:** Snowflake Governor, Streamlit Governor, Lambda Governor, Testing Governor
**Status:** BRIEFING ON RECENT FIXES
**Issue:** Pluto Latin America Q3 2025 data processing failures

---

## Executive Summary

Three critical issues were identified and resolved for Pluto Latin America Q3 2025 revenue data:

1. **YYYYMMDD Date Format Parsing** - Dates in format `20250701` (as integers) were failing to parse, defaulting to Unix epoch (1970-08-23)
2. **DATA_TYPE Column Missing** - Template configuration wasn't saving the `data_type` field, causing Lambda type parameter to be undefined
3. **Territory Mapping Incorrect** - `active_deals` was mapping Latin America → United States instead of Latin America → Latin America

All issues have been fixed and deployed to both staging and production.

---

## Issue 1: YYYYMMDD Date Parsing Failure

### Snowflake Governor

**Impact:** SET_DATE_COLUMNS_DYNAMIC stored procedure was extracting year=1970 from placeholder dates and overwriting correct year values from Lambda payload.

**Changes Deployed:**
- **File:** `snowflake/stored_procedures/staging/generic/set_date_columns_dynamic.sql:18-28`
- **File:** `snowflake/stored_procedures/production/generic/set_date_columns_dynamic.sql:18-28`
- **Change:** Added year validation `YEAR(TRY_CAST(date AS DATE)) >= 2000` to exclude placeholder dates
- **Reasoning:** Unix epoch dates (1970-08-23) indicate failed parsing, not real data

**Before:**
```javascript
var checkDateQuery = `
    SELECT COUNT(*) as cnt
    FROM TEST_STAGING.public.platform_viewership
    WHERE platform = '${platform}'
      AND filename = '${filename}'
      AND date IS NOT NULL
      AND TRIM(date) != ''
`;
```

**After:**
```javascript
var checkDateQuery = `
    SELECT COUNT(*) as cnt
    FROM TEST_STAGING.public.platform_viewership
    WHERE platform = '${platform}'
      AND filename = '${filename}'
      AND date IS NOT NULL
      AND TRIM(date) != ''
      AND YEAR(TRY_CAST(date AS DATE)) >= 2000  // ← ADDED
`;
```

### Streamlit Governor

**Impact:** Database Write Delegate wasn't detecting YYYYMMDD dates when pandas read them as integers.

**Changes Deployed:**
- **File:** `src/snowflake_utils.py:735-748`
- **Change:** Enhanced date parsing to handle both string "20250701" and integer 20250701 formats
- **Reasoning:** Pandas type inference can read 8-digit dates as integers, floats, or strings depending on context

**Code Added:**
```python
elif isinstance(val, (int, float)) and not pd.isna(val):
    # Handle numeric dates (e.g., 20250701 as integer)
    try:
        val_str = str(int(val))  # Convert to string, remove decimals
        if len(val_str) == 8:
            # YYYYMMDD format as integer
            parsed_date = pd.to_datetime(val_str, format='%Y%m%d', errors='coerce')
            formatted_values.append(f"'{parsed_date.strftime('%Y-%m-%d')}'")
        else:
            # Try general parsing
            parsed_date = pd.to_datetime(val)
            formatted_values.append(f"'{parsed_date.strftime('%Y-%m-%d')}'")
    except:
        formatted_values.append(f"'{str(val)}'")
```

### Lambda Governor

**Impact:** No code changes required. Lambda relies on correct dates from Streamlit and date normalization from Snowflake procedures.

**Note for Future:** Phase 1 verification failures ("Unable to verify the correct record count and normalization") can indicate date parsing issues upstream. Check Snowflake date columns and Streamlit parsing logs.

### Testing Governor

**Regression Test Required:**
- Platform: Any
- Date Format: YYYYMMDD as integer (20250701)
- Expected: Dates parse correctly as 2025-07-01, NOT 1970-08-23
- Verification Query:
```sql
SELECT MIN(YEAR(TRY_CAST(date AS DATE))), MAX(YEAR(TRY_CAST(date AS DATE)))
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE filename = '[test_file]';
```
- Expected Result: Both MIN and MAX should be >= 2000

---

## Issue 2: DATA_TYPE Column Not Saved

### Streamlit Governor

**Impact:** Template configurations were not saving the `data_type` field to `DICTIONARY.PUBLIC.VIEWERSHIP_FILE_FORMATS`.

**Changes Deployed:**
- **File:** `src/snowflake_utils.py:127,153` (insert_config)
- **File:** `src/snowflake_utils.py:209,232` (update_config)
- **Change:** Added `data_type` to INSERT and UPDATE SQL statements
- **Reasoning:** Lambda requires this field to determine whether data is Revenue or Viewership for pipeline orchestration

**Before:**
```python
insert_sql = f"""
INSERT INTO dictionary.public.viewership_file_formats (
    config_id, platform, partner, channel, ...
)
SELECT %s, %s, %s, %s, ...
"""
```

**After:**
```python
insert_sql = f"""
INSERT INTO dictionary.public.viewership_file_formats (
    config_id, platform, partner, channel, ..., data_type  # ← ADDED
)
SELECT %s, %s, %s, %s, ..., %s  # ← ADDED
"""
```

### Lambda Governor

**Impact:** Type parameter was undefined in Lambda payload, causing SQL compilation errors.

**Symptom:** Lambda logs showed `SQL (original): undefined` and `MissingParameterError`

**Resolution:** Now receives correct type parameter ('Revenue' or 'Viewership') from template configuration.

**Note for Future:** If Lambda fails with "undefined" parameter errors, check that Streamlit is saving all required template fields.

### Snowflake Governor

**Impact:** No stored procedure changes required.

**Note:** Procedures expect correct type parameter from Lambda for validation queries.

### Testing Governor

**Verification Required:**
- Create new template in Streamlit
- Set `data_type = 'Revenue'`
- Save template
- Query: `SELECT data_type FROM DICTIONARY.PUBLIC.VIEWERSHIP_FILE_FORMATS WHERE config_id = '[new_id]'`
- Expected: Should return 'Revenue', NOT NULL

---

## Issue 3: Territory Mapping Incorrect

### Snowflake Governor

**Impact:** `DICTIONARY.PUBLIC.active_deals` table had wrong internal_territory mapping for Pluto Latin America.

**Database Fix Applied:**
```sql
-- BEFORE
Platform: Pluto
Platform Territory: Latin America
Internal Territory: United States  # ← WRONG
Internal Territory ID: 1           # ← WRONG

-- AFTER
Platform: Pluto
Platform Territory: Latin America
Internal Territory: Latin America  # ← CORRECT
Internal Territory ID: 3          # ← CORRECT
```

**How Territory Mapping Works:**
- NULL territories in `active_deals` do NOT act as wildcards for data with specific territories
- Data territory must exactly match active_deals territory, or data territory must be NULL to match any active_deals territory
- Example:
  - Data: territory='Latin America' + active_deals: territory=NULL = NO MATCH
  - Data: territory='Latin America' + active_deals: territory='Latin America' = MATCH
  - Data: territory=NULL + active_deals: territory='Latin America' = MATCH (data wildcard)

**Future Reference:**
When data fails to set `deal_parent`, check for territory mismatches:
1. Query data territories: `SELECT DISTINCT platform_territory FROM TEST_STAGING.PUBLIC.platform_viewership WHERE platform = 'X' AND deal_parent IS NULL`
2. Query active_deals: `SELECT platform_territory FROM DICTIONARY.PUBLIC.active_deals WHERE platform = 'X'`
3. Create missing territory-specific entries in active_deals

### Streamlit Governor

**Impact:** No code changes required.

**Note:** Streamlit correctly writes user-selected territory to database. The issue was in the lookup table mapping.

### Lambda Governor

**Impact:** No code changes required.

**Note:** Lambda orchestrates stored procedures that use active_deals for territory mapping. When procedures fail to set deal_parent, investigate active_deals configuration.

### Testing Governor

**Verification Required:**
- Upload data with specific territory (e.g., 'Latin America')
- Run Phase 1 normalization
- Query: `SELECT platform_territory, deal_parent, internal_territory FROM STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING WHERE filename = '[test_file]'`
- Expected: internal_territory should match platform_territory (Latin America → Latin America)

---

## Deployment Status

**Committed:** December 9, 2025
**Branches:** main (prod) and staging
**Commit:** cf57a23

**Files Changed:**
1. `src/snowflake_utils.py` - Date parsing and DATA_TYPE field
2. `snowflake/stored_procedures/staging/generic/set_date_columns_dynamic.sql` - Year validation
3. `snowflake/stored_procedures/production/generic/set_date_columns_dynamic.sql` - Year validation
4. Governor README files - Documentation updates

**Database Changes:**
1. Updated `DICTIONARY.PUBLIC.active_deals` for Pluto Latin America (internal_territory and internal_territory_id)

---

## Action Items

### Lambda Governor
- [ ] Monitor phase 1 verification errors for date-related failures
- [ ] Document type parameter dependency on Streamlit template configuration

### Testing Governor
- [ ] Add regression test for YYYYMMDD integer date parsing
- [ ] Add verification test for DATA_TYPE column persistence
- [ ] Add test for territory mapping correctness
- [ ] Update deployment checklist with these verification steps

### Snowflake Governor
- [ ] Document active_deals territory matching logic in delegate knowledge base
- [ ] Create troubleshooting guide for deal_parent NULL issues

### Streamlit Governor
- [ ] Verify all template fields are being saved correctly
- [ ] Add logging for date format detection to aid debugging

---

## Next Pluto Latin America Upload

**Status:** Data reset and ready for reprocessing
- 322 records in TEST_STAGING.PUBLIC.platform_viewership at phase 0
- 0 records in EPISODE_DETAILS_TEST_STAGING (old incorrect data deleted)
- Active_deals mapping corrected

**To Reprocess:**
1. Go to Streamlit and trigger Lambda for "Pluto LatAm Q3 2025.xlsx - 1.csv"
2. Lambda will execute with type='Revenue'
3. Expected result: 270-322 records inserted with correct dates (2025-07-01) and territory (Latin America)

---

## Governance Notes

**Decision Authority:**
- Snowflake Governor approved stored procedure changes (within established patterns)
- Streamlit Governor approved data handling enhancements (no breaking changes)
- Database configuration change (active_deals) approved as bug fix

**Cross-Governor Coordination:**
- All governors briefed on issues and resolutions
- No breaking changes to inter-component contracts
- Testing requirements established for regression prevention
