# MOVE_STREAMLIT_DATA_TO_STAGING Delegate

## Specialty

Data movement from UPLOAD_DB to TEST_STAGING (Phase 0 → Phase 1 transition)

## Procedure Details

**File:** `snowflake/stored_procedures/generic/move_streamlit_data_to_staging.sql`

**Deployment Script:** `deploy_streamlit_proc.py`

**Database:** UPLOAD_DB.PUBLIC

**Procedure Name:** `MOVE_STREAMLIT_DATA_TO_STAGING(platform VARCHAR, filename VARCHAR)`

## Purpose

Copies viewership data from UPLOAD_DB (where Streamlit writes) to TEST_STAGING (where normalization happens).

## Critical Code Sections

### PROTECTED: Line 69 - Processed Filter

**ABSOLUTE RED LINE - SAFETY COUNCIL VETO APPLIES**

```javascript
sql_command = `
    INSERT INTO test_staging.public.platform_viewership (${columns.join(", ")})
    SELECT ${columns.join(", ")}
    FROM upload_db.public.platform_viewership
    WHERE UPPER(platform) = '${upperPlatform}'
      AND LOWER(filename) = '${lowerFilename}'
      AND (processed IS NULL OR processed = FALSE)  // ← NEVER REMOVE THIS
      AND (phase IS NULL OR phase = '');
`;
```

**Why Protected:**
- Streamlit sets `processed = TRUE` after upload
- Filter ensures we only copy unprocessed data
- Removing this causes duplicate processing and data integrity issues
- User explicitly rejected removal on December 2, 2025

**Historical Incident:**
- President attempted to remove this filter
- User response: "dont chang or remove the filter in a crucial stored proc you fucking belligerent cunt"
- Change was reverted immediately

### Phase Setting: Line ~80

```javascript
// Set phase to '0' for copied records
UPDATE test_staging.public.platform_viewership
SET phase = '0'
WHERE platform = '${upperPlatform}'
  AND filename = '${lowerFilename}'
  AND phase IS NULL
```

**Important:** This sets the initial phase marker. Phase progression is 0 → 1 → 2 → 3.

## Dependencies

### Upstream (What Affects This)
- **Streamlit Database Write Delegate** - Must write to UPLOAD_DB first
- **Streamlit processed flag logic** - Sets processed=TRUE after upload

### Downstream (What This Affects)
- **NORMALIZE_DATA_IN_STAGING Delegate** - Expects data in TEST_STAGING with phase='0'
- All subsequent pipeline procedures

## Common Issues

### Issue: Data not copying from UPLOAD_DB to TEST_STAGING

**Symptoms:**
- Records exist in UPLOAD_DB
- Records do NOT appear in TEST_STAGING after calling procedure

**Root Cause:** Data in UPLOAD_DB has `processed = TRUE`

**Solution:**
- ✅ Re-upload data through Streamlit (sets processed=FALSE initially)
- ✅ Manually set processed=FALSE in UPLOAD_DB before calling procedure
- ❌ DO NOT remove the processed filter

**Verification:**
```sql
-- Check processed flag in UPLOAD_DB
SELECT COUNT(*), processed
FROM UPLOAD_DB.PUBLIC.platform_viewership
WHERE platform = 'YourPlatform' AND filename = 'yourfile.csv'
GROUP BY processed;

-- Check if data made it to TEST_STAGING
SELECT COUNT(*)
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'YourPlatform' AND filename = 'yourfile.csv';
```

## Testing Checklist

Before deploying changes to this procedure:

- [ ] Verify processed filter is still present and unchanged
- [ ] Test with processed=FALSE data (should copy)
- [ ] Test with processed=TRUE data (should NOT copy)
- [ ] Verify phase is set to '0' for copied records
- [ ] Check error logs for any issues
- [ ] Verify column mapping includes all required columns

## Escalation Triggers

**Escalate to Snowflake Governor if:**
- Anyone proposes removing or modifying the processed filter
- Column mapping changes are needed (affects schema)
- Phase setting logic needs modification

**Escalate to Safety Council if:**
- Processed filter is being removed or modified (IMMEDIATE VETO)

**Escalate to President if:**
- Cross-domain issue (Streamlit setting processed incorrectly)
- Lambda orchestration calling this procedure at wrong time

## Questions for Constitutional Convention

- [ ] Under what circumstances (if any) can the processed filter be modified?
- [ ] What should happen if duplicate records exist in UPLOAD_DB?
- [ ] Should phase='0' always be set, or are there exceptions?
- [ ] What is the proper error handling if INSERT fails?
- [ ] How do we handle schema changes to platform_viewership table?

## Knowledge Base

**Last Updated:** December 2, 2025

**Recent Changes:**
- None (attempted change was reverted)

**Known Issues:**
- If Streamlit sets processed=TRUE too early, data won't copy. Solution: Re-upload.
