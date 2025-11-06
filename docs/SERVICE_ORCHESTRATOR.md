# Service: Content Matching Orchestrator

## Purpose
Coordinates the 6-bucket cascade strategy to match viewership records against internal content metadata.

## Location
- **Template:** `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` (starts ~line 1133)
- **Procedure Name:** `ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC`
- **Database:** `UPLOAD_DB` (staging) or `UPLOAD_DB_PROD` (production)

## Function Signature
```sql
PROCEDURE ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(
    PLATFORM VARCHAR,
    FILENAME VARCHAR,
    TITLES_ARRAY ARRAY DEFAULT null  -- Optional: specific titles to process
)
RETURNS VARCHAR
```

## Input Parameters

### `PLATFORM`
- Required: Yes
- Type: VARCHAR
- Examples: 'Philo', 'WURL', 'Pluto', 'YouTube'
- Used to filter records in `platform_viewership` table

### `FILENAME`
- Required: Yes
- Type: VARCHAR
- Examples: '20251106_103826.csv'
- Used to filter records for this specific upload batch

### `TITLES_ARRAY`
- Required: No (default: null)
- Type: ARRAY
- Purpose: Process only specific titles (used for debugging/reprocessing)
- Example: `['Show Name 1', 'Show Name 2']`

## Core Algorithm

### 1. Initialization (lines ~1145-1360)
```javascript
// Define which table to process
const viewershipTable = `{{STAGING_DB}}.public.platform_viewership`;

// Define base filtering conditions
const baseConditions = `
    platform = '${platformArg}'
    AND processed IS NULL
    AND content_provider IS NULL
    -- NOTE: platform_content_name NOT required here (SERIES_SEASON_EPISODE doesn't need it)
    ${filenameArg ? `AND filename = '${filenameArg}'` : ''}
`;

// Create TEMP_UNMATCHED table with ALL unprocessed records
CREATE OR REPLACE TABLE TEMP_${PLATFORM}_UNMATCHED AS
SELECT DISTINCT id
FROM platform_viewership
WHERE ${baseConditions};
```

**Key Decision:** We create `TEMP_UNMATCHED` with ALL records that need processing. This table acts as a queue - records are deleted as they're successfully matched.

### 2. Bucket Processing Loop (lines ~1362-1537)
```javascript
const bucketOrder = [
    "FULL_DATA",
    "REF_ID_SERIES",
    "REF_ID_ONLY",
    "SERIES_SEASON_EPISODE",
    "SERIES_ONLY",
    "TITLE_ONLY"
];

for (const bucketType of bucketOrder) {
    // Check how many unmatched records remain
    SELECT COUNT(*) FROM TEMP_${PLATFORM}_UNMATCHED;

    if (unmatchedCount === 0) continue;  // All records matched, done!

    // Create bucket-specific temp table
    CREATE TEMPORARY TABLE TEMP_${PLATFORM}_${BUCKET}_BUCKET AS
    SELECT u.id
    FROM TEMP_${PLATFORM}_UNMATCHED u
    JOIN platform_viewership v ON u.id = v.id
    WHERE v.platform = '${platform}'
      AND [bucket-specific conditions];

    // Call bucket procedure
    CALL process_viewership_${bucketType}_generic('${platform}', '${filename}');

    // Remove successfully matched records from TEMP_UNMATCHED
    DELETE FROM TEMP_${PLATFORM}_UNMATCHED u
    WHERE EXISTS (
        SELECT 1 FROM platform_viewership v
        WHERE v.id = u.id
        AND v.content_provider IS NOT NULL  -- Match indicator
    );

    // Clean up bucket table
    DROP TABLE TEMP_${PLATFORM}_${BUCKET}_BUCKET;
}
```

### 3. Handle Remaining Unmatched Records (lines ~1545-1602)
```javascript
// Count final unmatched
SELECT COUNT(*) FROM TEMP_${PLATFORM}_UNMATCHED;

if (finalUnmatchedCount > 0) {
    // Log to record_reprocessing_batch_logs for manual review
    INSERT INTO METADATA_DB.public.record_reprocessing_batch_logs (
        title, viewership_id, filename, notes, platform
    )
    SELECT
        v.platform_content_name,
        v.id,
        v.filename,
        'Final unmatched: No bucket could process this record',
        '${platform}'
    FROM platform_viewership v
    JOIN TEMP_${PLATFORM}_UNMATCHED u ON v.id = u.id
    WHERE v.content_provider IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM record_reprocessing_batch_logs l
          WHERE l.viewership_id = v.id
      );
}

// Cleanup
DROP TABLE TEMP_${PLATFORM}_UNMATCHED;
```

## Bucket Creation Logic

Each bucket has specific filtering criteria when creating `TEMP_${PLATFORM}_${BUCKET}_BUCKET`:

### FULL_DATA
```sql
WHERE v.platform = '${platform}'
AND v.platform_content_name IS NOT NULL  -- Title required
AND v.ref_id IS NOT NULL
AND v.internal_series IS NOT NULL
AND v.episode_number IS NOT NULL
AND v.season_number IS NOT NULL
AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
```

### SERIES_SEASON_EPISODE
```sql
WHERE v.platform = '${platform}'
-- NOTE: platform_content_name NOT required!
AND v.internal_series IS NOT NULL
AND v.episode_number IS NOT NULL
AND v.season_number IS NOT NULL
AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
```

### TITLE_ONLY
```sql
WHERE v.platform = '${platform}'
AND v.platform_content_name IS NOT NULL  -- Title required
AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
```

## Success Indicators

### Record Successfully Matched
A record is considered matched when:
```sql
content_provider IS NOT NULL
```

This field is set by bucket procedures when they successfully match a record to metadata.

### Orchestrator Success
Returns string: `"FINAL SUMMARY: Successfully updated {N} total records. Breakdown: FULL_DATA: X, SERIES_SEASON_EPISODE: Y, ..."`

### Orchestrator Failure
Returns string: `"Error in analyze_and_process_viewership_data_generic: {error message}"`

## Error Handling

### Non-Blocking Errors
- Bucket procedure fails → Log error, continue to next bucket
- Conflict handling fails → Log warning, continue
- Sample record logging fails → Log warning, continue

### Blocking Errors
- Can't create TEMP_UNMATCHED → Throw error, halt
- Can't access viewership table → Throw error, halt

## Logging

All execution steps logged to `UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE`:

```sql
INSERT INTO ERROR_LOG_TABLE (
    LOG_TIME,
    LOG_MESSAGE,
    PROCEDURE_NAME,
    PLATFORM,
    STATUS,              -- 'STARTED', 'IN_PROGRESS', 'SUCCESS', 'WARNING', 'ERROR', 'COMPLETED'
    ROWS_AFFECTED,
    ERROR_MESSAGE,
    EXECUTION_TIME       -- Seconds since start
)
```

## Performance Characteristics

### Time Complexity
- O(B × N) where B = buckets with records, N = unmatched records
- Optimizes over time: Each successful bucket reduces N for next bucket

### Typical Execution Times
- 100 records: ~10-30 seconds
- 1,000 records: ~1-3 minutes
- 10,000 records: ~5-15 minutes

Depends on:
- Match distribution across buckets
- Metadata table join complexity
- Snowflake warehouse size

## Common Issues & Solutions

### Issue: "Object TEMP_PHILO_UNMATCHED already exists"
**Cause:** Previous run didn't clean up temp table (error mid-execution)
**Solution:**
```sql
DROP TABLE IF EXISTS UPLOAD_DB_PROD.PUBLIC.TEMP_PHILO_UNMATCHED;
```

### Issue: Unmatched count = 0 but records missing
**Cause:** Records filtered out by baseConditions (e.g., `processed IS NOT NULL`)
**Solution:** Check record's `processed`, `content_provider`, `platform_content_name` values

### Issue: All buckets report 0 matches
**Cause:** Procedure names wrong (missing `_generic` suffix) or database name mismatch
**Solution:** Verify procedure calls at line ~1483:
```javascript
CALL process_viewership_${bucketType.toLowerCase()}_generic(...)  // Must have _generic!
```

### Issue: Records with NULL titles not processed
**Cause:** baseConditions includes `platform_content_name IS NOT NULL` (old bug)
**Solution:** Verify baseConditions at line ~1151 does NOT include platform_content_name check

## Dependencies

### Required Procedures
Must exist before orchestrator runs:
- `process_viewership_full_data_generic`
- `process_viewership_ref_id_series_generic`
- `process_viewership_ref_id_only_generic`
- `process_viewership_series_season_episode_generic`
- `process_viewership_series_only_generic`
- `process_viewership_title_only_generic`

Deploy via: `sql/templates/DEPLOY_GENERIC_CONTENT_REFERENCES.sql`

### Required Tables
- `{{STAGING_DB}}.public.platform_viewership` (source data)
- `{{METADATA_DB}}.public.record_reprocessing_batch_logs` (unmatched logging)
- `{{UPLOAD_DB}}.public.ERROR_LOG_TABLE` (execution logging)
- `{{UPLOAD_DB}}.public.flagged_metadata` (conflict tracking)

### Required Metadata Tables
- `metadata_master_cleaned_staging.public.episode`
- `metadata_master_cleaned_staging.public.series`
- `metadata_master_cleaned_staging.public.metadata`

(or `metadata_master.public.*` in production)

## Testing

### Manual Test
```sql
-- 1. Upload test file via Streamlit (creates records in platform_viewership)

-- 2. Run orchestrator manually
CALL UPLOAD_DB_PROD.public.analyze_and_process_viewership_data_generic(
    'Philo',
    '20251106_103826.csv',
    NULL
);

-- 3. Check results
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END) as matched,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END) as unmatched
FROM NOSEY_PROD.PUBLIC.platform_viewership
WHERE filename = '20251106_103826.csv';

-- 4. Check unmatched logged
SELECT *
FROM METADATA_MASTER.PUBLIC.record_reprocessing_batch_logs
WHERE filename = '20251106_103826.csv';
```

### Debug Specific Title
```sql
CALL UPLOAD_DB_PROD.public.analyze_and_process_viewership_data_generic(
    'Philo',
    '20251106_103826.csv',
    ARRAY_CONSTRUCT('Show Name 1', 'Show Name 2')  -- Only process these titles
);
```

## Maintenance

### Regular Cleanup
```sql
-- Clean up orphaned temp tables (if procedures crashed)
SHOW TABLES LIKE 'TEMP_%' IN UPLOAD_DB_PROD.PUBLIC;
-- Manually drop any found

-- Archive old error logs
DELETE FROM UPLOAD_DB_PROD.PUBLIC.ERROR_LOG_TABLE
WHERE LOG_TIME < DATEADD(month, -3, CURRENT_TIMESTAMP());
```

### Performance Tuning
- Increase Snowflake warehouse size for large batches
- Consider partitioning `platform_viewership` by `platform` if table grows large
- Add indexes on frequently joined columns (Snowflake auto-optimizes but can suggest)

## Change History

### 2025-11-06
- Removed `platform_content_name IS NOT NULL` from baseConditions
- Added `platform_content_name` checks to individual bucket filters (except SERIES_SEASON_EPISODE)
- Allows records without titles to be matched on series+episode+season

### 2025-11-05
- Fixed procedure calls to use `_generic` suffix
- Changed database references to use `{{STAGING_DB}}` placeholder

### Earlier
- Original implementation with platform-specific tables
- Migration to generic platform_viewership architecture
