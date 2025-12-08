# Diagnostic: Temporary Snowflake Scripts

This document indexes **temporary** debugging and analysis scripts. These are one-off or exploratory queries that may be useful for troubleshooting but are not part of the canonical codebase.

## Purpose of Diagnostic Scripts

Diagnostic scripts are:
- **Temporary**: Not meant for long-term use
- **Exploratory**: Understanding data states or debugging issues
- **One-off**: Specific to a particular problem or investigation
- **Non-production**: Should never be deployed as stored procedures

## Common Diagnostic Locations

### `sql/temp_scripts/`
One-off SQL queries for debugging specific issues

### `sql/diagnostics/`
Diagnostic and troubleshooting scripts

### Root-level Test Files
- `test_*.sql` - Quick test queries during development
- `debug_*.sql` - Debug investigation scripts

## Common Diagnostic Patterns

### Check Phase Distribution
```sql
-- See how many records are in each phase
SELECT phase, COUNT(*) as count, processed
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Tubi' AND filename = 'some_file.csv'
GROUP BY phase, processed
ORDER BY phase;
```

### Check Ref_ID Mapping
```sql
-- Verify ref_id was set correctly from platform_content_id
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN ref_id IS NOT NULL THEN 1 ELSE 0 END) as has_ref_id,
    SUM(CASE WHEN platform_content_id IS NOT NULL THEN 1 ELSE 0 END) as has_platform_id
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Tubi' AND filename = 'some_file.csv';
```

### Check Asset Matching Results
```sql
-- See what percentage of records were successfully matched
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN asset_series IS NOT NULL THEN 1 ELSE 0 END) as has_asset_series,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END) as has_content_provider,
    SUM(CASE WHEN asset_title IS NOT NULL THEN 1 ELSE 0 END) as has_asset_title
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE filename = 'some_file.csv';
```

### Sample Matched Records
```sql
-- View examples of successful matches
SELECT
    platform_content_name,
    asset_title,
    asset_series,
    content_provider
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE filename = 'some_file.csv' AND asset_series IS NOT NULL
LIMIT 10;
```

### Check Unmatched Records
```sql
-- Find records that failed to match
SELECT
    platform_content_name,
    platform_content_id,
    ref_id
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE filename = 'some_file.csv'
  AND phase = '2'
  AND (asset_series IS NULL OR content_provider IS NULL);
```

### Verify Data Movement to Final Table
```sql
-- Check if records moved to EPISODE_DETAILS
SELECT COUNT(*) as count
FROM STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING
WHERE platform = 'Tubi' AND filename = 'some_file.csv';
```

### Check Error Logs
```sql
-- View recent errors
SELECT
    LOG_TIMESTAMP,
    PROCEDURE_NAME,
    PLATFORM,
    LOG_MESSAGE
FROM UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE
ORDER BY LOG_TIMESTAMP DESC
LIMIT 20;
```

### Reset Data for Testing
```sql
-- Reset records back to phase 0 for re-processing
UPDATE TEST_STAGING.PUBLIC.platform_viewership
SET phase = '0',
    ref_id = NULL,
    asset_title = NULL,
    asset_series = NULL,
    content_provider = NULL,
    processed = NULL
WHERE filename = 'some_file.csv';
```

## Background Testing Scripts

During active development, background Python scripts may run Snowflake procedure tests. These are indicated by system reminders about background Bash processes.

**Example Pattern**:
```python
# Test full pipeline with specific file
cursor.execute("UPDATE ... SET phase = '0' WHERE filename = 'test.csv'")
cursor.execute("CALL UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING('Tubi', 'test.csv')")
cursor.execute("CALL UPLOAD_DB.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('Tubi', 'test.csv')")
# Check results...
```

## Investigation Workflows

### Bug: NULL ref_id After Processing
1. Check if platform_content_id exists in source data
2. Verify DICTIONARY.PUBLIC.REF_ID_MAPPING has matching entries
3. Check if SET_REF_ID_FROM_PLATFORM_CONTENT_ID was called
4. Review procedure logs in ERROR_LOG_TABLE

### Bug: Asset Matching Not Working
1. Check if ref_id was set correctly
2. Verify STAGING_ASSETS.PUBLIC.ASSETS_MASTER has matching series
3. Check asset_series_alt_names for alternate spellings
4. Review ANALYZE_AND_PROCESS procedure logic
5. Check for string formatting issues (extra spaces, case mismatch)

### Bug: Data Not Moving to Final Table
1. Check if phase = '2' was set
2. Verify asset_series, ref_id, and content_provider are all NOT NULL
3. Check HANDLE_FINAL_INSERT_DYNAMIC_GENERIC procedure logs
4. Verify target table exists and has correct schema
5. Check for permission issues with web_app role

## Cleaning Up Diagnostic Scripts

Diagnostic scripts should be:
- Removed when investigation is complete
- Moved to archive if they document a significant bug
- Never committed to git unless they represent a pattern worth preserving

If a diagnostic query becomes frequently useful, consider:
- Converting it to a stored procedure
- Adding it to TROUBLESHOOTING.md
- Creating a reusable view or helper function

## Production Debugging

When debugging production issues:
1. **Never** modify production data directly
2. Use SELECT queries only
3. Export results for analysis
4. Test fixes in staging first
5. Document findings in issue tracker

## Useful Views for Diagnostics

Consider creating these views for frequent diagnostic needs:

```sql
-- View: Recent uploads summary
CREATE OR REPLACE VIEW UPLOAD_SUMMARY AS
SELECT
    filename,
    platform,
    phase,
    COUNT(*) as record_count,
    MAX(UPLOADED_DATE) as upload_date
FROM TEST_STAGING.PUBLIC.platform_viewership
GROUP BY filename, platform, phase
ORDER BY upload_date DESC;
```

```sql
-- View: Asset matching success rate
CREATE OR REPLACE VIEW MATCHING_SUCCESS_RATE AS
SELECT
    filename,
    COUNT(*) as total,
    SUM(CASE WHEN asset_series IS NOT NULL THEN 1 ELSE 0 END) as matched,
    ROUND(100.0 * matched / total, 2) as success_rate_pct
FROM TEST_STAGING.PUBLIC.platform_viewership
GROUP BY filename;
```

## Notes on Background Processes

The system may show reminders about background Bash processes running Snowflake tests. These are:
- Autonomous testing procedures running in parallel
- Not directly related to the current task unless explicitly invoked
- Can be monitored with BashOutput tool if needed
- Should not interrupt the main workflow

When background processes report results, review them if relevant to current debugging, otherwise acknowledge and continue with primary task.
