# REF_ID_SERIES Bucket Delegate

## Specialty

Content matching for records with ref_id + internal_series (no numeric episode/season)

## Procedure Details

**File:** `snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql`

**Deployment Script:** `deploy_ref_id_series_proc.py`

**Database:** UPLOAD_DB.PUBLIC

**Procedure Name:** `PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC(platform VARCHAR, filename VARCHAR)`

## Purpose

Matches viewership records that have:
- `ref_id` populated (from platform_content_id matching)
- `internal_series` populated (from deal_parent matching)
- `episode_number` is NOT numeric (e.g., string "None")
- `season_number` is NOT numeric (e.g., string "None")

Sets: `asset_series`, `content_provider`, `series_code`, `asset_title`

## Critical Code Sections

### PROTECTED: Lines 53-130 - UNION Fallback Pattern

**SAFETY COUNCIL PROTECTION - Must include UNION**

```sql
UPDATE test_staging.public.platform_viewership w
SET w.series_code = q.series_code,
    w.content_provider = q.content_provider,
    w.asset_title = q.title,
    w.asset_series = q.asset_series
FROM (
    -- First attempt: Match with title check (strict)
    SELECT
        v.id AS id,
        m.title AS title,
        s.content_provider AS content_provider,
        s.series_code AS series_code,
        upload_db.public.extract_primary_title(s.titles) AS asset_series
    FROM
        test_staging.public.platform_viewership v
    JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg}_${bucketName}_BUCKET b ON (v.id = b.id)
    JOIN metadata_master_cleaned_staging.public.episode e ON (v.ref_id = e.ref_id)
    JOIN metadata_master_cleaned_staging.public.series s ON (s.id = e.series_id)
    JOIN metadata_master_cleaned_staging.public.metadata m ON (e.ref_id = m.ref_id)
    JOIN metadata_master_cleaned_staging.public.metadata m_title_check ON (
        e.ref_id = m_title_check.ref_id
        AND (
            LOWER(REGEXP_REPLACE(TRIM(m_title_check.title), '[^A-Za-z0-9]', '')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
            OR
            LOWER(REGEXP_REPLACE(TRIM(m_title_check.clean_title), '[^A-Za-z0-9]', '')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
        )
    )
    WHERE v.platform = '${platformArg}'
        AND v.processed IS NULL
        AND v.content_provider IS NULL
        AND v.ref_id IS NOT NULL
        AND v.internal_series IS NOT NULL
        AND lower(s.status) = 'active'
        AND LOWER(upload_db.public.extract_primary_title(s.titles)) = LOWER(v.internal_series)

    UNION  // ← CRITICAL - Must include UNION fallback

    -- Fallback: Match on ref_id and series only, no title check (looser)
    SELECT
        v.id AS id,
        m.title AS title,
        s.content_provider AS content_provider,
        s.series_code AS series_code,
        upload_db.public.extract_primary_title(s.titles) AS asset_series
    FROM
        test_staging.public.platform_viewership v
    JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg}_${bucketName}_BUCKET b ON (v.id = b.id)
    JOIN metadata_master_cleaned_staging.public.episode e ON (v.ref_id = e.ref_id)
    JOIN metadata_master_cleaned_staging.public.series s ON (s.id = e.series_id)
    JOIN metadata_master_cleaned_staging.public.metadata m ON (e.ref_id = m.ref_id)
    WHERE v.platform = '${platformArg}'
        AND v.processed IS NULL
        AND v.content_provider IS NULL
        AND v.ref_id IS NOT NULL
        AND v.internal_series IS NOT NULL
        AND lower(s.status) = 'active'
        AND LOWER(upload_db.public.extract_primary_title(s.titles)) = LOWER(v.internal_series)
        -- Exclude records already matched in first attempt
        AND NOT EXISTS (
            SELECT 1
            FROM metadata_master_cleaned_staging.public.metadata m_check
            WHERE e.ref_id = m_check.ref_id
            AND (
                LOWER(REGEXP_REPLACE(TRIM(m_check.title), '[^A-Za-z0-9]', '')) =
                LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                OR
                LOWER(REGEXP_REPLACE(TRIM(m_check.clean_title), '[^A-Za-z0-9]', '')) =
                LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
            )
        )
) q
WHERE w.id = q.id
```

**Why UNION is Critical:**
- **First SELECT**: Strict matching - requires ref_id + series + title match
- **UNION + Second SELECT**: Fallback - matches on ref_id + series WITHOUT title check
- **NOT EXISTS**: Prevents duplicates by excluding records already matched

**Historical Issue (December 2, 2025):**
- Deployed procedure was MISSING lines 92-127 (the entire UNION fallback)
- Result: Tubi data with 321 records routed to REF_ID_SERIES bucket, but 0 rows updated
- Platform content names like "S03:E140 - You Waited 17 Years for This!" don't match metadata titles exactly
- Strict matching failed, fallback wasn't present, so no matches occurred

**Why Titles Don't Match:**
- Tubi content names: "S03:E140 - You Waited 17 Years for This!"
- Metadata titles: "You Waited 17 Years for This!"
- Different format, so strict title check fails
- Fallback matches on ref_id + series (which ARE correct), ignoring title mismatch

## Dependencies

### Upstream (What Affects This)
- **ANALYZE_AND_PROCESS Delegate** - Creates TEMP bucket and routes records here
- **SET_REF_ID Delegate** - Must populate ref_id first
- **SET_DEAL_PARENT Delegate** - Must populate internal_series first

### Downstream (What This Affects)
- **HANDLE_FINAL_INSERT Delegate** - Uses asset_series, content_provider, series_code for final insert
- Reporting that depends on these fields

## Common Issues

### Issue: Bucket has records but 0 rows updated

**Symptoms:**
- Error log shows: "Created REF_ID_SERIES bucket with 321 records"
- Error log shows: "REF_ID_SERIES bucket: Updated 0 rows"
- asset_series, content_provider, series_code remain NULL

**Root Cause:** UNION fallback missing from deployed procedure

**Solution:** Deploy complete procedure with UNION (FIXED December 2, 2025)

**Verification:**
```sql
-- Check if UNION exists in deployed procedure
SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC(VARCHAR, VARCHAR)');
-- Search output for "UNION" keyword

-- Check if fields are set
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN asset_series IS NOT NULL THEN 1 ELSE 0 END) as has_asset_series,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END) as has_content_provider,
    SUM(CASE WHEN series_code IS NOT NULL THEN 1 ELSE 0 END) as has_series_code
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';
```

### Issue: Records not routed to this bucket

**Symptoms:**
- Records have ref_id and internal_series
- But REF_ID_SERIES bucket shows 0 records

**Possible Causes:**
1. episode_number or season_number ARE numeric → Goes to FULL_DATA bucket instead
2. internal_series is NULL → Can't route to REF_ID_SERIES
3. ref_id is NULL → Can't route to REF_ID_SERIES

**Solution:** Check bucket routing logic in ANALYZE_AND_PROCESS procedure

## Testing Checklist

Before deploying changes to this procedure:

- [ ] Verify UNION fallback is present (lines 92-127)
- [ ] Test with data where titles match exactly (strict matching works)
- [ ] Test with data where titles DON'T match (fallback needed)
- [ ] Verify NOT EXISTS prevents duplicates
- [ ] Check that only active series are matched (status='active')
- [ ] Verify error logs show correct row counts
- [ ] Test with multiple platforms
- [ ] Verify DDL after deployment contains UNION

## Escalation Triggers

**Escalate to Snowflake Governor if:**
- UNION fallback is being removed or modified
- Matching criteria changes (ref_id, series, title logic)
- New joins needed to metadata tables

**Escalate to Safety Council if:**
- UNION fallback is being removed (IMMEDIATE VETO)

**Escalate to Testing Governor if:**
- Test coverage needed for new edge cases
- Deployment verification shows UNION missing

**Escalate to President if:**
- Data quality issue (ref_id or internal_series being set incorrectly upstream)
- Cross-bucket issue (records going to wrong bucket)

## Questions for Constitutional Convention

- [ ] When can UNION fallback be modified (if ever)?
- [ ] Should all bucket procedures have UNION fallbacks?
- [ ] What is the precedence when both strict and fallback match the same record?
- [ ] How do we handle cases where fallback matches multiple series?
- [ ] Should we add additional fallback layers?
- [ ] What testing is required before deploying bucket procedure changes?

## Knowledge Base

**Last Updated:** December 2, 2025

**Recent Changes:**
- ✅ Redeployed complete procedure with UNION fallback (was missing)
- ✅ Verified UNION exists in deployed DDL

**Test Results (Tubi VOD July):**
- Before fix: 321 records in bucket, 0 rows updated
- After fix: 321 records in bucket, 593 rows updated (includes matches from other batches), all 321 records have asset_series set

**Sample Matches:**
```
S03:E140 - You Waited 17 Years for This! | Series: The Steve Wilkos Show | Provider: NBC | Code: SW
S11:E33 - Torn Apart. Where Are They Now  | Series: Maury                | Provider: NBC | Code: MS
S12:E31 - I Want Your Lover!               | Series: Jerry Springer       | Provider: NBC | Code: JS
S01:E03 - I'll Unlock My Phone to Prove   | Series: Karamo               | Provider: NBC | Code: KA
```

**Deployment Status:**
- ✅ Deployed to STAGING: December 2, 2025
- ✅ Deployed to PROD: December 2, 2025
- ✅ UNION verified present in deployed DDL
