-- =============================================================================
-- RERUN: Analyze & Process for unmatched Q4 2025 records
-- Platforms: Amagi, Wurl, Philo, Roku
-- =============================================================================
-- Problem:
--   Unmatched records across 4 platforms could not be inserted into EPISODE_DETAILS
--   because content_provider was never set. Root causes:
--     - Amagi/Wurl: 'Divorce Court' + 'Operation Repo' marked inactive in METADATA_MASTER
--     - Philo:      21,674 records with no ref_id (no platform_content_id mapping)
--     - Roku:        1,710 records with no ref_id + 4,493 never pipeline'd (processed=NULL)
--
-- Approach:
--   1. SET processed = NULL on all unmatched records (makes them eligible for pipeline)
--   2. MANUALLY backfill DC+OR metadata (bypasses inactive filter — analyze_and_process
--      won't fix these because lower(s.status) = 'active' is still in bucket procs)
--   3. CALL analyze_and_process for each platform (fixes all other unmatched)
--   4. INSERT newly matched records into EPISODE_DETAILS
--   5. MARK processed = TRUE
--
-- NOTE on procedure database:
--   UPLOAD_DB_PROD.public.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC is called below.
--   The source file (snowflake/stored_procedures/production/generic/ANALYZE_AND_PROCESS...)
--   still references TEST_STAGING in the viewershipTable variable — this appears to be a
--   stale file. Verify the DEPLOYED procedure targets NOSEY_PROD before running:
--     SELECT GET_DDL('PROCEDURE',
--       'UPLOAD_DB_PROD.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(VARCHAR, VARCHAR)');
--   If it shows TEST_STAGING, DO NOT run step 3 and flag to ops.
-- =============================================================================


-- =============================================================================
-- STEP 1: RESET processed = NULL for all unmatched records (4 platforms, Q4 2025)
--         Only resets records where content_provider IS NULL (unmatched).
--         Matched records (content_provider IS NOT NULL) are untouched — they keep
--         processed = TRUE and won't be re-processed by analyze_and_process.
-- =============================================================================

UPDATE NOSEY_PROD.public.platform_viewership
SET processed = NULL
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
  AND content_provider IS NULL;

-- Verify reset
SELECT
    platform,
    SUM(CASE WHEN processed IS NULL     THEN 1 ELSE 0 END) AS processed_null,
    SUM(CASE WHEN processed = TRUE      THEN 1 ELSE 0 END) AS processed_true,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END) AS still_unmatched
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
GROUP BY platform
ORDER BY platform;
-- All unmatched records should now show processed_null = still_unmatched.


-- =============================================================================
-- STEP 2: MANUALLY backfill DC+OR metadata in NOSEY_PROD
--         analyze_and_process won't fix these (series still inactive in METADATA_MASTER).
--         Now that processed = NULL, the INSERT in step 4 will naturally pick them up.
--
--         Uses ROW_NUMBER() to deduplicate multiple metadata rows per record.
--         Prefers rows where episode title matches platform_content_name.
-- =============================================================================

UPDATE NOSEY_PROD.public.platform_viewership w
SET
    w.series_code      = matched.series_code,
    w.content_provider = matched.content_provider,
    w.asset_title      = matched.title,
    w.asset_series     = matched.asset_series
FROM (
    SELECT
        v.id,
        m.title,
        s.content_provider,
        s.series_code,
        UPLOAD_DB_PROD.public.extract_primary_title(s.titles) AS asset_series,
        ROW_NUMBER() OVER (
            PARTITION BY v.id
            ORDER BY
                CASE
                    WHEN LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9]', '')) =
                         LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                    THEN 0 ELSE 1
                END,
                m.ref_id
        ) AS rn
    FROM NOSEY_PROD.public.platform_viewership v
    JOIN METADATA_MASTER.public.episode e  ON (v.ref_id = e.ref_id)
    JOIN METADATA_MASTER.public.series s   ON (s.id = e.series_id)
    JOIN METADATA_MASTER.public.metadata m ON (e.ref_id = m.ref_id)
    WHERE v.year = 2025
      AND LOWER(v.quarter) = 'q4'
      AND v.platform IN ('Amagi', 'Wurl')
      AND v.content_provider IS NULL
      AND v.ref_id IS NOT NULL
      AND TRIM(v.ref_id) != ''
      AND LOWER(UPLOAD_DB_PROD.public.extract_primary_title(s.titles)) IN ('divorce court', 'operation repo')
) matched
WHERE w.id = matched.id
  AND matched.rn = 1;

-- Verify DC+OR backfill
SELECT
    platform,
    LOWER(asset_series)                                               AS series,
    COUNT(*)                                                          AS total,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END)        AS still_unmatched,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END)    AS backfilled
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl')
  AND (
      LOWER(internal_series) IN ('divorce court', 'operation repo')
      OR LOWER(asset_series)  IN ('divorce court', 'operation repo')
  )
GROUP BY 1, 2
ORDER BY 1, 2;
-- Expected: still_unmatched = 0 for all four rows before proceeding.
-- If still_unmatched > 0, check:
--   SELECT DISTINCT internal_series, ref_id IS NOT NULL AS has_ref_id, COUNT(*)
--   FROM NOSEY_PROD.public.platform_viewership
--   WHERE year = 2025 AND LOWER(quarter) = 'q4'
--     AND platform IN ('Amagi','Wurl') AND content_provider IS NULL
--   GROUP BY 1, 2;


-- =============================================================================
-- STEP 3: RUN analyze_and_process for each platform
--         Handles all other unmatched records (non-DC/OR Amagi, Philo, Roku, Wurl).
--         DC+OR records are now excluded (content_provider IS NOT NULL after step 2).
--
--         Called WITHOUT filename so it processes ALL unmatched records for the platform.
--         Procedure filters: processed IS NULL AND content_provider IS NULL.
--
--         *** VERIFY DEPLOYED DDL TARGETS NOSEY_PROD BEFORE RUNNING ***
--         (See note at top of file)
-- =============================================================================

CALL UPLOAD_DB_PROD.public.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('Amagi', NULL);
CALL UPLOAD_DB_PROD.public.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('Wurl', NULL);
CALL UPLOAD_DB_PROD.public.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('Philo', NULL);
CALL UPLOAD_DB_PROD.public.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('Roku', NULL);

-- Check how many records got matched by the proc (content_provider now set, still processed=NULL)
SELECT
    platform,
    COUNT(*)                                                          AS total_records,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END)    AS matched,
    SUM(CASE WHEN content_provider IS NULL     THEN 1 ELSE 0 END)    AS still_unmatched,
    SUM(CASE WHEN processed IS NULL            THEN 1 ELSE 0 END)    AS ready_for_insert
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
GROUP BY platform
ORDER BY platform;


-- =============================================================================
-- STEP 4: INSERT newly matched records into EPISODE_DETAILS
--         Targets: processed IS NULL AND content_provider IS NOT NULL
--         (processed=NULL = records we reset in step 1; content_provider set = matched)
--         Unmatched records (content_provider IS NULL) are excluded by quality gates.
--
--         Column list sourced exactly from move_data_to_final_table_dynamic_generic.sql
--         (archived: _archive/2025-12-05-pre-staging-production-split/
--          stored_procedures/-sub-procedures/migrate/move_to_final_table_dynamic_generic.sql)
-- =============================================================================

INSERT INTO ASSETS.public.EPISODE_DETAILS (
    viewership_id,
    ref_id,
    deal_parent,
    platform_content_name,
    platform_series,
    asset_title,
    asset_series,
    content_provider,
    month,
    year_month_day,
    channel,
    channel_id,
    territory,
    territory_id,
    sessions,
    minutes,
    hours,
    year,
    quarter,
    platform,
    viewership_partner,
    domain,
    label,
    filename,
    phase,
    week,
    day,
    unique_viewers,
    platform_content_id,
    views
)
SELECT
    id,
    ref_id,
    deal_parent,
    platform_content_name,
    platform_series,
    asset_title,
    asset_series,
    content_provider,
    month,
    year_month_day,
    channel,
    channel_id,
    territory,
    territory_id,
    SUM(tot_sessions)       AS sessions,
    SUM(tot_mov)            AS minutes,
    SUM(tot_hov)            AS hours,
    year,
    quarter,
    platform,
    partner                 AS viewership_partner,
    domain,
    'Viewership'            AS label,
    filename,
    CAST(phase AS VARCHAR)  AS phase,
    week,
    day,
    SUM(unique_viewers)     AS unique_viewers,
    platform_content_id,
    SUM(views)              AS views
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
  AND processed       IS NULL       -- only records we reset in step 1
  AND deal_parent     IS NOT NULL
  AND ref_id          IS NOT NULL
  AND asset_series    IS NOT NULL
  AND tot_mov         IS NOT NULL
  AND tot_hov         IS NOT NULL
GROUP BY ALL;


-- =============================================================================
-- STEP 5: MARK processed = TRUE for records that were inserted
--         Only marks records where content_provider is set (matched and inserted).
--         Unmatched records (content_provider IS NULL) stay processed=NULL — they
--         are genuinely unresolved and should be investigated separately.
-- =============================================================================

UPDATE NOSEY_PROD.public.platform_viewership
SET processed = TRUE
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
  AND processed IS NULL
  AND content_provider IS NOT NULL;

-- Verify final state
SELECT
    platform,
    COUNT(*)                                                        AS total_records,
    SUM(CASE WHEN processed = TRUE    THEN 1 ELSE 0 END)           AS processed,
    SUM(CASE WHEN processed IS NULL   THEN 1 ELSE 0 END)           AS still_unprocessed,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END)      AS genuinely_unmatched
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
GROUP BY platform
ORDER BY platform;


-- =============================================================================
-- STEP 6: VERIFY EPISODE_DETAILS
-- =============================================================================

-- 6a. New counts by platform (compare to pre-fix counts from diagnostic 0d)
--     Pre-fix: Amagi=146329, Philo=69033, Roku=20390, Wurl=213618
SELECT
    platform,
    COUNT(*) AS records_in_episode_details
FROM ASSETS.public.EPISODE_DETAILS
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND label = 'Viewership'
  AND platform IN ('Amagi', 'Wurl', 'Philo', 'Roku')
GROUP BY platform
ORDER BY platform;

-- 6b. Confirm DC+OR are now in EPISODE_DETAILS
SELECT
    platform,
    asset_series,
    COUNT(*) AS records
FROM ASSETS.public.EPISODE_DETAILS
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND label = 'Viewership'
  AND platform IN ('Amagi', 'Wurl')
  AND LOWER(asset_series) IN ('divorce court', 'operation repo')
GROUP BY 1, 2
ORDER BY 1, 2;
