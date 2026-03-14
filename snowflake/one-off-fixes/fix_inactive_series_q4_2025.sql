-- =============================================================================
-- FIX: Inactive Series Q4 2025 — Divorce Court & Operation Repo
-- Affected platforms: Amagi, Wurl
-- =============================================================================
-- Problem:
--   'Divorce Court' and 'Operation Repo' were marked inactive in
--   METADATA_MASTER.public.series. All bucket procedures filter
--   lower(s.status) = 'active', so these records never got:
--     asset_series, content_provider, series_code, asset_title set.
--   They were not inserted into ASSETS.public.EPISODE_DETAILS.
--
-- Diagnostic findings (run 2026-02-16):
--   Amagi: 6,778 unmatched total — 3,285 DC + 3,381 OR = 6,666 confirmed inactive-series
--          112 unmatched from other causes (separate issue, not addressed here)
--   Wurl:  14,566 unmatched total — 11,263 DC + 3,303 OR = exactly accounts for all
--   EPISODE_DETAILS confirmed: matched counts == EPISODE_DETAILS counts for both platforms
--   → DC+OR records are NOT in EPISODE_DETAILS yet. Safe to INSERT directly.
--
-- WHY NOT wipe+reinsert:
--   FreeVee has 11,835 in EPISODE_DETAILS but only 2,221 in NOSEY_PROD Q4 2025.
--   Zype has 365,708 in NOSEY_PROD (all processed=NULL) vs 94,608 in EPISODE_DETAILS.
--   Wiping EPISODE_DETAILS would permanently destroy data that doesn't exist in NOSEY_PROD.
--
-- Approach (TARGETED FIX):
--   1. UPDATE NOSEY_PROD — backfill metadata for DC+OR records only (bypass active filter)
--   2. VERIFY — confirm all target records are now matched
--   3. INSERT — targeted insert of only DC+OR records into EPISODE_DETAILS
--   4. VERIFY — confirm they appear in EPISODE_DETAILS
--
-- SEPARATE ISSUES (out of scope, investigate independently):
--   Amagi:  112 records still unmatched after this fix (not DC/OR)
--   Philo:  21,674 unmatched (no ref_id, not DC/OR)
--   Roku:   1,710 unmatched (no ref_id) + 4,493 processed=NULL (never pipeline'd)
--   Youtube: 2,397 unmatched
--   Zype:   365,708 processed=NULL — data was uploaded but never ran through pipeline
-- =============================================================================


-- =============================================================================
-- STEP 1: UPDATE NOSEY_PROD
--         Backfill asset metadata for DC+OR records via ref_id path.
--         Bypasses lower(s.status) = 'active' filter.
--         Uses ROW_NUMBER() to deduplicate multiple metadata rows per record.
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
                -- Prefer rows where episode title matches platform_content_name
                CASE
                    WHEN LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9]', '')) =
                         LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                    THEN 0 ELSE 1
                END,
                m.ref_id
        ) AS rn
    FROM NOSEY_PROD.public.platform_viewership v
    JOIN METADATA_MASTER.public.episode e    ON (v.ref_id = e.ref_id)
    JOIN METADATA_MASTER.public.series s     ON (s.id = e.series_id)
    JOIN METADATA_MASTER.public.metadata m   ON (e.ref_id = m.ref_id)
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


-- =============================================================================
-- STEP 2: VERIFY UPDATE
--         Confirm all DC+OR records now have content_provider set.
--         DO NOT proceed to step 3 if still_unmatched > 0.
-- =============================================================================

SELECT
    platform,
    LOWER(asset_series) AS series,
    COUNT(*)                                                          AS total_records,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END)        AS still_unmatched,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END)    AS now_matched,
    MIN(content_provider)                                             AS content_provider_sample,
    MIN(asset_series)                                                 AS asset_series_sample
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

-- Expected:
--   Amagi / divorce court    → 3285 now_matched, 0 still_unmatched
--   Amagi / operation repo   → 3381 now_matched, 0 still_unmatched
--   Wurl  / divorce court    → 11263 now_matched, 0 still_unmatched
--   Wurl  / operation repo   → 3303 now_matched, 0 still_unmatched

-- If still_unmatched > 0, check what internal_series values these records have:
-- SELECT DISTINCT internal_series, ref_id IS NOT NULL AS has_ref_id, COUNT(*)
-- FROM NOSEY_PROD.public.platform_viewership
-- WHERE year = 2025 AND LOWER(quarter) = 'q4'
--   AND platform IN ('Amagi', 'Wurl')
--   AND content_provider IS NULL
-- GROUP BY 1, 2;


-- =============================================================================
-- STEP 3: INSERT — targeted insert of DC+OR records into EPISODE_DETAILS
--
-- Column list sourced exactly from move_data_to_final_table_dynamic_generic.sql
-- (archived: _archive/2025-12-05-pre-staging-production-split/
--  stored_procedures/-sub-procedures/migrate/move_to_final_table_dynamic_generic.sql)
--
-- Intentional deviations from the original per-file procedure:
--   ✗ No `AND processed IS NULL` — these records are processed=TRUE (Lambda already ran)
--     but they were never inserted because asset_series was NULL at insert time.
--     EPISODE_DETAILS counts confirmed they are absent. Safe to insert.
--   ✗ No `AND platform = '${PLATFORM}'` / `AND filename = ...` — we're targeting by
--     asset_series and platform instead, scoped to Q4 2025.
--   ✓ All other quality gates preserved: deal_parent, ref_id, asset_series,
--     tot_mov, tot_hov must all be NOT NULL.
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
    SUM(tot_sessions)      AS sessions,
    SUM(tot_mov)           AS minutes,
    SUM(tot_hov)           AS hours,
    year,
    quarter,
    platform,
    partner                AS viewership_partner,
    domain,
    'Viewership'           AS label,
    filename,
    CAST(phase AS VARCHAR) AS phase,
    week,
    day,
    SUM(unique_viewers)    AS unique_viewers,
    platform_content_id,
    SUM(views)             AS views
FROM NOSEY_PROD.public.platform_viewership
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND platform IN ('Amagi', 'Wurl')
  AND LOWER(asset_series) IN ('divorce court', 'operation repo')
  AND deal_parent     IS NOT NULL
  AND ref_id          IS NOT NULL
  AND asset_series    IS NOT NULL
  AND tot_mov         IS NOT NULL
  AND tot_hov         IS NOT NULL
GROUP BY ALL;


-- =============================================================================
-- STEP 4: FINAL VERIFICATION
-- =============================================================================

-- 4a. Confirm DC+OR records are now in EPISODE_DETAILS
SELECT
    platform,
    asset_series,
    COUNT(*) AS records_inserted
FROM ASSETS.public.EPISODE_DETAILS
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND label = 'Viewership'
  AND platform IN ('Amagi', 'Wurl')
  AND LOWER(asset_series) IN ('divorce court', 'operation repo')
GROUP BY 1, 2
ORDER BY 1, 2;

-- Expected approximately:
--   Amagi / Divorce Court    → ~3285 records
--   Amagi / Operation Repo   → ~3381 records
--   Wurl  / Divorce Court    → ~11263 records
--   Wurl  / Operation Repo   → ~3303 records
-- (Actual may differ slightly from GROUP BY ALL aggregation)

-- 4b. Confirm total Amagi + Wurl counts in EPISODE_DETAILS increased correctly
SELECT
    platform,
    COUNT(*) AS total_in_episode_details
FROM ASSETS.public.EPISODE_DETAILS
WHERE year = 2025
  AND LOWER(quarter) = 'q4'
  AND label = 'Viewership'
  AND platform IN ('Amagi', 'Wurl')
GROUP BY platform;

-- Expected (before + newly inserted):
--   Amagi: 146329 + ~6666 = ~152995
--   Wurl:  213618 + ~14566 = ~228184
