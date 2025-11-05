-- ==============================================================================
-- ALTER TABLE: Rename IMPRESSIONS to TOT_IMPRESSIONS
-- ==============================================================================
-- This script renames the IMPRESSIONS column to TOT_IMPRESSIONS for consistency
-- with other metric columns (TOT_SESSIONS, TOT_COMPLETIONS, etc.)
-- ==============================================================================

-- Rename IMPRESSIONS to TOT_IMPRESSIONS in platform_viewership table
ALTER TABLE {{UPLOAD_DB}}.public.platform_viewership
RENAME COLUMN IMPRESSIONS TO TOT_IMPRESSIONS;

-- Verify the column was renamed
SELECT 'platform_viewership table updated' AS status;

-- Optional: Rename in episode_details table if it exists
-- Note: episode_details is in ASSETS_DB
ALTER TABLE {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}
RENAME COLUMN IMPRESSIONS TO TOT_IMPRESSIONS;

SELECT 'episode_details table updated' AS status;

-- Verify columns exist
SELECT COLUMN_NAME, DATA_TYPE
FROM {{UPLOAD_DB}}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC'
  AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
  AND COLUMN_NAME = 'TOT_IMPRESSIONS'
ORDER BY COLUMN_NAME;

SELECT 'Column verification complete' AS status;
