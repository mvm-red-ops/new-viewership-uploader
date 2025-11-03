-- ==============================================================================
-- ALTER TABLE: Add TOT_COMPLETIONS column
-- ==============================================================================
-- This script adds the TOT_COMPLETIONS column to existing tables
-- START_TIME and END_TIME already exist, so only TOT_COMPLETIONS is added
-- ==============================================================================

-- Add TOT_COMPLETIONS to platform_viewership table
ALTER TABLE {{UPLOAD_DB}}.public.platform_viewership
ADD COLUMN IF NOT EXISTS TOT_COMPLETIONS INTEGER;

-- Verify the column was added
SELECT 'platform_viewership table updated' AS status;

-- Optional: Add to episode_details table if needed
-- Note: episode_details is in ASSETS_DB, check if column already exists first
ALTER TABLE {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS tot_completions INTEGER;

-- Optional: Add start_time and end_time if they don't exist in episode_details
ALTER TABLE {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS start_time TIMESTAMP_NTZ;

ALTER TABLE {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS end_time TIMESTAMP_NTZ;

SELECT 'episode_details table updated' AS status;

-- Verify columns exist
SELECT COLUMN_NAME, DATA_TYPE
FROM {{UPLOAD_DB}}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC'
  AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
  AND COLUMN_NAME IN ('TOT_COMPLETIONS', 'START_TIME', 'END_TIME')
ORDER BY COLUMN_NAME;

SELECT 'Column verification complete' AS status;
