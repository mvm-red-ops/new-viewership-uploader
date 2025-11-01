-- ============================================================
-- VIEWERSHIP UPLOADER - TABLE SETUP
-- ============================================================
--
-- 👉 RUN THIS SCRIPT IN SNOWFLAKE (copy/paste into Snowflake worksheet)
--
-- WHAT THIS DOES:
-- 1. Creates platform_viewership table with columns for:
--    - Original values (PLATFORM_PARTNER_NAME, PLATFORM_CHANNEL_NAME, etc.)
--    - Transformed values (PARTNER, CHANNEL, TERRITORY, CONTENT)
--
-- 2. Fixes viewership_file_formats config table:
--    - Makes channel/territory nullable (for platform-wide configs)
--    - Adds domain column
--
-- After running this, restart your Streamlit app.
-- ============================================================

-- ============================================================
-- PART 1: Rebuild platform_viewership table
-- ============================================================

-- Drop existing table
DROP TABLE IF EXISTS upload_db.public.platform_viewership;

-- Create table with new schema including transformed columns
CREATE TABLE upload_db.public.platform_viewership (
    -- Required columns
    PLATFORM VARCHAR(255),
    DOMAIN VARCHAR(255),

    -- Original/raw values from platform files
    PLATFORM_PARTNER_NAME VARCHAR(255),
    PLATFORM_CHANNEL_NAME VARCHAR(255),
    PLATFORM_TERRITORY VARCHAR(255),
    PLATFORM_CONTENT_NAME VARCHAR(500),

    -- Transformed/normalized values (after applying transformations)
    PARTNER VARCHAR(255),
    CHANNEL VARCHAR(255),
    TERRITORY VARCHAR(255),
    CONTENT VARCHAR(500),

    DATE DATE,
    PLATFORM_CONTENT_ID VARCHAR(255),
    PLATFORM_SERIES VARCHAR(500),
    ASSET_TITLE VARCHAR(500),
    ASSET_SERIES VARCHAR(500),
    TOT_HOV FLOAT,
    TOT_MOV FLOAT,

    -- Optional Metrics columns
    AVG_DURATION_PER_SESSION FLOAT,
    AVG_DURATION_PER_VIEWER FLOAT,
    AVG_SESSION_COUNT FLOAT,
    CHANNEL_ADPOOL_IMPRESSIONS INTEGER,
    DURATION FLOAT,
    IMPRESSIONS INTEGER,
    SESSIONS INTEGER,
    TOT_SESSIONS INTEGER,
    UNIQUE_VIEWERS INTEGER,
    VIEWS INTEGER,

    -- Optional Geo columns
    CITY VARCHAR(255),
    COUNTRY VARCHAR(255),

    -- Optional Device columns
    DEVICE_ID VARCHAR(255),
    DEVICE_NAME VARCHAR(255),
    DEVICE_TYPE VARCHAR(255),

    -- Optional Content columns
    EPISODE_NUMBER VARCHAR(50),
    LANGUAGE VARCHAR(100),
    CONTENT_PROVIDER VARCHAR(255),
    REF_ID VARCHAR(255),
    SEASON_NUMBER VARCHAR(50),
    SERIES_CODE VARCHAR(255),
    VIEWERSHIP_TYPE VARCHAR(100),

    -- Optional Date columns
    END_TIME TIMESTAMP_NTZ,
    MONTH VARCHAR(50),
    QUARTER VARCHAR(50),
    START_TIME TIMESTAMP_NTZ,
    YEAR_MONTH_DAY VARCHAR(50),
    YEAR INTEGER,

    -- Optional Monetary columns
    CHANNEL_ADPOOL_REVENUE FLOAT,
    REVENUE FLOAT,

    -- Metadata columns
    FILENAME VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Processing tracking columns (used by Lambda post-processing)
    PROCESSED BOOLEAN DEFAULT FALSE,
    PHASE VARCHAR(10)
);

-- Verify platform_viewership table
SELECT 'Platform_viewership table created' AS status;
DESCRIBE TABLE upload_db.public.platform_viewership;


-- ============================================================
-- PART 2: Fix viewership_file_formats table
-- ============================================================

-- Make channel and territory nullable (they are optional metadata fields)
ALTER TABLE dictionary.public.viewership_file_formats
ALTER COLUMN channel DROP NOT NULL;

ALTER TABLE dictionary.public.viewership_file_formats
ALTER COLUMN territory DROP NOT NULL;

-- Add domain column if it doesn't exist
ALTER TABLE dictionary.public.viewership_file_formats
ADD COLUMN IF NOT EXISTS domain VARCHAR(255);

-- Verify viewership_file_formats table
SELECT 'Viewership_file_formats table updated' AS status;
DESCRIBE TABLE dictionary.public.viewership_file_formats;


-- ============================================================
-- FINAL VERIFICATION
-- ============================================================

-- Check platform_viewership key columns
SELECT 'Platform_viewership key columns:' AS info;
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM upload_db.information_schema.columns
WHERE table_schema = 'PUBLIC'
  AND table_name = 'PLATFORM_VIEWERSHIP'
  AND column_name IN (
    'PLATFORM', 'DOMAIN', 'PARTNER', 'CHANNEL', 'TERRITORY', 'CONTENT',
    'PLATFORM_PARTNER_NAME', 'PLATFORM_CHANNEL_NAME', 'PLATFORM_TERRITORY', 'PLATFORM_CONTENT_NAME'
  )
ORDER BY ORDINAL_POSITION;

-- Check viewership_file_formats key columns
SELECT 'Viewership_file_formats key columns:' AS info;
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM dictionary.information_schema.columns
WHERE table_schema = 'PUBLIC'
  AND table_name = 'VIEWERSHIP_FILE_FORMATS'
  AND column_name IN (
    'PLATFORM', 'PARTNER', 'CHANNEL', 'TERRITORY', 'DOMAIN'
  )
ORDER BY ORDINAL_POSITION;

SELECT 'All tables updated successfully!' AS status;
