-- Create platform_viewership table in staging database
-- Run this if test_staging.public.platform_viewership doesn't exist yet

CREATE TABLE IF NOT EXISTS test_staging.public.platform_viewership (
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
    CHANNEL_ID INTEGER,
    TERRITORY VARCHAR(255),
    TERRITORY_ID INTEGER,

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
    DEAL_PARENT INTEGER,
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
    PROCESSED BOOLEAN,  -- NULL = not processed, TRUE = processed
    PHASE VARCHAR(10)
);

-- Verify table created
DESCRIBE TABLE test_staging.public.platform_viewership;
