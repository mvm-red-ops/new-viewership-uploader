-- ==============================================================================
-- Add missing columns to PRODUCTION platform_viewership table
-- ==============================================================================
-- This script adds columns that exist in staging but may be missing in production
-- Safe to run multiple times - uses ADD COLUMN IF NOT EXISTS where supported
-- ==============================================================================

-- 1. Add PROCESSED column (for Lambda tracking)
ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PROCESSED BOOLEAN DEFAULT NULL;

-- 2. Add PHASE column (for Lambda tracking)
ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PHASE VARCHAR(10);

-- 3. Add date columns (required by stored procedures)
ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS FULL_DATE VARCHAR(50);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS WEEK VARCHAR(50);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS DAY VARCHAR(50);

-- 4. Add platform identifier columns (recently added)
ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PLATFORM_PARTNER_NAME VARCHAR(255);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PLATFORM_CHANNEL_NAME VARCHAR(255);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PLATFORM_TERRITORY VARCHAR(255);

-- 5. Add internal normalized identifier columns
ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS PARTNER VARCHAR(255);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS CHANNEL VARCHAR(255);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS CHANNEL_ID INTEGER;

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS TERRITORY VARCHAR(255);

ALTER TABLE UPLOAD_DB_PROD.PUBLIC.platform_viewership
ADD COLUMN IF NOT EXISTS TERRITORY_ID INTEGER;

-- Verification query - check if all columns exist
SELECT 'Migration complete. Verifying columns...' AS status;

SELECT column_name, data_type
FROM UPLOAD_DB_PROD.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'PUBLIC'
  AND table_name = 'PLATFORM_VIEWERSHIP'
  AND column_name IN (
    'PROCESSED', 'PHASE', 'FULL_DATE', 'WEEK', 'DAY',
    'PLATFORM_PARTNER_NAME', 'PLATFORM_CHANNEL_NAME', 'PLATFORM_TERRITORY',
    'PARTNER', 'CHANNEL', 'CHANNEL_ID', 'TERRITORY', 'TERRITORY_ID'
  )
ORDER BY column_name;
