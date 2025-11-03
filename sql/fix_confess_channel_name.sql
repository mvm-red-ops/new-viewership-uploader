-- ============================================================
-- FIX CHANNEL NAME: Update "Confess Nosey" to "Confess by Nosey"
-- ============================================================
--
-- This script updates any records with the old channel name
-- "Confess Nosey" to the correct name "Confess by Nosey"
--
-- Run this in Snowflake to fix historical data
-- ============================================================

-- Check current records with old channel name
SELECT 'Records with old channel name in platform_viewership:' AS info;
SELECT COUNT(*) as record_count, FILENAME
FROM upload_db.public.platform_viewership
WHERE CHANNEL = 'Confess Nosey'
GROUP BY FILENAME;

SELECT 'Records with old channel name in viewership_file_formats:' AS info;
SELECT COUNT(*) as record_count, PLATFORM, PARTNER
FROM dictionary.public.viewership_file_formats
WHERE CHANNEL = 'Confess Nosey'
GROUP BY PLATFORM, PARTNER;

-- Update platform_viewership table
UPDATE upload_db.public.platform_viewership
SET CHANNEL = 'Confess by Nosey'
WHERE CHANNEL = 'Confess Nosey';

-- Update viewership_file_formats table
UPDATE dictionary.public.viewership_file_formats
SET CHANNEL = 'Confess by Nosey'
WHERE CHANNEL = 'Confess Nosey';

-- Verify the updates
SELECT 'Verification - Records with new channel name:' AS info;
SELECT COUNT(*) as record_count
FROM upload_db.public.platform_viewership
WHERE CHANNEL = 'Confess by Nosey';

SELECT COUNT(*) as record_count
FROM dictionary.public.viewership_file_formats
WHERE CHANNEL = 'Confess by Nosey';

SELECT 'Update complete!' AS status;
