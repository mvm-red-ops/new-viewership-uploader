-- Check which columns exist in platform_viewership
SELECT
    id,
    platform,
    tot_sessions,      -- Does this exist?
    unique_viewers,    -- Does this exist?
    views,             -- Does this exist?
    week,              -- Does this exist?
    day                -- Does this exist?
FROM test_staging.public.platform_viewership
WHERE platform = 'Amagi'
AND LOWER(filename) = 'amagi_q3_25_test - amagi_q3_25.csv.csv'
LIMIT 1;
