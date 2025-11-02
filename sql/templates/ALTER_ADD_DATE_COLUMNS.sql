-- ==============================================================================
-- ALTER TABLE: Add date columns to existing platform_viewership tables
-- ==============================================================================
-- Run this on your existing tables to add the missing date columns
-- These columns are required by:
-- - set_date_columns_dynamic procedure (FULL_DATE)
-- - move_data_to_final_table_dynamic_generic procedure (WEEK, DAY)
-- ==============================================================================

-- IMPORTANT: Must add to BOTH upload_db AND test_staging databases
-- Lambda copies data between these tables and both need matching schemas

-- 1. Add to upload_db (where Streamlit loads data)
ALTER TABLE {{UPLOAD_DB}}.public.platform_viewership
    ADD COLUMN FULL_DATE VARCHAR(50),
    ADD COLUMN WEEK VARCHAR(50),
    ADD COLUMN DAY VARCHAR(50);

-- 2. Add to test_staging (where Lambda processes data)
ALTER TABLE {{STAGING_DB}}.public.platform_viewership
    ADD COLUMN FULL_DATE VARCHAR(50),
    ADD COLUMN WEEK VARCHAR(50),
    ADD COLUMN DAY VARCHAR(50);

-- For production database (uncomment when ready to deploy to prod)
-- ALTER TABLE upload_db_prod.public.platform_viewership
--     ADD COLUMN FULL_DATE VARCHAR(50),
--     ADD COLUMN WEEK VARCHAR(50),
--     ADD COLUMN DAY VARCHAR(50);
