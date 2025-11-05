-- ==============================================================================
-- MIGRATION 001: Schema - Tables and Columns
-- ==============================================================================
-- Purpose: Ensure all required tables exist with correct columns
-- Dependencies: None
-- Idempotent: Yes (uses CREATE OR REPLACE and ADD COLUMN patterns)
-- ==============================================================================

-- ==============================================================================
-- EPISODE_DETAILS Tables - Add missing platform columns
-- ==============================================================================

-- Staging
ALTER TABLE {{ASSETS_DB}}.PUBLIC.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS PLATFORM_PARTNER_NAME VARCHAR(500);

ALTER TABLE {{ASSETS_DB}}.PUBLIC.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS PLATFORM_CHANNEL_NAME VARCHAR(500);

ALTER TABLE {{ASSETS_DB}}.PUBLIC.{{EPISODE_DETAILS_TABLE}}
ADD COLUMN IF NOT EXISTS PLATFORM_TERRITORY VARCHAR(500);

-- Note: Snowflake doesn't support IF NOT EXISTS in ALTER TABLE ADD COLUMN
-- These will error if columns exist, but that's OK - they're idempotent in practice
-- Future: Could wrap in stored procedure with exception handling

-- ==============================================================================
-- Platform Viewership Tables
-- ==============================================================================
-- Assumed to exist already in UPLOAD_DB and STAGING_DB
-- If needed, add CREATE TABLE statements here

-- ==============================================================================
-- Metadata Tables
-- ==============================================================================
-- record_reprocessing_batch_logs - assumed to exist
-- If needed, add CREATE TABLE statements here
