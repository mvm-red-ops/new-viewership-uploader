-- ==============================================================================
-- MIGRATION 006: Permissions and Grants
-- ==============================================================================
-- Purpose: Grant all necessary permissions to roles
-- Dependencies: 001-005 (all objects must exist)
-- Idempotent: Yes (GRANT is idempotent)
-- ==============================================================================

-- ==============================================================================
-- UDF Permissions
-- ==============================================================================

GRANT USAGE ON FUNCTION {{UPLOAD_DB}}.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR) TO ROLE WEB_APP;

-- ==============================================================================
-- Table Permissions - Metadata
-- ==============================================================================

GRANT INSERT, SELECT ON TABLE {{METADATA_DB}}.PUBLIC.record_reprocessing_batch_logs TO ROLE WEB_APP;

-- ==============================================================================
-- Table Permissions - Episode Details
-- ==============================================================================

GRANT INSERT, SELECT, UPDATE ON TABLE {{ASSETS_DB}}.PUBLIC.{{EPISODE_DETAILS_TABLE}} TO ROLE WEB_APP;

-- ==============================================================================
-- Table Permissions - Platform Viewership
-- ==============================================================================

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE {{UPLOAD_DB}}.PUBLIC.platform_viewership TO ROLE WEB_APP;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE {{STAGING_DB}}.PUBLIC.platform_viewership TO ROLE WEB_APP;

-- ==============================================================================
-- Sequence Permissions
-- ==============================================================================

-- Sequence used for auto-incrementing ID in record_reprocessing_batch_logs
GRANT USAGE ON SEQUENCE {{UPLOAD_DB}}.PUBLIC.RECORD_REPROCESSING_IDS TO ROLE WEB_APP;

-- ==============================================================================
-- Stored Procedure Permissions
-- ==============================================================================

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_phase_generic(VARCHAR, FLOAT, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.calculate_viewership_metrics(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_date_columns_dynamic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.handle_viewership_conflicts(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_deal_parent_generic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_channel_generic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_territory_generic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.send_unmatched_deals_alert(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.set_internal_series_generic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.move_data_to_final_table_dynamic_generic(VARCHAR, VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.handle_final_insert_dynamic_generic(VARCHAR, VARCHAR, VARCHAR) TO ROLE WEB_APP;
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.validate_viewership_for_insert(VARCHAR, VARCHAR) TO ROLE WEB_APP;

-- ==============================================================================
-- Add more permissions as needed
-- ==============================================================================
