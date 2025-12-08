# Canonical Snowflake Knowledge

This document indexes the **source of truth** for all production-ready Snowflake code.

## Stored Procedures

### Staging Environment (`UPLOAD_DB`)
**Location**: `snowflake/stored_procedures/staging/generic/`

**Phase 0 → 1: Normalization**
- `normalize_data_in_staging.sql` - Full normalization wrapper for legacy S3 uploads
- `normalize_data_in_staging_generic.sql` - **NEW** Wrapper for Streamlit uploads (includes SET_REF_ID step)
- `set_deal_parent_generic.sql` - Sets deal/partner/channel/territory from DICTIONARY
- `set_ref_id_from_platform_content_id.sql` - **CRITICAL** Maps platform IDs to internal ref_ids
- `calculate_viewership_metrics.sql` - Calculates TOT_MOV from TOT_HOV or vice versa
- `set_date_columns_dynamic.sql` - Sets week/day/quarter/year/month from date column
- `set_phase_generic.sql` - Updates phase field for records

**Phase 1 → 2: Asset Matching**
- `analyze_and_process_viewership_data_generic.sql` - Main asset matching coordinator
- `set_internal_series_generic.sql` - Matches series names to internal catalog

**Phase 2 → Final: Data Movement**
- `handle_final_insert_dynamic_generic.sql` - Moves matched data to EPISODE_DETAILS tables
- `move_viewership_to_staging.sql` - Phase 0: Moves from UPLOAD_DB to viewership DB
- `move_streamlit_data_to_staging.sql` - **NEW** For Streamlit uploads (already normalized)

### Production Environment (`UPLOAD_DB_PROD`)
**Location**: `snowflake/stored_procedures/production/generic/`

Same procedures as staging, but targeting production databases:
- `UPLOAD_DB_PROD` instead of `UPLOAD_DB`
- `NOSEY_PROD` instead of `TEST_STAGING`
- `ASSETS` instead of `STAGING_ASSETS`

### Deployment Scripts
- `snowflake/stored_procedures/deploy_staging.py` - Deploys to UPLOAD_DB
- `snowflake/stored_procedures/deploy_production.py` - Deploys to UPLOAD_DB_PROD (with safety prompt)

## Key Architecture Patterns

### Multi-Phase Processing
1. **Phase 0**: Raw data in upload database
2. **Phase 1**: Normalized data (deal/partner/ref_id/metrics set)
3. **Phase 2**: Asset-matched data (series/title/content_provider set)
4. **Final**: Data in EPISODE_DETAILS tables, marked as `processed=TRUE`

### Streamlit vs. S3 Upload Paths

**S3 Upload (Legacy)**:
1. Upload → UPLOAD_DB.platform_viewership
2. MOVE_VIEWERSHIP_TO_STAGING → TEST_STAGING.platform_viewership (phase=0)
3. NORMALIZE_DATA_IN_STAGING → (phase=1)
4. ANALYZE_AND_PROCESS → (phase=2)
5. HANDLE_FINAL_INSERT → EPISODE_DETAILS

**Streamlit Upload (New)**:
1. Upload → UPLOAD_DB.platform_viewership (already normalized!)
2. MOVE_STREAMLIT_DATA_TO_STAGING → TEST_STAGING.platform_viewership (phase=0)
3. NORMALIZE_DATA_IN_STAGING_GENERIC → (phase=1) **includes SET_REF_ID step!**
4. ANALYZE_AND_PROCESS → (phase=2)
5. HANDLE_FINAL_INSERT → EPISODE_DETAILS

### Critical Fix (Dec 2025)
The Lambda Streamlit path was skipping `SET_REF_ID_FROM_PLATFORM_CONTENT_ID`, causing ref_id to remain NULL. Fixed by calling `NORMALIZE_DATA_IN_STAGING_GENERIC` instead of individual procedures.

See: `LAMBDA_FIX_REF_ID.md` in root directory.

## Database Schema

### platform_viewership Table
**Purpose**: Central staging table for all viewership data

**Key Columns**:
- `platform` - Data source (Tubi, Zype, Pluto, etc.)
- `filename` - Original upload filename
- `phase` - Processing stage (0, 1, 2)
- `processed` - Whether moved to final table
- `platform_content_id` - Platform's content identifier
- `ref_id` - Internal reference ID (matched from platform_content_id)
- `asset_series`, `asset_title` - Matched asset metadata
- `content_provider` - Content owner
- `deal_parent`, `partner`, `channel`, `territory` - Business dimensions
- `tot_hov`, `tot_mov` - Total hours/minutes of viewing
- `date`, `week`, `day`, `quarter`, `year`, `month` - Time dimensions

## Current State (Dec 2025)

**Staging/Production Separation**: ✅ Complete
- All procedures duplicated in `staging/` and `production/` directories
- Deployment scripts enforce correct database targeting
- No cross-contamination between environments

**Generic vs. Platform-Specific**: ✅ Migrated to Generic
- Old platform-specific tables deprecated
- All new uploads use `platform_viewership` table
- Generic procedures handle all platforms via `platform` parameter
