# Reference: Deprecated Snowflake Code

This document indexes **deprecated** code that is no longer in active use but preserved for historical reference.

## Archived Pre-Government (2025-12-05)

**Location**: `_archive/2025-12-05-pre-government/`

### One-Off SQL Scripts (Root)
- `cleanup_tubi_test.sql` - One-time Tubi data cleanup
- `drop_old_validation.sql` - One-time migration dropping old validation tables
- `fix_tubi_data.sql` - One-time data correction script

### One-Off Python Scripts (Snowflake)
- `deploy_prod_direct.py` - Workaround deployment script for emergency production updates
- `fix_production_database_names.py` - One-time migration fixing database name references

### Historical Documentation
- `DEPLOY_LAMBDA.md` - Superseded by `DEPLOY_LAMBDA_FINAL.md` (in root)
- `DEPLOYMENT_CHECKLIST.md` - Old deployment workflow
- `DEPLOYMENT_VERIFICATION.md` - Old verification steps
- `PRODUCTION_DEPLOYMENT_SUMMARY.md` - Historical deployment record
- `READY_TO_DEPLOY.md` - Old pre-deployment readiness doc
- `REVENUE_BY_EPISODE_FIXES.md` - Historical fix documentation

## Platform-Specific Tables (Deprecated)

**Timeline**: Deprecated circa 2024-2025 in favor of generic `platform_viewership` table

**Old Architecture**:
- `tubi_viewership` table
- `zype_viewership` table
- `pluto_viewership` table
- Platform-specific stored procedures (`normalize_tubi_data`, etc.)

**Why Deprecated**:
- Required duplicate procedures for each platform
- Hard to maintain and extend
- Replaced by generic procedures that accept `platform` parameter

**Migration Status**: All new uploads use `platform_viewership` table with `platform` column

## Old Normalization Pattern

**Location**: `sql/archive/old_normalize_*.sql` (if exists)

**Old Pattern**:
```sql
-- Platform-specific procedure
CREATE OR REPLACE PROCEDURE NORMALIZE_TUBI_DATA()
...
-- Hard-coded table reference
FROM upload_db.public.tubi_viewership
```

**New Pattern**:
```sql
-- Generic procedure
CREATE OR REPLACE PROCEDURE NORMALIZE_DATA_IN_STAGING_GENERIC(platform STRING, filename STRING)
...
-- Dynamic table reference
FROM upload_db.public.platform_viewership
WHERE platform = ? AND filename = ?
```

## Legacy S3 Upload Path

**Status**: Still supported but being phased out in favor of Streamlit uploads

**Old Flow**:
1. S3 upload → Lambda trigger
2. Raw CSV data → `UPLOAD_DB.platform_viewership` (phase=0, not normalized)
3. `MOVE_VIEWERSHIP_TO_STAGING` → `TEST_STAGING.platform_viewership`
4. Manual individual procedure calls:
   - `SET_DEAL_PARENT`
   - `CALCULATE_VIEWERSHIP_METRICS`
   - `SET_DATE_COLUMNS`
   - `SET_PHASE`
5. Asset matching and final insert

**New Flow (Streamlit)**:
1. Streamlit UI upload with validation
2. Pre-normalized data → `UPLOAD_DB.platform_viewership` (already has deal/partner/etc.)
3. `MOVE_STREAMLIT_DATA_TO_STAGING` → `TEST_STAGING.platform_viewership`
4. `NORMALIZE_DATA_IN_STAGING_GENERIC` wrapper (includes critical `SET_REF_ID` step)
5. Asset matching and final insert

**Key Difference**: Streamlit uploads arrive pre-normalized, reducing error potential and processing time.

## Historical Bugs and Fixes

### Bug: Missing ref_id in Streamlit Upload Path (Dec 2025)

**Symptom**: Streamlit uploads had NULL `ref_id` values, causing asset matching failures

**Root Cause**: Lambda was calling individual normalization procedures instead of the comprehensive wrapper, skipping `SET_REF_ID_FROM_PLATFORM_CONTENT_ID`

**Fix**: Lambda now calls `NORMALIZE_DATA_IN_STAGING_GENERIC` which includes the ref_id mapping step

**Documentation**: `LAMBDA_FIX_REF_ID.md` in root directory

### Bug: Production Database Name Hardcoding

**Symptom**: Production procedures had hardcoded `TEST_STAGING` references instead of `NOSEY_PROD`

**Root Cause**: Procedures were copied from staging without updating database references

**Fix**: Created separate `production/` directory with correct database names:
- `NOSEY_PROD` instead of `TEST_STAGING`
- `ASSETS` instead of `STAGING_ASSETS`
- `UPLOAD_DB_PROD` instead of `UPLOAD_DB`

**Tool**: `fix_production_database_names.py` (now archived)

## Superseded Documentation

The following documents in root directory remain **CANONICAL** (not deprecated):
- `ARCHITECTURE.md` - Current system architecture
- `TROUBLESHOOTING.md` - Active debugging guide
- `LAMBDA_FIX_REF_ID.md` - Critical fix reference
- `DEPLOY_LAMBDA_FINAL.md` - Current Lambda deployment process
- `CHEATSHEET.md` - Active command reference

## Using This Reference

When reviewing old code or investigating historical bugs:
1. Check this document first to understand context
2. Verify if the pattern/code is deprecated
3. Refer to CANONICAL.md for current best practices
4. Don't replicate deprecated patterns in new code

## Deprecation Process

Before deprecating code:
1. Verify it's not in active use
2. Document why it's being deprecated
3. Update this REFERENCE.md
4. Move to appropriate archive location
5. Update CANONICAL.md if replacement exists
