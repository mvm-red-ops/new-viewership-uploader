# Generic Procedures Deployment Guide

## Overview

This directory contains SQL files for deploying generic (platform-agnostic) stored procedures that process viewership data from `test_staging.public.platform_viewership` table.

## File Structure

```
sql/
├── DEPLOY_GENERIC_CONTENT_REFERENCES.sql      ← Deploy FIRST
│   └── Sub-procedures for asset matching buckets
│
├── DEPLOY_ALL_GENERIC_PROCEDURES.sql          ← Deploy SECOND
│   └── Main procedures (orchestration + business logic)
│
└── README_DEPLOYMENT.md                        ← This file
```

## Prerequisites

**IMPORTANT: The `platform_viewership` table MUST have an `id` column!**

The ID column and sequence are automatically created if you use:
- `create_platform_viewership.sql` (for staging/development)
- `create_platform_viewership_prod.sql` (for production)

If your table already exists without an ID column, you'll need to add it manually:
```sql
-- Create sequence
CREATE SEQUENCE IF NOT EXISTS upload_db.public.platform_viewership_id_seq
    START = 1 INCREMENT = 1;

-- Add ID column
ALTER TABLE upload_db.public.platform_viewership
    ADD COLUMN id NUMBER DEFAULT upload_db.public.platform_viewership_id_seq.NEXTVAL NOT NULL;

-- Backfill existing records
UPDATE upload_db.public.platform_viewership
SET id = upload_db.public.platform_viewership_id_seq.NEXTVAL
WHERE id IS NULL;
```

## Deployment Order

### Step 1: Add Missing Columns to Existing Tables
```sql
-- Run this file FIRST if you have existing platform_viewership tables:
@/Users/tayloryoung/work/new-viewership-uploader/sql/ALTER_ADD_DATE_COLUMNS.sql
```

**Adds date columns to both databases:**
- `FULL_DATE` - Used by set_date_columns_dynamic
- `WEEK` - Required for final table insert
- `DAY` - Required for final table insert

**Must run on BOTH:**
- `upload_db.public.platform_viewership` (where Streamlit loads data)
- `test_staging.public.platform_viewership` (where Lambda processes data)

### Step 2: Deploy Content References Sub-Procedures
```sql
-- Run this file SECOND in Snowflake:
@/Users/tayloryoung/work/new-viewership-uploader/sql/DEPLOY_GENERIC_CONTENT_REFERENCES.sql
```

**Creates 6 sub-procedures** (will be called by `analyze_and_process_viewership_data_generic`):
1. `process_viewership_full_data_generic` - Best case (all fields)
2. `process_viewership_ref_id_series_generic` - Has ref_id + series
3. `process_viewership_ref_id_only_generic` - Has ref_id only
4. `process_viewership_series_season_episode_generic` - Has series + ep/season
5. `process_viewership_series_only_generic` - Has series only
6. `process_viewership_title_only_generic` - Worst case (title only)

### Step 3: Deploy Main Procedures
```sql
-- Run this file THIRD in Snowflake:
@/Users/tayloryoung/work/new-viewership-uploader/sql/DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

**Creates 11 procedures:**
- `set_phase_generic` - Updates processing phase
- `calculate_viewership_metrics` - Calculates TOT_HOV/TOT_MOV
- `set_date_columns_dynamic` - Sets date columns (full_date, week, day, quarter, year, month)
- `set_deal_parent_generic` - Sets normalized fields from active_deals
- `set_channel_generic` - Fallback pattern matching for channel
- `set_territory_generic` - Normalizes territory names
- `send_unmatched_deals_alert` - Email alerts for unmatched records
- `set_internal_series_generic` - Matches platform series names
- **`analyze_and_process_viewership_data_generic`** - Main content references orchestrator
- `move_data_to_final_table_dynamic_generic` - Moves to final table
- `handle_final_insert_dynamic_generic` - Phase 3 orchestrator

### Step 4: Deploy Validation Procedure
```sql
-- Run this file FOURTH in Snowflake:
@/Users/tayloryoung/work/new-viewership-uploader/sql/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
```

**Creates validation procedure:**
- `validate_viewership_for_insert` - Validates records before final insert
  - Checks test_staging.public.platform_viewership (not upload_db)
  - Validates all required fields including week/day columns
  - Returns JSON with validation results

### Step 5: Update Platform Config (Optional for date handling)
```sql
-- Run this file FIFTH if you want automatic date column population:
@/Users/tayloryoung/work/new-viewership-uploader/sql/UPDATE_PLATFORM_CONFIG_FOR_DATE_COLUMNS.sql
```

**Enables set_date_columns_dynamic in normalization:**
- Updates `dictionary.public.platform_config` for your platform
- Sets `has_custom_date_handling = TRUE`
- Sets `date_handling_procedure = 'set_date_columns_dynamic'`

## How It Works

### Content References Processing Flow

```
1. analyze_and_process_viewership_data_generic() is called
   ↓
2. Categorizes records into buckets based on which fields exist
   (ref_id? internal_series? episode_number? season_number?)
   ↓
3. Creates temporary bucket tables (TEMP_{PLATFORM}_{BUCKET}_BUCKET)
   ↓
4. Processes each bucket in order:
   ├─ Calls process_viewership_full_data_generic()
   ├─ Calls process_viewership_ref_id_series_generic()
   ├─ Calls process_viewership_ref_id_only_generic()
   ├─ Calls process_viewership_series_season_episode_generic()
   ├─ Calls process_viewership_series_only_generic()
   └─ Calls process_viewership_title_only_generic()
   ↓
5. Each sub-procedure:
   - Joins to metadata_master_cleaned_staging.public.{episode, series, metadata}
   - Sets content_provider, series_code, asset_title, asset_series
   - Tracks unmatched records
   ↓
6. Returns summary of matched records
```

## Key Differences from Platform-Specific Version

| Old (Platform-Specific) | New (Generic) |
|-------------------------|---------------|
| `test_staging.public.{platform}_viewership` | `test_staging.public.platform_viewership` |
| Separate procedures per platform | Single set with platform filter |
| Called like: `process_viewership_full_data('wurl', 'file.csv')` | Called like: `process_viewership_full_data_generic('wurl', 'file.csv')` |
| Database name: varies | Database name: always `test_staging` |

## Important Notes

⚠️ **DO NOT** modify the bucket categorization logic in `analyze_and_process_viewership_data_generic`. It's a carefully designed system that ensures data integrity.

⚠️ All sub-procedures use the **correct 3-table join structure**:
- `metadata_master_cleaned_staging.public.episode`
- `metadata_master_cleaned_staging.public.series`
- `metadata_master_cleaned_staging.public.metadata`

⚠️ The old incorrect reference to `assets_staging.public.episode_details` has been removed.

## Source Files

Individual sub-procedure source files are in:
```
snowflake/stored_procedures/-sub-procedures/content_references/generic/
├── full_data.sql
├── ref_id_series.sql
├── ref_id_only.sql
├── series_season_episode.sql
├── series_only.sql
└── title_only.sql
```

These are combined into `DEPLOY_GENERIC_CONTENT_REFERENCES.sql` for easy deployment.

## CRITICAL: Deployment Process

**⚠️ IMPORTANT: Individual procedure files in `snowflake/stored_procedures/` are NOT automatically deployed!**

The deployment uses **template files** in `sql/templates/`:
- `DEPLOY_ALL_GENERIC_PROCEDURES.sql` - Contains ALL main procedures inline
- `DEPLOY_GENERIC_CONTENT_REFERENCES.sql` - Contains ALL content reference sub-procedures inline

**To deploy changes:**

1. **Make changes in the template files directly** (sql/templates/*.sql)
   - OR regenerate templates from individual files (if you have a script)

2. **Use the deployment script:**
   ```bash
   # Deploy to staging
   python3 sql/deploy/deploy.py --env staging

   # Deploy to production
   python3 sql/deploy/deploy.py --env prod
   ```

3. **The script automatically replaces template variables:**
   - `{{UPLOAD_DB}}` → UPLOAD_DB (staging) or UPLOAD_DB_PROD (prod)
   - `{{STAGING_DB}}` → TEST_STAGING (staging) or NOSEY_PROD (prod)
   - `{{METADATA_DB}}` → METADATA_MASTER_CLEANED_STAGING or METADATA_MASTER
   - `{{ASSETS_DB}}` → STAGING_ASSETS or ASSETS

**DO NOT:**
- ❌ Manually deploy individual procedure files - they won't have template variables replaced
- ❌ Edit procedures directly in Snowflake - changes will be overwritten on next deploy
- ❌ Hardcode database names - always use template variables like {{STAGING_DB}}

**Configuration:**
- See `sql/deploy/config.yaml` for deployment order and environment settings
