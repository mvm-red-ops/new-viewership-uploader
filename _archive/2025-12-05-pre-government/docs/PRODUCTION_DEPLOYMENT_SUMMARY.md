# Production Deployment Summary

## Date: 2025-11-30

## Overview
Successfully deployed all revenue-by-episode and asset matching improvements to production (`UPLOAD_DB_PROD.PUBLIC`).

## Deployed Procedures

### 1. validate_viewership_for_insert
**Location:** `UPLOAD_DB_PROD.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(VARCHAR, VARCHAR, VARCHAR)`

**Features:**
- Revenue-specific validation
  - Checks: `revenue`, `year`, `quarter`, `year_month_day`
  - Does NOT require `week` or `day` (can be NULL for revenue-by-episode files)
- Viewership validation
  - Checks: `tot_mov`, `tot_hov`, `week`, `day`
- Conditional column selection to avoid errors when columns don't exist
- Uses `NOSEY_PROD.public.platform_viewership`

**Source:** `sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql`

---

### 2. set_date_columns_dynamic
**Location:** `UPLOAD_DB_PROD.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(VARCHAR, VARCHAR)`

**Features:**
- Detects if file has `date` column or only `month`/`year`
- For revenue-by-episode files (month/year only):
  - Converts month names to numbers: 'January' → '1', 'July' → '7', etc.
  - Calculates quarter from month number: q1, q2, q3, q4 (lowercase)
  - Sets `year_month_day` from year + month (day = 01)
  - Does NOT set `week` or `day` (leaves as NULL)
- For viewership files (with date column):
  - Uses existing date normalization logic
  - Sets all date fields including `week` and `day`
- Uses `NOSEY_PROD.public.platform_viewership`

**Source:** `snowflake/stored_procedures/generic/set_date_columns_dynamic.sql`

---

### 3. process_viewership_ref_id_series_generic
**Location:** `UPLOAD_DB_PROD.PUBLIC.PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC(VARCHAR, VARCHAR)`

**Features:**
- UNION fallback logic for asset matching
  - First SELECT: Matches on `ref_id` + `internal_series` + `platform_content_name` to metadata title
  - Second SELECT (UNION): Matches on `ref_id` + `internal_series` only (no title check)
  - Uses `NOT EXISTS` to prevent duplicates
- Only processes records where:
  - `ref_id IS NOT NULL`
  - `internal_series IS NOT NULL`
  - `content_provider IS NULL` (not already matched)
- Uses `NOSEY_PROD.public.platform_viewership` and `METADATA_MASTER.public.*`

**Source:** `snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql`

---

### 4. process_viewership_ref_id_only_generic
**Location:** `UPLOAD_DB_PROD.PUBLIC.PROCESS_VIEWERSHIP_REF_ID_ONLY_GENERIC(VARCHAR, VARCHAR)`

**Features:**
- UNION fallback logic for asset matching (same as ref_id_series but without internal_series requirement)
  - First SELECT: Matches on `ref_id` + `platform_content_name` to metadata title
  - Second SELECT (UNION): Matches on `ref_id` only (no title check)
  - Uses `NOT EXISTS` to prevent duplicates
- Only processes records where:
  - `ref_id IS NOT NULL`
  - `content_provider IS NULL` (not already matched)
- Uses `NOSEY_PROD.public.platform_viewership` and `METADATA_MASTER.public.*`

**Source:** `snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_only.sql`

---

## Database References

All procedures correctly use production databases:
- `UPLOAD_DB_PROD` - Production upload database
- `NOSEY_PROD` - Production staging database (equivalent to TEST_STAGING in staging)
- `ASSETS` - Production assets database
- `METADATA_MASTER` - Production metadata database (equivalent to METADATA_MASTER_CLEANED_STAGING in staging)

## Configuration

Production database names are defined in `sql/deploy/config.yaml`:
```yaml
environments:
  prod:
    UPLOAD_DB: "UPLOAD_DB_PROD"
    STAGING_DB: "NOSEY_PROD"
    ASSETS_DB: "ASSETS"
    EPISODE_DETAILS_TABLE: "EPISODE_DETAILS"
    METADATA_DB: "METADATA_MASTER"
```

## Verification

All procedures have been verified to:
1. Use correct production database references (no TEST_STAGING or UPLOAD_DB references)
2. Contain all expected features and logic
3. Have proper GRANT permissions for `web_app` role

Run `python verify_final_correct.py` to re-verify deployment.

## Bug Fixes Included

### 1. Active Deals Mapping (Fixed 2025-11-30)
**Issue:** `dictionary.public.active_deals` had incorrect entry for deal 39:
- Matched on: `platform_partner_name='Tubi VOD'`, `platform_channel_name='VOD'`
- But set: `internal_partner='Tubi Linear'`, `internal_channel='Nosey'`

**Fix:** Updated the row to:
- `deal_parent=40` (changed from 39)
- `internal_partner='Tubi VOD'` (changed from 'Tubi Linear')
- `internal_channel='VOD'` (changed from 'Nosey')

Now files with channel='VOD' correctly match to deal 40 (VOD deal) instead of deal 39 (Linear deal).

## Testing

To test revenue-by-episode uploads in production:
1. Upload a file with `month` and `year` columns (no `date` column)
2. Verify normalization converts month names to numbers and calculates quarter
3. Verify asset matching uses fallback logic (matches even when title doesn't match)
4. Verify validation requires `year`, `quarter`, `year_month_day` (not `week`/`day`)
5. Verify final table has correct channel/partner from active_deals mapping

## Deployment Scripts

- `deploy_to_prod_correct.py` - Deploys procedures to UPLOAD_DB_PROD
- `verify_final_correct.py` - Verifies all features are present
- `fix_active_deals.py` - Fixes the Tubi VOD/Linear channel mapping

## Next Steps

For future deployments, use:
```bash
cd sql/deploy
python deploy.py --env prod
```

Note: This requires Streamlit secrets configured. For manual deployment, use the scripts in the root directory.
