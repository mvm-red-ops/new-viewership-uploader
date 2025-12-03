# Snowflake Stored Procedures - Environment Management

## Directory Structure

```
stored_procedures/
├── staging/              # Staging environment procedures (UPLOAD_DB, TEST_STAGING, METADATA_MASTER_CLEANED_STAGING)
│   ├── buckets/          # Asset matching bucket procedures
│   ├── generic/          # Main processing procedures
│   └── helpers/          # Utility functions
├── production/           # Production environment procedures (UPLOAD_DB_PROD, NOSEY_PROD, METADATA_MASTER)
│   ├── buckets/          # Asset matching bucket procedures
│   ├── generic/          # Main processing procedures
│   └── helpers/          # Utility functions
├── deploy_staging.py     # Deploy all procedures to staging
└── deploy_production.py  # Deploy all procedures to production

# Legacy directories (keep for reference, do NOT edit):
├── buckets/              # Original templates (with {{variables}})
├── generic/              # Original templates
└── helpers/              # Original templates
```

## Database Mappings

### Staging Environment
- `{{STAGING_DB}}` → `TEST_STAGING`
- `{{METADATA_DB}}` → `METADATA_MASTER_CLEANED_STAGING`
- `{{UPLOAD_DB}}` → `UPLOAD_DB`

### Production Environment
- `{{STAGING_DB}}` → `NOSEY_PROD`
- `{{METADATA_DB}}` → `METADATA_MASTER`
- `{{UPLOAD_DB}}` → `UPLOAD_DB_PROD`

## Workflow

### Making Changes

1. **Edit the appropriate environment's files directly:**
   - For staging changes: Edit files in `staging/`
   - For production changes: Edit files in `production/`

2. **Deploy to staging:**
   ```bash
   cd snowflake/stored_procedures
   python3 deploy_staging.py
   ```

3. **Test in staging** using real data

4. **Copy working changes to production** (if applicable):
   ```bash
   # Manually copy the file from staging/ to production/
   # Then replace database names
   ```

5. **Deploy to production:**
   ```bash
   cd snowflake/stored_procedures
   python3 deploy_production.py
   ```
   You will be prompted to type "DEPLOY TO PRODUCTION" to confirm.

## Important Rules

1. **NEVER edit the original template files** in `buckets/`, `generic/`, or `helpers/` - these are legacy templates
2. **ALWAYS edit in `staging/` or `production/` directories**
3. **ALWAYS test in staging before deploying to production**
4. **When fixing a bug:** Fix it in both `staging/` and `production/` (or copy from staging to production after testing)

## Common Procedures

### Bucket Procedures (Asset Matching)
- `process_viewership_full_data_generic.sql` - Matches with series, season, episode, title
- `process_viewership_ref_id_series_generic.sql` - Matches with ref_id + series
- `process_viewership_ref_id_only_generic.sql` - Matches with ref_id only
- `process_viewership_series_only_generic.sql` - Matches with series only
- `process_viewership_series_season_episode_generic.sql` - Matches with series/season/episode
- `process_viewership_title_only_generic.sql` - Matches with title only

### Main Processing Procedures
- `normalize_data_in_staging.sql` - Phase 0 → 1 (normalization)
- `set_ref_id_from_platform_content_id.sql` - Maps platform_content_id to ref_id
- Generic phase processors

### Helper Procedures
- `extract_primary_title.sql`
- `calculate_viewership_metrics.sql`
- Date helpers

## Troubleshooting

### If you accidentally deploy wrong code:
1. Check which environment is affected (staging or production)
2. Find the correct version in the appropriate directory
3. Redeploy using the deploy script

### If staging and production diverge:
1. Identify which version is correct
2. Copy the correct file to the other environment
3. Update database references if copying from staging → production or vice versa
4. Redeploy

## Recent Fixes

- **2025-12-03**: Fixed `process_viewership_ref_id_series_generic.sql` - removed double `PUBLIC.public` bug in database references
- **2025-12-03**: Fixed `set_ref_id_from_platform_content_id.sql` - corrected metadata source tables for production
