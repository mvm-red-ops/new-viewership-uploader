# SQL Archive

This directory contains scripts that are no longer actively used but kept for reference.

## Directory Structure

```
archive/
├── setup/        # Initial setup scripts (run once, historical)
├── fixes/        # One-off fixes for specific issues
└── deprecated/   # Replaced by new deployment system
```

## Setup Scripts (`setup/`)

Scripts that were run once during initial database setup:

- `CREATE_NOSEY_PROD_PLATFORM_VIEWERSHIP.sql` - Created prod table structure
- `SETUP.sql` - Initial database setup
- `setup_staging_table.sql` - Created staging table structure
- `PROD_ADD_MISSING_COLUMNS.sql` - Added columns to prod tables
- `staging_CREATE_SET_INTERNAL_SERIES_WITH_EXTRACTION.sql` - Created series matching procedure

**Note:** These scripts have already been executed. Schema changes should now go in `sql/migrations/`.

## One-off Fixes (`fixes/`)

Scripts written to fix specific issues:

- `fix_confess_channel_name.sql` - Fixed channel name for specific content
- `DEBUG_MISSING_COLUMNS.sql` - Debugged missing column issue
- `add_platform_columns.py` - Added platform columns (now in migrations/001_schema_tables.sql)

**Note:** These were temporary fixes. Permanent solutions are in the main system.

## Deprecated Scripts (`deprecated/`)

Scripts replaced by the new deployment system (`sql/deploy/deploy.py`):

- `deploy_credentials.py` - Old credential deployment
- `deploy_with_python.py` - Old Python-based deployment
- `redeploy_procedures_prod.py` - Replaced by `sql/deploy/deploy.py --env prod`
- `grant_table_permissions.py` - Now in `sql/migrations/006_permissions.sql`
- `grant_udf_permissions.py` - Now in `sql/migrations/006_permissions.sql`

**Use the new system instead:**
```bash
python sql/deploy/deploy.py --env staging
python sql/deploy/deploy.py --env prod
```

## Why Archive Instead of Delete?

These scripts provide:
1. **Historical context** - Shows how the system evolved
2. **Reference** - Useful if we need to rebuild from scratch
3. **Documentation** - Shows which issues were encountered and solved

## If You Need to Use These Scripts

You probably don't. But if you must:

```bash
# From sql/archive directory
python deprecated/redeploy_procedures_prod.py
```

**Better option:** Use the new deployment system which includes all fixes and improvements.
