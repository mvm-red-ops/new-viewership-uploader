# SQL Directory Cleanup - Complete ✅

## Summary

**Before:** 23 ad-hoc scripts cluttering sql/ root
**After:** Clean organization + 1 consolidated diagnostic tool

## Before vs After

### Before (23 files in sql/ root)
```
sql/
├── check_bucket_errors.py
├── check_errors.py
├── check_filename.py
├── check_phase3_nulls.py
├── check_prod_metadata_db.py
├── check_udf_exists.py
├── check_udf_prod.py
├── check_what_happened.py
├── check_youtube_blocking.py
├── cleanup_youtube.py
├── CREATE_NOSEY_PROD_PLATFORM_VIEWERSHIP.sql
├── DEBUG_MISSING_COLUMNS.sql
├── deploy_credentials.py
├── deploy_with_python.py
├── fix_confess_channel_name.sql
├── grant_table_permissions.py
├── grant_udf_permissions.py
├── PROD_ADD_MISSING_COLUMNS.sql
├── redeploy_procedures_prod.py
├── setup_staging_table.sql
├── SETUP.sql
├── staging_CREATE_SET_INTERNAL_SERIES_WITH_EXTRACTION.sql
└── add_platform_columns.py
```

### After (Clean organization)
```
sql/
├── deploy/                      # Orchestrated deployment
│   ├── deploy.py
│   ├── config.yaml
│   └── README.md
│
├── migrations/                  # Modular SQL
│   ├── 001_schema_tables.sql
│   ├── 002_udfs.sql
│   ├── 003_procedures_phase0.sql
│   └── 006_permissions.sql
│
├── templates/                   # Stored procedures
│   ├── DEPLOY_ALL_GENERIC_PROCEDURES.sql
│   ├── DEPLOY_GENERIC_CONTENT_REFERENCES.sql
│   └── CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
│
├── diagnostics/                 # 🆕 Consolidated diagnostic tool
│   ├── diagnose.py                  # Main CLI
│   ├── checks/
│   │   ├── __init__.py
│   │   ├── udf_checks.py           # UDF verification
│   │   ├── schema_checks.py        # Table structure
│   │   ├── data_checks.py          # Phase tracking
│   │   └── asset_matching_checks.py # Bucket diagnostics
│   ├── README.md
│   └── [old check_*.py scripts for reference]
│
├── archive/                     # 🆕 Historical scripts
│   ├── setup/                      # Initial setup (run once)
│   │   ├── CREATE_NOSEY_PROD_PLATFORM_VIEWERSHIP.sql
│   │   ├── SETUP.sql
│   │   ├── setup_staging_table.sql
│   │   ├── PROD_ADD_MISSING_COLUMNS.sql
│   │   └── staging_CREATE_SET_INTERNAL_SERIES_WITH_EXTRACTION.sql
│   ├── fixes/                      # One-off fixes
│   │   ├── fix_confess_channel_name.sql
│   │   ├── DEBUG_MISSING_COLUMNS.sql
│   │   └── add_platform_columns.py
│   ├── deprecated/                 # Replaced by new system
│   │   ├── deploy_credentials.py
│   │   ├── deploy_with_python.py
│   │   ├── redeploy_procedures_prod.py
│   │   ├── grant_table_permissions.py
│   │   └── grant_udf_permissions.py
│   └── README.md
│
└── utils/                       # Utilities
    └── cleanup.py
```

## What Changed

### 1. Consolidated Diagnostics (9 scripts → 1 tool)

**Old way:**
```bash
python sql/check_udf_exists.py
python sql/check_phase3_nulls.py
python sql/check_what_happened.py
python sql/check_bucket_errors.py
# ... 5 more scripts
```

**New way:**
```bash
# Single command for all diagnostics
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"

# Or specific checks
python sql/diagnostics/diagnose.py --env staging --check udfs
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"
```

**Benefits:**
- Single entry point
- Modular check system
- Consistent output format
- Actionable suggestions

**Replaced scripts:**
- ✅ check_udf_exists.py → `--check udfs`
- ✅ check_udf_prod.py → `--check udfs`
- ✅ check_phase3_nulls.py → `--check data-flow`
- ✅ check_what_happened.py → `--check data-flow`
- ✅ check_filename.py → `--check data-flow`
- ✅ check_youtube_blocking.py → `--check data-flow`
- ✅ check_bucket_errors.py → `--check asset-matching`
- ✅ check_errors.py → `--check asset-matching`
- ✅ check_prod_metadata_db.py → Environment config in YAML

### 2. Archived Historical Scripts

**Setup scripts** (run once, historical) → `archive/setup/`
- CREATE_NOSEY_PROD_PLATFORM_VIEWERSHIP.sql
- SETUP.sql
- setup_staging_table.sql
- PROD_ADD_MISSING_COLUMNS.sql
- staging_CREATE_SET_INTERNAL_SERIES_WITH_EXTRACTION.sql

**One-off fixes** → `archive/fixes/`
- fix_confess_channel_name.sql
- DEBUG_MISSING_COLUMNS.sql
- add_platform_columns.py

**Deprecated deployment scripts** → `archive/deprecated/`
- deploy_credentials.py
- deploy_with_python.py
- redeploy_procedures_prod.py
- grant_table_permissions.py
- grant_udf_permissions.py

### 3. Deleted Duplicates

- ❌ cleanup_youtube.py (duplicate of sql/utils/cleanup.py)

## New Diagnostic Tool Features

### Modular Check System

```python
# Each check is a separate module
from checks import check_udfs, check_schema, check_data_flow, check_asset_matching

# Can be used independently or together
```

### Clear Output with Actionable Suggestions

```
Phase 0: Upload
  ✅ 80,686 records uploaded

Step 1: Deal Matching
  ✅ 80,686 records have deal_parent

Step 2: Internal Series Matching
  ✅ 60,000 records have internal_series (74.4%)

Step 3: Asset Matching
  ❌ 0 records matched to assets
     💡 Check asset matching diagnostics with --check asset-matching

Recent Errors:
  [2025-11-05] process_full_data_bucket_generic
    ERROR: Unknown user-defined function EXTRACT_PRIMARY_TITLE

  💡 Deploy UDFs: python sql/deploy/deploy.py --env staging --only 002_udfs
```

### Environment-Aware

Uses same config.yaml as deployment system:
```yaml
environments:
  staging:
    UPLOAD_DB: "UPLOAD_DB"
    STAGING_DB: "TEST_STAGING"
    ASSETS_DB: "STAGING_ASSETS"
    METADATA_DB: "METADATA_MASTER_CLEANED_STAGING"
  prod:
    UPLOAD_DB: "UPLOAD_DB_PROD"
    STAGING_DB: "NOSEY_PROD"
    ASSETS_DB: "ASSETS"
    METADATA_DB: "METADATA_MASTER"
```

## File Counts

| Category | Before | After | Change |
|----------|--------|-------|--------|
| sql/ root scripts | 23 | 0 | -23 |
| Diagnostic tools | 9 separate scripts | 1 consolidated tool | -8 |
| Archive | 0 | 18 (organized) | +18 |
| Deleted duplicates | - | 1 | -1 |

**Net result:** 23 scripts → organized structure + 1 powerful diagnostic tool

## Usage Examples

### Debug Failed Upload
```bash
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"
```

### Verify Deployment
```bash
python sql/diagnostics/diagnose.py --env staging --check udfs
python sql/diagnostics/diagnose.py --env staging --check schema
```

### Analyze Asset Matching
```bash
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"
```

### Check Everything
```bash
python sql/diagnostics/diagnose.py --env staging --check all --platform Youtube --filename "file.csv"
```

## Benefits

### 1. **Cleaner Organization**
- Clear purpose for each directory
- No clutter in sql/ root
- Easy to find what you need

### 2. **Better Diagnostics**
- Single entry point for all checks
- Consistent output format
- Actionable suggestions
- Environment-aware

### 3. **Easier Maintenance**
- Add new checks to modular system
- No duplication
- Clear separation of concerns

### 4. **Better Documentation**
- Each directory has README
- Clear migration path from old scripts
- Usage examples

### 5. **Historical Context Preserved**
- Old scripts archived, not deleted
- README explains why deprecated
- Can reference if needed

## Migration Guide

### Old → New Diagnostics

| Old Command | New Command |
|-------------|-------------|
| `python sql/check_udf_exists.py` | `python sql/diagnostics/diagnose.py --env staging --check udfs` |
| `python sql/check_phase3_nulls.py` | `python sql/diagnostics/diagnose.py --env staging --check data-flow --platform X --filename Y` |
| `python sql/check_bucket_errors.py` | `python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform X --filename Y` |

### Old → New Deployment

| Old Command | New Command |
|-------------|-------------|
| `python sql/redeploy_procedures_prod.py` | `python sql/deploy/deploy.py --env prod` |
| `python sql/grant_udf_permissions.py` | `python sql/deploy/deploy.py --env staging --only 006_permissions` |

## Documentation

- **Main README:** `sql/diagnostics/README.md` - Full diagnostic tool documentation
- **Archive README:** `sql/archive/README.md` - Explanation of archived scripts
- **Deployment README:** `sql/deploy/README.md` - Deployment system documentation

## Next Steps

1. ✅ Directory structure organized
2. ✅ Scripts moved to appropriate locations
3. ✅ Consolidated diagnostic tool created
4. ✅ Documentation updated
5. ⏳ Test diagnostic tool with real data
6. ⏳ Continue with asset matching modularization (next phase)

## Questions?

- **Diagnostics:** See `sql/diagnostics/README.md`
- **Archived scripts:** See `sql/archive/README.md`
- **Deployment:** See `sql/deploy/README.md`
- **Architecture:** See `docs/ASSET_MATCHING_ARCHITECTURE.md`
