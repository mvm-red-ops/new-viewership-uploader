# Diagnostics

Consolidated diagnostic tool for the viewership upload pipeline. Replaces 9+ individual check scripts with a single modular tool.

## Quick Start

```bash
# Check everything for a specific upload
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "youtube_daily_2025-07-01_to_2025-09-30.csv"

# Check only UDFs and schema
python sql/diagnostics/diagnose.py --env staging --check udfs
python sql/diagnostics/diagnose.py --env staging --check schema

# Check asset matching performance
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"
```

## Check Types

### `--check udfs`
Verifies required User-Defined Functions exist and have correct permissions:
- EXTRACT_PRIMARY_TITLE function exists in UPLOAD_DB.PUBLIC
- WEB_APP role has USAGE permission

**Use when:**
- Asset matching is failing with "Unknown UDF" errors
- After deploying UDFs

### `--check schema`
Validates database schema and table structure:
- EPISODE_DETAILS table exists
- Required columns present (PLATFORM_PARTNER_NAME, PLATFORM_CHANNEL_NAME, PLATFORM_TERRITORY, START_TIME, END_TIME, TOT_COMPLETIONS)
- record_reprocessing_batch_logs has correct permissions

**Use when:**
- INSERT queries failing with "invalid identifier" errors
- After schema migrations

### `--check data-flow`
Tracks data progression through pipeline phases:
- **Phase 0:** Upload count
- **Step 1:** Deal matching (deal_parent populated)
- **Step 2:** Internal series matching (internal_series populated)
- **Step 3:** Asset matching (ref_id, asset_series populated)
- **Phase 3:** INSERT eligibility

Shows which step is blocking the pipeline.

**Use when:**
- Records uploaded but not appearing in EPISODE_DETAILS
- Verification shows 0 inserted records
- Need to trace where data is getting stuck

**Requires:** `--platform` and `--filename`

### `--check asset-matching`
Analyzes asset matching strategy performance:
- Recent errors from bucket procedures
- Strategy performance (which buckets matched records)
- Unmatched records count and samples
- Common failure patterns (missing platform_content_id, missing internal_series)
- Unmatched logging verification

**Use when:**
- Most/all records not matching to assets
- Need to understand which asset matching strategies are failing
- Tuning matching thresholds

**Requires:** `--platform` and `--filename`

### `--check all` (default)
Runs all checks. For data-flow and asset-matching, requires `--platform` and `--filename`.

## Example Workflows

### Debugging Failed Upload

```bash
# 1. Check overall data flow
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"

# Output shows:
# ✅ Phase 0: 80,686 records uploaded
# ✅ Step 1: 80,686 records have deal_parent
# ⚠️  Step 2: 60,000 records have internal_series (74.4%)
# ❌ Step 3: 0 records matched to assets
#
# DIAGNOSIS: Asset matching completely failed

# 2. Drill into asset matching
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"

# Output shows:
# Recent Errors:
#   [2025-11-05] process_full_data_bucket_generic
#     ERROR: Unknown user-defined function EXTRACT_PRIMARY_TITLE
#
# DIAGNOSIS: UDF missing permissions

# 3. Check UDF
python sql/diagnostics/diagnose.py --env staging --check udfs

# Output shows:
# ✅ Found EXTRACT_PRIMARY_TITLE in UPLOAD_DB.PUBLIC
# ❌ WEB_APP role missing USAGE permission
#
# SOLUTION: Deploy permissions
python sql/deploy/deploy.py --env staging --only 006_permissions
```

### After Deployment Verification

```bash
# Verify deployment was successful
python sql/diagnostics/diagnose.py --env staging --check udfs
python sql/diagnostics/diagnose.py --env staging --check schema

# Both should show ✅ for all checks
```

### Analyzing Asset Matching Performance

```bash
# Check which strategies are matching
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"

# Shows bucket-level performance:
#   FULL_DATA bucket matched: 0 records
#   REF_ID_SERIES bucket matched: 0 records
#   SERIES_SEASON_EPISODE bucket matched: 5,000 records
#   SERIES_ONLY bucket matched: 2,000 records
#   TITLE_ONLY bucket matched: 15 records
#
# Unmatched: 73,671 records
# Common patterns:
#   ⚠️  73,671 records missing platform_content_id
```

## Modular Check System

Checks are organized in `checks/` directory:

```
checks/
├── __init__.py
├── udf_checks.py              # UDF existence and permissions
├── schema_checks.py           # Table structure and columns
├── data_checks.py             # Phase tracking and data flow
└── asset_matching_checks.py   # Bucket performance and diagnostics
```

Each module can be imported and used independently:

```python
from checks import check_udfs, check_schema

cursor = conn.cursor()
env_config = load_env_config('staging')

check_udfs(cursor, env_config)
check_schema(cursor, env_config)
```

## Common Issues and Solutions

### ❌ Unknown UDF EXTRACT_PRIMARY_TITLE
**Check:** `--check udfs`
**Solution:** `python sql/deploy/deploy.py --env staging --only 002_udfs`

### ❌ Invalid identifier PLATFORM_PARTNER_NAME
**Check:** `--check schema`
**Solution:** `python sql/deploy/deploy.py --env staging --only 001_schema_tables`

### ❌ Insufficient privileges on record_reprocessing_batch_logs
**Check:** `--check schema`
**Solution:** `python sql/deploy/deploy.py --env staging --only 006_permissions`

### ❌ 0 records matched to assets
**Check:** `--check asset-matching`
**Common causes:**
- UDF permissions missing
- platform_content_id not populated
- full_data table doesn't have matching content

### ⚠️  High unmatched count but some matches
**Check:** `--check asset-matching`
**Analysis:** Review sample unmatched records and adjust matching strategies

## Replaced Scripts

This tool consolidates functionality from:

- `check_udf_exists.py` → `--check udfs`
- `check_udf_prod.py` → `--check udfs`
- `check_phase3_nulls.py` → `--check data-flow`
- `check_what_happened.py` → `--check data-flow`
- `check_filename.py` → `--check data-flow`
- `check_youtube_blocking.py` → `--check data-flow`
- `check_bucket_errors.py` → `--check asset-matching`
- `check_errors.py` → `--check asset-matching`
- `check_prod_metadata_db.py` → Environment config in YAML

Old scripts remain in `sql/diagnostics/` for reference but should not be used.

## Tips

1. **Start broad, then narrow:** Run `--check all` first, then drill into specific components
2. **Read error messages carefully:** Each check provides actionable "💡" suggestions
3. **Use with deployment:** Run diagnostics after deployment to verify success
4. **Platform/filename required:** Most useful checks need specific upload context
