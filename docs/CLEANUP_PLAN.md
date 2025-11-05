# Directory Cleanup Plan

## Current Problem
Too many files in root directory - hard to find what you need.

## New Structure

```
new-viewership-uploader/
├── app.py                          # Main Streamlit app (KEEP IN ROOT)
├── config.py                       # Configuration (KEEP IN ROOT)
├── requirements.txt                # Dependencies (KEEP IN ROOT)
├── README.md                       # Main readme (KEEP IN ROOT)
│
├── src/                            # Application code
│   ├── __init__.py
│   ├── column_mapper.py           # ← MOVE HERE
│   ├── snowflake_utils.py         # ← MOVE HERE
│   ├── transformations.py         # ← MOVE HERE
│   └── wide_format_handler.py     # ← MOVE HERE
│
├── docs/                           # Documentation
│   ├── DEPLOYMENT_SUMMARY.md      # ← MOVE HERE
│   ├── REORGANIZATION_COMPLETE.md # ← MOVE HERE
│   └── REORGANIZATION_PLAN.md     # ← MOVE HERE
│
├── scripts/                        # Deployment & utilities
│   ├── setup_database.py          # ← MOVE HERE
│   └── legacy/                    # Old deployment scripts
│       ├── deploy_lambda.sh       # ← MOVE HERE (deprecated)
│       ├── deploy_snowflake.sh    # ← MOVE HERE (deprecated)
│       ├── redeploy_procedures_prod.py      # ← MOVE HERE (deprecated)
│       ├── redeploy_procedures_staging.py   # ← MOVE HERE (deprecated)
│       └── redeploy_validation_prod.py      # ← MOVE HERE (deprecated)
│
├── lambda/                         # Lambda functions (EXISTING)
│   ├── index.js
│   └── snowflake-helpers.js
│
└── sql/                            # SQL (EXISTING - CLEANED UP)
    ├── deploy/                    # NEW orchestration
    │   ├── deploy.py
    │   └── config.yaml
    ├── migrations/                # NEW modular SQL
    │   ├── 001_schema_tables.sql
    │   ├── 002_udfs.sql
    │   └── 006_permissions.sql
    ├── utils/                     # NEW utilities
    │   └── cleanup.py
    └── templates/                 # Legacy templates

```

## Root Directory (Clean & Minimal)

**KEEP IN ROOT:**
- `app.py` - Main entry point
- `config.py` - Core configuration
- `requirements.txt` - Dependencies
- `README.md` - Getting started guide
- `.gitignore` - Git config

**EVERYTHING ELSE MOVES TO:**
- `src/` - Application modules
- `docs/` - All documentation
- `scripts/` - Deployment & setup scripts
- `sql/` - All SQL (already organized)
- `lambda/` - Lambda functions (already good)

## Benefits

1. **Root is clean** - Only 5 essential files
2. **Easy to find things** - Clear folder names
3. **Clear separation** - App code vs scripts vs docs vs SQL
4. **Backwards compatible** - Update imports, nothing breaks

## Implementation Steps

1. ✅ Create new directories
2. ⏳ Move files to new locations
3. ⏳ Update imports in app.py
4. ⏳ Update documentation paths
5. ⏳ Test everything still works
6. ⏳ Update README with new structure
