# SQL Directory

Organized SQL deployment system for the viewership uploader.

## Quick Start

### Deploy to Staging
```bash
python sql/deploy/deploy.py --env staging
```

### Deploy to Production
```bash
python sql/deploy/deploy.py --env prod
```

### Run Diagnostics
```bash
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"
```

## Directory Structure

```
sql/
├── deploy/                      # Orchestrated deployment system
│   ├── deploy.py                   # Main deployment script
│   ├── config.yaml                 # Environment configuration
│   └── README.md                   # Deployment documentation
│
├── migrations/                  # Modular SQL files
│   ├── 001_schema_tables.sql       # Table schemas and columns
│   ├── 002_udfs.sql                # User-defined functions
│   ├── 003_procedures_phase0.sql   # Phase 0 procedures
│   └── 006_permissions.sql         # Permission grants
│
├── templates/                   # Stored procedure templates
│   ├── DEPLOY_ALL_GENERIC_PROCEDURES.sql
│   ├── DEPLOY_GENERIC_CONTENT_REFERENCES.sql
│   └── CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
│
├── diagnostics/                 # Diagnostic tools
│   ├── diagnose.py                 # Consolidated diagnostic CLI
│   ├── checks/                     # Modular check system
│   │   ├── udf_checks.py
│   │   ├── schema_checks.py
│   │   ├── data_checks.py
│   │   └── asset_matching_checks.py
│   └── README.md                   # Diagnostic documentation
│
├── archive/                     # Historical scripts
│   ├── setup/                      # Initial setup (run once)
│   ├── fixes/                      # One-off fixes
│   ├── deprecated/                 # Replaced by new system
│   └── README.md                   # Archive documentation
│
└── utils/                       # Utility scripts
    └── cleanup.py                  # Data cleanup utility
```

## Common Tasks

### Deploy All Changes
```bash
python sql/deploy/deploy.py --env staging
```

### Deploy Specific Component
```bash
python sql/deploy/deploy.py --env staging --only 002_udfs
python sql/deploy/deploy.py --env staging --only 006_permissions
```

### Verify Deployment
```bash
python sql/diagnostics/diagnose.py --env staging --check udfs
python sql/diagnostics/diagnose.py --env staging --check schema
```

### Debug Failed Upload
```bash
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"
```

### Clean Up Test Data
```bash
python sql/utils/cleanup.py --platform Youtube --filename "test.csv"
```

### Dry Run (Preview Changes)
```bash
python sql/deploy/deploy.py --env staging --dry-run
```

## Detailed Documentation

- **Deployment:** See `deploy/README.md`
- **Diagnostics:** See `diagnostics/README.md`
- **Archive:** See `archive/README.md`

## Migration from Old System

### Old deployment scripts → New system

| Old | New |
|-----|-----|
| `redeploy_procedures_prod.py` | `python sql/deploy/deploy.py --env prod` |
| `grant_udf_permissions.py` | `python sql/deploy/deploy.py --env staging --only 006_permissions` |
| `check_udf_exists.py` | `python sql/diagnostics/diagnose.py --env staging --check udfs` |
| `check_phase3_nulls.py` | `python sql/diagnostics/diagnose.py --env staging --check data-flow` |

Old scripts are archived in `sql/archive/` for reference.

## Environment Configuration

Environments are configured in `deploy/config.yaml`:

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

## Data Flow Pipeline

The viewership pipeline has 4 steps:

### Phase 0: Upload
- Records uploaded to `platform_viewership` table

### Step 1: Deal Matching
- Matches platform/partner/channel/territory → `active_deals` table
- Sets: `deal_parent`, normalized `channel`, normalized `territory`

### Step 2: Internal Series Matching
- Matches `platform_series` → `internal_series_dictionary`
- Sets: `internal_series`

### Step 3: Asset Matching
- 6 strategies (FULL_DATA, REF_ID_SERIES, REF_ID_ONLY, SERIES_SEASON_EPISODE, SERIES_ONLY, TITLE_ONLY)
- Sets: `ref_id`, `asset_series`, `content_provider` (auto-derived from full_data)

### Phase 3: INSERT to EPISODE_DETAILS
- Records with all required fields inserted
- Unmatched records logged to `record_reprocessing_batch_logs`

See `docs/ASSET_MATCHING_ARCHITECTURE.md` for detailed architecture documentation.

## Troubleshooting

### ❌ Unknown UDF EXTRACT_PRIMARY_TITLE
```bash
python sql/deploy/deploy.py --env staging --only 002_udfs
```

### ❌ Invalid identifier PLATFORM_PARTNER_NAME
```bash
python sql/deploy/deploy.py --env staging --only 001_schema_tables
```

### ❌ Insufficient privileges
```bash
python sql/deploy/deploy.py --env staging --only 006_permissions
```

### ❌ 0 records matched to assets
```bash
python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"
```

## Benefits of New System

✅ **Single-command deployment** - One command for all environments
✅ **Modular migrations** - Each component can be deployed independently
✅ **Automatic verification** - Checks deployment success
✅ **Dry-run mode** - Preview changes before applying
✅ **Consolidated diagnostics** - One tool for all checks
✅ **Environment-aware** - Configuration in YAML
✅ **Clear documentation** - Each directory has README

## Questions?

- General overview: This README
- Deployment details: `deploy/README.md`
- Diagnostic tools: `diagnostics/README.md`
- Archived scripts: `archive/README.md`
- Architecture: `docs/ASSET_MATCHING_ARCHITECTURE.md`
