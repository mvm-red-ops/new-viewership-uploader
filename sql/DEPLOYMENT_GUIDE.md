# Environment-Based SQL Deployment Guide

## Overview

This deployment system uses **templates + environment configs** to generate environment-specific SQL files. This means:
- ✅ **No code duplication** - Single source of truth in templates
- ✅ **Easy maintenance** - Update once, deploy to any environment
- ✅ **Clear separation** - Staging and prod database names in config files
- ✅ **Safe deployments** - Review generated SQL before executing

## Directory Structure

```
sql/
├── config/
│   ├── staging.env          # Staging database names
│   └── prod.env             # Production database names
├── templates/               # SQL templates with placeholders
│   ├── DEPLOY_ALL_GENERIC_PROCEDURES.sql
│   ├── DEPLOY_GENERIC_CONTENT_REFERENCES.sql
│   ├── CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
│   ├── create_platform_viewership.sql
│   └── ALTER_ADD_DATE_COLUMNS.sql
├── generated/               # Generated SQL (git-ignored)
│   ├── staging_DEPLOY_ALL_GENERIC_PROCEDURES.sql
│   └── prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql
└── deploy.sh                # Deployment script
```

## Quick Start

### 1. Configure Environment (First Time Only)

Edit the database names in config files if needed:

```bash
# Check staging config
cat sql/config/staging.env

# Check prod config (UPDATE THESE!)
cat sql/config/prod.env
```

**IMPORTANT**: Update `sql/config/prod.env` with your actual production database names!

### 2. Generate SQL for an Environment

```bash
# Generate staging SQL
cd sql
./deploy.sh staging DEPLOY_ALL_GENERIC_PROCEDURES.sql

# Generate production SQL
./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

This creates environment-specific SQL in `sql/generated/`

### 3. Review Generated SQL

```bash
# Review what will be deployed
cat sql/generated/staging_DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

### 4. Deploy to Snowflake

**Option A: Manual (Safest)**
```bash
# Copy/paste from generated file into Snowflake UI
cat sql/generated/staging_DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

**Option B: SnowSQL CLI**
```bash
snowsql -f sql/generated/staging_DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

**Option C: Auto-execute (Use with caution!)**
```bash
./deploy.sh staging DEPLOY_ALL_GENERIC_PROCEDURES.sql --execute
```

## Complete Deployment Steps

### Initial Setup (New Environment)

```bash
cd sql

# 1. Create tables
./deploy.sh staging create_platform_viewership.sql
# Review: cat generated/staging_create_platform_viewership.sql
# Execute in Snowflake

# 2. Add date columns (if tables already exist)
./deploy.sh staging ALTER_ADD_DATE_COLUMNS.sql
# Review and execute

# 3. Deploy content reference sub-procedures
./deploy.sh staging DEPLOY_GENERIC_CONTENT_REFERENCES.sql
# Review and execute

# 4. Deploy main procedures
./deploy.sh staging DEPLOY_ALL_GENERIC_PROCEDURES.sql
# Review and execute

# 5. Deploy validation procedure
./deploy.sh staging CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
# Review and execute
```

### Deploy Updates to Existing Environment

```bash
# Generate the changed file for your environment
./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql

# Review changes
cat generated/prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql

# Execute in Snowflake
snowsql -f generated/prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

## How Templates Work

Templates use placeholders that get replaced based on environment:

```sql
-- Template:
CREATE TABLE {{UPLOAD_DB}}.public.platform_viewership (...)

-- Staging (from staging.env):
CREATE TABLE upload_db.public.platform_viewership (...)

-- Production (from prod.env):
CREATE TABLE upload_db_prod.public.platform_viewership (...)
```

### Available Placeholders

| Placeholder | Staging Value | Production Value | Purpose |
|------------|---------------|------------------|---------|
| `{{UPLOAD_DB}}` | `upload_db` | `upload_db_prod` | Where Streamlit loads data |
| `{{STAGING_DB}}` | `test_staging` | `production_staging` | Where Lambda processes data |
| `{{ASSETS_DB}}` | `staging_assets` | `production_assets` | Final processed data |
| `{{METADATA_DB}}` | `metadata_master_cleaned_staging` | `metadata_master_cleaned_staging` | Metadata reference |

## Creating New Templates

When adding new SQL files:

1. Write SQL with placeholders:
```sql
CREATE PROCEDURE {{UPLOAD_DB}}.public.my_procedure()
...
FROM {{STAGING_DB}}.public.platform_viewership
...
```

2. Save to `sql/templates/my_new_file.sql`

3. Generate for your environment:
```bash
./deploy.sh staging my_new_file.sql
```

## Troubleshooting

### "Config file not found"
Make sure you're in the `sql/` directory when running `deploy.sh`

### "Template file not found"
Check that the file exists in `sql/templates/` directory

### Wrong database names in generated SQL
Update the environment config file (`sql/config/staging.env` or `prod.env`)

### Need to update production database names
Edit `sql/config/prod.env` with the correct production database names

## Best Practices

1. ✅ **Always review generated SQL before executing**
2. ✅ **Test in staging before deploying to prod**
3. ✅ **Keep config files up to date**
4. ✅ **Commit templates to git, not generated files**
5. ✅ **Use version control for config files**
6. ⚠️ **Never commit credentials to config files**

## Deployment Checklist

- [ ] Update prod.env with correct database names
- [ ] Generate SQL for target environment
- [ ] Review generated SQL file
- [ ] Back up production (if applicable)
- [ ] Execute in Snowflake
- [ ] Test procedures work correctly
- [ ] Commit template changes to git
