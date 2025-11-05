# Legacy Deployment Scripts

**⚠️ DEPRECATED** - These scripts are kept for reference only.

## Use New System Instead

```bash
# Old way (deprecated)
python scripts/legacy/redeploy_procedures_staging.py

# New way (recommended)
python sql/deploy/deploy.py --env staging
```

## What's Here

### Deprecated Deployment Scripts
- `redeploy_procedures_prod.py` - Deploy procedures to prod (use `sql/deploy/deploy.py --env prod`)
- `redeploy_procedures_staging.py` - Deploy procedures to staging (use `sql/deploy/deploy.py --env staging`)
- `redeploy_validation_prod.py` - Deploy validation to prod (included in main deployment)

### Setup Scripts
- `setup_database.py` - Initial database setup (manual, run once)

### Shell Scripts
- `deploy_lambda.sh` - Deploy Lambda functions (if exists)
- `deploy_snowflake.sh` - Deploy SQL to Snowflake (if exists)

## Why Deprecated?

These scripts had several problems:
1. **No separation of concerns** - Mixed UDFs, procedures, permissions
2. **Manual coordination** - Had to run multiple scripts in correct order
3. **No verification** - Couldn't tell if deployment succeeded
4. **Environment-specific** - Separate scripts for staging/prod
5. **No dry-run** - Couldn't preview changes before applying

## Migration Path

Replace old deployments:

| Old | New |
|-----|-----|
| `python redeploy_procedures_staging.py` | `python sql/deploy/deploy.py --env staging` |
| `python redeploy_procedures_prod.py` | `python sql/deploy/deploy.py --env prod` |
| `python redeploy_validation_prod.py` | Included in main deployment |
| `python sql/grant_udf_permissions.py` | `python sql/deploy/deploy.py --env staging --only permissions` |

## If You Need These Scripts

If for some reason you need to use the old scripts:

```bash
# From root directory
python scripts/legacy/redeploy_procedures_staging.py
```

**Note:** These scripts may not include recent fixes (UDFs, permissions, unmatched logging).

## New System Benefits

✅ Single command deployment
✅ Automatic verification
✅ Dry-run mode
✅ Modular migrations
✅ Clear separation of concerns
✅ Environment config in YAML
✅ Better error messages

See `sql/deploy/README.md` for full documentation.
