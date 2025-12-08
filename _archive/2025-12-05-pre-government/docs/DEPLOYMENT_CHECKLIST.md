# Deployment Checklist - Prevent Production Issues

## Problem: Hardcoded Database Names Breaking Production

**What went wrong:**
- Individual procedure files had hardcoded database names like `TEST_STAGING`, `UPLOAD_DB`
- These work in staging but break in production (which uses `NOSEY_PROD`, `UPLOAD_DB_PROD`)
- Procedures were edited directly in Snowflake, bypassing the template system
- The deployment script was not used consistently

## Solution: Always Use the Deployment Script

### 🚨 RULE #1: Never Hardcode Database Names

**WRONG:**
```sql
CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MY_PROCEDURE(...)
AS $$
  UPDATE test_staging.public.platform_viewership
  SET ...
$$;
```

**CORRECT:**
```sql
CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.PUBLIC.MY_PROCEDURE(...)
AS $$
  UPDATE {{STAGING_DB}}.public.platform_viewership
  SET ...
$$;
```

### 🚨 RULE #2: Use the Deployment Script for ALL Changes

```bash
# Deploy to staging FIRST
python3 sql/deploy/deploy.py --env staging

# Test in staging
# ...

# Deploy to production
python3 sql/deploy/deploy.py --env prod
```

### 🚨 RULE #3: Edit Templates, Not Individual Files

**Files that get deployed:**
- ✅ `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` - Main procedures
- ✅ `sql/templates/DEPLOY_GENERIC_CONTENT_REFERENCES.sql` - Content matching procedures
- ✅ `sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql` - Validation
- ✅ `sql/migrations/*.sql` - Schema and UDF migrations

**Files that do NOT get deployed automatically:**
- ❌ `snowflake/stored_procedures/generic/*.sql` - Individual procedure files
- ❌ `snowflake/stored_procedures/-sub-procedures/**/*.sql` - Sub-procedures
- ❌ Direct edits in Snowflake UI

### 🚨 RULE #4: Never Edit Procedures Directly in Snowflake

**Why:**
1. Your changes will be overwritten on next deployment
2. Changes won't propagate between staging and production
3. Template variables won't be replaced
4. No version control / git history

**Instead:**
1. Edit the template file in `sql/templates/`
2. Run the deployment script
3. Test in staging
4. Deploy to production

## Template Variable Reference

| Variable | Staging Value | Production Value |
|----------|---------------|------------------|
| `{{UPLOAD_DB}}` | `UPLOAD_DB` | `UPLOAD_DB_PROD` |
| `{{STAGING_DB}}` | `TEST_STAGING` | `NOSEY_PROD` |
| `{{METADATA_DB}}` | `METADATA_MASTER_CLEANED_STAGING` | `METADATA_MASTER` |
| `{{ASSETS_DB}}` | `STAGING_ASSETS` | `ASSETS` |
| `{{EPISODE_DETAILS_TABLE}}` | `EPISODE_DETAILS_TEST_STAGING` | `EPISODE_DETAILS` |

## Configuration File

All environment settings are in: `sql/deploy/config.yaml`

```yaml
environments:
  staging:
    UPLOAD_DB: "UPLOAD_DB"
    STAGING_DB: "TEST_STAGING"
    ASSETS_DB: "STAGING_ASSETS"
    # ...

  prod:
    UPLOAD_DB: "UPLOAD_DB_PROD"
    STAGING_DB: "NOSEY_PROD"
    ASSETS_DB: "ASSETS"
    # ...
```

## Deployment Order

The script deploys in this order (defined in `config.yaml`):

1. **Schema and Tables** - `migrations/001_schema_tables.sql`
2. **User-Defined Functions** - `migrations/002_udfs.sql`
3. **Stored Procedures (All)** - `templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql`
4. **Content Reference Procedures** - `templates/DEPLOY_GENERIC_CONTENT_REFERENCES.sql`
5. **Validation Procedures** - `templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql`
6. **Permissions and Grants** - `migrations/006_permissions.sql`

## Quick Fix: If You Already Edited a Procedure Directly

1. Get the DDL from Snowflake:
   ```sql
   SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.MY_PROCEDURE(STRING, STRING)');
   ```

2. Replace hardcoded names with templates:
   - `UPLOAD_DB` → `{{UPLOAD_DB}}`
   - `TEST_STAGING` → `{{STAGING_DB}}`
   - `NOSEY_PROD` → `{{STAGING_DB}}`
   - `METADATA_MASTER_CLEANED_STAGING` → `{{METADATA_DB}}`

3. Add the templated procedure to the appropriate template file in `sql/templates/`

4. Run the deployment script:
   ```bash
   python3 sql/deploy/deploy.py --env staging
   python3 sql/deploy/deploy.py --env prod
   ```

## Common Mistakes to Avoid

### ❌ Mistake 1: Deploying Individual Files
```bash
# DON'T DO THIS:
snowsql -f snowflake/stored_procedures/generic/my_proc.sql
```

### ❌ Mistake 2: Hardcoding in JavaScript Strings
```javascript
// WRONG:
const sql = `CALL UPLOAD_DB.PUBLIC.MY_PROC(?, ?)`;

// CORRECT:
const sql = `CALL {{UPLOAD_DB}}.PUBLIC.MY_PROC(?, ?)`;
```

### ❌ Mistake 3: Assuming Staging Procedure Will Work in Prod
```javascript
// This works in staging but breaks in prod:
UPDATE test_staging.public.platform_viewership ...

// This works everywhere:
UPDATE {{STAGING_DB}}.public.platform_viewership ...
```

### ❌ Mistake 4: Skipping the Deployment Script
```bash
# DON'T manually deploy:
python3 deploy_single_proc.py

# USE the official script:
python3 sql/deploy/deploy.py --env prod
```

## How to Add a New Procedure

1. **Create the templated procedure:**
   - Edit `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql`
   - Add your procedure with template variables
   - Use `$$` delimiters for JavaScript body (not single quotes)

2. **Deploy to staging:**
   ```bash
   python3 sql/deploy/deploy.py --env staging
   ```

3. **Test in staging:**
   - Upload a test file
   - Check logs
   - Verify data in tables

4. **Deploy to production:**
   ```bash
   python3 sql/deploy/deploy.py --env prod
   ```

5. **Verify in production:**
   - Test with a real upload
   - Monitor logs

## Troubleshooting

### Issue: "Object does not exist or not authorized"
**Cause:** Procedure wasn't deployed or was deployed to wrong database
**Fix:** Run `python3 sql/deploy/deploy.py --env prod`

### Issue: "JavaScript compilation error"
**Cause:** Hardcoded database name doesn't match environment
**Fix:** Replace hardcoded names with templates, redeploy

### Issue: "Procedure works in staging but not prod"
**Cause:** Template variables weren't used or deployment script wasn't run
**Fix:** Check procedure for hardcoded names, use templates, redeploy

## Resources

- Deployment script: `sql/deploy/deploy.py`
- Configuration: `sql/deploy/config.yaml`
- Templates: `sql/templates/`
- Documentation: `sql/README_DEPLOYMENT.md`
