# SQL Deployment System

Modular, orchestrated deployment system for the Viewership Upload Pipeline.

## 📁 Directory Structure

```
sql/
├── migrations/          # Modular SQL files (executed in order)
├── deploy/             # Deployment orchestration
├── templates/          # Legacy templates (for reference)
└── utils/              # Helper utilities
```

## 🚀 Quick Start

### Deploy to Staging
```bash
python sql/deploy/deploy.py --env staging
```

### Deploy to Production
```bash
python sql/deploy/deploy.py --env prod
```

### Deploy Only Permissions
```bash
python sql/deploy/deploy.py --env staging --only permissions
```

### Dry Run (see what would be deployed)
```bash
python sql/deploy/deploy.py --env staging --dry-run
```

## 📋 Migrations

Migrations are executed in numerical order:

| File | Purpose | Dependencies |
|------|---------|--------------|
| `001_schema_tables.sql` | Table schema & columns | None |
| `002_udfs.sql` | User-defined functions | 001 |
| `003+` | Stored procedures (uses templates) | 001, 002 |
| `006_permissions.sql` | All GRANT statements | All previous |

## ⚙️ Configuration

Edit `sql/deploy/config.yaml` to:
- Add/modify environments
- Change database names
- Add/remove migrations
- Customize verification queries

### Environment Variables

**Staging:**
- `UPLOAD_DB`: UPLOAD_DB
- `STAGING_DB`: TEST_STAGING
- `ASSETS_DB`: STAGING_ASSETS
- `EPISODE_DETAILS_TABLE`: EPISODE_DETAILS_TEST_STAGING
- `METADATA_DB`: METADATA_MASTER_CLEANED_STAGING

**Production:**
- `UPLOAD_DB`: UPLOAD_DB_PROD
- `STAGING_DB`: NOSEY_PROD
- `ASSETS_DB`: ASSETS
- `EPISODE_DETAILS_TABLE`: EPISODE_DETAILS
- `METADATA_DB`: METADATA_MASTER

## 🛠️ Utilities

### Cleanup Test Data
```bash
python sql/utils/cleanup.py --platform Youtube --filename "test_file.csv" --env staging
```

## 📝 Creating New Migrations

1. Create numbered file in `sql/migrations/`:
   ```sql
   -- 007_new_feature.sql
   CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.PUBLIC.my_new_proc()
   ...
   ```

2. Add to `config.yaml`:
   ```yaml
   migrations:
     - name: "My New Feature"
       file: "007_new_feature.sql"
       required: true
   ```

3. Deploy:
   ```bash
   python sql/deploy/deploy.py --env staging
   ```

## ✅ Verification

Post-deployment verification runs automatically unless `--skip-verify` is used.

Verifications check:
- UDFs exist
- Tables have required columns
- Stored procedures are deployed
- Permissions are granted

## 📚 Best Practices

1. **Always use placeholders**: `{{UPLOAD_DB}}` not `UPLOAD_DB`
2. **Make migrations idempotent**: Use `CREATE OR REPLACE`
3. **Test in staging first**: Never deploy untested changes to prod
4. **Use dry-run**: Preview changes before applying
5. **Document changes**: Add clear comments to migration files

## 🐛 Troubleshooting

### Permission Errors
```bash
# Deploy only permissions
python sql/deploy/deploy.py --env staging --only permissions
```

### Verification Failures
```bash
# Skip verification and fix manually
python sql/deploy/deploy.py --env staging --skip-verify
```

### See What's Different
```bash
# Dry run shows what would be deployed
python sql/deploy/deploy.py --env staging --dry-run
```
