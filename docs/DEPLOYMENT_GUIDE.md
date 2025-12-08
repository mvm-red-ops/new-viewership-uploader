# Deployment Guide

## Quick Reference

### Deploy Individual Procedures

The project uses individual deployment scripts for each procedure located in the root directory:

```bash
# Bucket procedures
python3 deploy_ref_id_series_proc.py          # REF_ID_SERIES bucket
python3 deploy_ref_id_only_proc.py             # REF_ID_ONLY bucket

# Normalization procedures
python3 deploy_date_proc.py                    # SET_DATE_COLUMNS_DYNAMIC
python3 deploy_normalize_proc.py               # NORMALIZE_DATA_IN_STAGING
python3 deploy_ref_id_proc.py                  # SET_REF_ID_FROM_PLATFORM_CONTENT_ID

# Data movement procedures
python3 deploy_streamlit_proc.py               # MOVE_STREAMLIT_DATA_TO_STAGING
```

All deployment scripts:
- Use `config.py` to load Snowflake credentials
- Deploy to current environment (check config.py for STAGING vs PROD settings)
- Read SQL from `snowflake/stored_procedures/` directory
- Verify deployment after completion
- Show success/failure status

### Verify Deployment

After deploying, verify the procedure exists:

```bash
python3 << 'EOF'
import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Show procedure
cursor.execute("SHOW PROCEDURES LIKE 'PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC' IN UPLOAD_DB.PUBLIC")
for row in cursor.fetchall():
    print(f"Found: {row[1]}.{row[2]}.{row[3]}")

# Get DDL to verify contents
cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC(VARCHAR, VARCHAR)')")
ddl = cursor.fetchone()[0]
print(f"\nContains UNION: {'Yes' if 'UNION' in ddl else 'No'}")

cursor.close()
conn.close()
EOF
```

## Deployment Architecture

### Environment Configuration

Snowflake connection details are stored in `config.py`:

```python
def load_snowflake_config():
    return {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }
```

### Template Variables

SQL files use template variables that get replaced during deployment:

- `{{UPLOAD_DB}}` → `UPLOAD_DB` (both staging and prod)
- `{{STAGING_DB}}` → `TEST_STAGING` (staging) or `STAGING` (prod)
- `{{ASSETS_DB}}` → `STAGING_ASSETS` (both)
- `{{METADATA_DB}}` → `METADATA_MASTER_CLEANED_STAGING` (both)
- `{{EPISODE_DETAILS_TABLE}}` → `EPISODE_DETAILS_TEST_STAGING` (staging) or `EPISODE_DETAILS` (prod)

### Deployment Script Pattern

Standard deployment script structure:

```python
#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import snowflake.connector
from config import load_snowflake_config

# Read the SQL file
sql_file = Path(__file__).parent / "snowflake" / "stored_procedures" / "..." / "procedure.sql"

with open(sql_file, 'r') as f:
    sql = f.read()

# Connect to Snowflake
print("🔗 Connecting to Snowflake...")
config = load_snowflake_config()
conn = snowflake.connector.connect(**config)
cursor = conn.cursor()
print("✅ Connected successfully\n")

# Deploy procedure
print("📦 Deploying PROCEDURE_NAME...")
for statement in cursor.execute(sql, num_statements=0):
    pass
print("✅ Deployed successfully\n")

# Verify deployment
print("🔍 Verifying deployment...")
cursor.execute("SHOW PROCEDURES LIKE 'PROCEDURE_NAME' IN UPLOAD_DB.PUBLIC")
results = cursor.fetchall()
if results:
    print(f"✅ Found procedure: {results[0][1]}.{results[0][2]}.{results[0][3]}")
else:
    print("❌ Procedure not found!")

cursor.close()
conn.close()
print("\n✅ Done!")
```

## Procedure Locations

```
snowflake/stored_procedures/
├── generic/
│   ├── move_streamlit_data_to_staging.sql
│   ├── normalize_data_in_staging_simple.sql
│   ├── set_date_columns_dynamic.sql
│   ├── set_phase_generic.sql
│   ├── calculate_viewership_metrics.sql
│   ├── set_deal_parent_generic.sql
│   ├── set_channel_generic.sql
│   ├── set_territory_generic.sql
│   └── set_internal_series_generic.sql
│
├── -sub-procedures/
│   └── content_references/
│       └── generic/
│           ├── ref_id_series.sql                  ← REF_ID_SERIES bucket
│           ├── ref_id_only.sql                    ← REF_ID_ONLY bucket
│           ├── full_data.sql                      ← FULL_DATA bucket
│           ├── series_season_episode.sql          ← SERIES_SEASON_EPISODE bucket
│           ├── series_only.sql                    ← SERIES_ONLY bucket
│           └── title_only.sql                     ← TITLE_ONLY bucket
│
└── 3.content_references/
    └── analyze_and_process_viewership_data.sql    ← Main orchestrator
```

## Common Deployment Scenarios

### Scenario 1: Fix a Bucket Procedure

If you need to fix a specific bucket procedure (e.g., ref_id_series):

1. Edit the source file:
   ```
   snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql
   ```

2. Deploy to staging:
   ```bash
   python3 deploy_ref_id_series_proc.py
   ```

3. Test with sample data in TEST_STAGING

4. Deploy to prod (same script, just update config.py)

### Scenario 2: Fix a Normalization Procedure

If you need to fix SET_DATE_COLUMNS_DYNAMIC:

1. Edit the source file:
   ```
   snowflake/stored_procedures/generic/set_date_columns_dynamic.sql
   ```

2. Deploy to staging:
   ```bash
   python3 deploy_date_proc.py
   ```

3. Test by calling NORMALIZE_DATA_IN_STAGING

4. Deploy to prod

### Scenario 3: Update Main Orchestrator

If you need to update ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC:

1. Edit the source file:
   ```
   snowflake/stored_procedures/3.content_references/analyze_and_process_viewership_data.sql
   ```

2. There's likely a deployment script for this (check root directory)

3. Test the full pipeline end-to-end

## Testing After Deployment

### Test Individual Procedure

```bash
python3 << 'EOF'
import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Test specific procedure
cursor.execute("CALL UPLOAD_DB.PUBLIC.SET_DATE_COLUMNS_DYNAMIC('Tubi', 'tubi_vod_july.csv')")
result = cursor.fetchone()
print(f"Result: {result[0]}")

cursor.close()
conn.close()
EOF
```

### Test Full Pipeline

```bash
python3 << 'EOF'
import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

platform = 'Tubi'
filename = 'tubi_vod_july.csv'

# Phase 0 → 1: Normalize
print("Phase 1: NORMALIZE_DATA_IN_STAGING")
cursor.execute(f"CALL UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING('{platform}', '{filename}')")
print(f"  {cursor.fetchone()[0]}")

# Phase 1 → 2: Analyze and process
print("\nPhase 2: ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC")
cursor.execute(f"CALL UPLOAD_DB.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC('{platform}', '{filename}')")
print(f"  {cursor.fetchone()[0]}")

# Check results
cursor.execute(f"""
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN asset_series IS NOT NULL THEN 1 ELSE 0 END) as has_asset_series,
    SUM(CASE WHEN year IS NOT NULL THEN 1 ELSE 0 END) as has_year
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = '{platform}' AND filename = '{filename}'
""")
row = cursor.fetchone()
print(f"\nResults: {row[0]} total, {row[1]} with asset_series, {row[2]} with year")

cursor.close()
conn.close()
EOF
```

## Rollback

To rollback a deployment, you need to:

1. Find the previous version of the SQL file (use git)
2. Deploy the previous version using the deployment script

Example:
```bash
# View previous version
git show HEAD~1:snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql

# If you need to rollback, checkout the previous version
git checkout HEAD~1 -- snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql

# Deploy the old version
python3 deploy_ref_id_series_proc.py

# Then restore to current version if needed
git checkout HEAD -- snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql
```

## Permissions

All procedures are granted to `WEB_APP` role via:

```sql
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.PROCEDURE_NAME(...) TO ROLE WEB_APP;
```

This grant is included at the end of each procedure SQL file.

The migration file `sql/migrations/006_permissions.sql` also contains all grants for reference.

## Best Practices

1. **Always test in staging first** - Never deploy directly to prod without testing

2. **Verify deployment** - Always check that the procedure exists and has expected contents (e.g., UNION keyword)

3. **Document changes** - Add entry to `docs/FIXES_YYYY_MM_DD.md` for significant changes

4. **Check error logs** - After deployment, verify the procedure works by checking error logs:
   ```sql
   SELECT * FROM UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE
   WHERE procedure_name = 'your_procedure_name'
   ORDER BY log_time DESC
   LIMIT 10;
   ```

5. **Use template variables** - Don't hardcode database names, use template variables for portability

6. **Include grants** - Every procedure SQL file should end with appropriate GRANT statement

7. **Test with real data** - Use actual problem data (like Tubi VOD) to verify fixes work end-to-end
