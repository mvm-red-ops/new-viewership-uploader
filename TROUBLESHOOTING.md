# Troubleshooting Guide

This document captures common issues and their solutions to avoid re-explaining the same problems.

## Table of Contents
- [Git / Deployment Issues](#git--deployment-issues)
- [Date Parsing Issues](#date-parsing-issues)
- [Snowflake Stored Procedures](#snowflake-stored-procedures)
- [Data Pipeline Issues](#data-pipeline-issues)

---

## Git / Deployment Issues

### Problem: "Permission denied (publickey)" when pushing to GitHub

**Symptoms:**
```
git@github.com: Permission denied (publickey).
fatal: Could not read from remote repository.
```

**Root Cause:** SSH key not loaded in the SSH agent.

**Solution:**
```bash
# Load the work GitHub SSH key
ssh-add ~/.ssh/work_github

# Verify it's loaded
ssh-add -l

# Then push
git push origin main
```

**Key Location:** `~/.ssh/work_github` (corresponding to tayloryoung@mvmediasales.com)

**Prevention:** Add this to `~/.ssh/config`:
```
Host github.com
    IdentityFile ~/.ssh/work_github
    AddKeysToAgent yes
```

---

## Date Parsing Issues

### Problem: Dates with DD-MM-YYYY format parsing incorrectly as MM-DD-YYYY

**Symptoms:**
- Data for "July to September" shows dates spanning January through December
- Query shows: `2025-01-07` to `2025-12-09` instead of `2025-07-01` to `2025-09-30`
- Dates like `06-07-2025` (July 6) appear as `2025-06-07` (June 7)

**Root Cause:**
Date auto-detection was not committed/deployed to production. Code existed locally but wasn't in git.

**Where the Fix Lives:**

1. **Detection Logic:** `src/transformations.py` - `detect_date_format()` function (lines 187-328)
   - Handles any separator (-, /, space, .)
   - Identifies year position (YYYY-MM-DD, DD-MM-YYYY, MM-DD-YYYY, etc.)
   - Uses two-stage detection:
     * Stage 1: Values > 12 = definite day
     * Stage 2: Repetition analysis (months repeat more than days)

2. **Auto-Application:** `app.py` lines 2636-2649
   - Automatically detects Date column format if no transformation configured
   - Creates parse_date transformation with detected format
   - Shows message: "📅 Auto-detected date format: DD/MM/YYYY"

**How to Verify It's Working:**

```python
import pandas as pd
from src.transformations import detect_date_format

# Test with sample data
df = pd.read_csv('example_data/Philo data July to sep - Philo data July to sep.csv')
date_col = df['watched_at_est']

detected_format = detect_date_format(date_col)
print(f'Detected format: {detected_format}')  # Should be: %d-%m-%Y

# Parse first date
from datetime import datetime
sample_date = date_col.iloc[0]  # e.g., "06-07-2025"
parsed = datetime.strptime(sample_date, detected_format)
print(f'{sample_date} -> {parsed.strftime("%Y-%m-%d")}')  # Should be: 2025-07-06
```

**Check if Deployed:**

```bash
# Check if changes are committed
git log --oneline -1 src/transformations.py
# Should show: "Add robust date format auto-detection for uploads"

# Check if changes are pushed
git log --oneline origin/main -1 -- src/transformations.py

# Verify detect_date_format exists
grep -n "def detect_date_format" src/transformations.py
```

**If Date Detection Isn't Working:**

1. Check git status: `git status src/transformations.py app.py`
2. If uncommitted: Commit and push the changes
3. Restart Streamlit app to pick up changes
4. Re-upload any data with incorrect dates

---

## Snowflake Stored Procedures

### Problem: Schema changes not reflected in EPISODE_DETAILS inserts

**Example:** `full_date` column calculated but not inserted into final table

**Root Cause:** The INSERT statement in `move_data_to_final_table_dynamic_generic` procedure doesn't include the new column.

**Location:** `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` line ~1673

**Fix Process:**

1. **Edit the template:**
   - Add column to INSERT column list (line 1673)
   - Add column to SELECT statement (line 1674)
   - Example: Add `full_date` to both places

2. **Deploy the procedure:**
```bash
cd sql
./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

3. **If deploy fails (permissions), use Python:**
```python
import sys
sys.path.insert(0, '.')
from src.snowflake_utils import SnowflakeConnection

conn = SnowflakeConnection()

# Read generated SQL
with open('sql/generated/prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql', 'r') as f:
    sql = f.read()

# Execute with multi-statement
conn.cursor.execute(sql, num_statements=0)
conn.conn.commit()
print('✓ Deployed successfully')
conn.close()
```

**Verify Deployment:**
```sql
-- Check procedure definition
SHOW PROCEDURES LIKE 'move_data_to_final_table_dynamic_generic';

-- Check if column is included
SELECT GET_DDL('PROCEDURE', 'upload_db_prod.public.move_data_to_final_table_dynamic_generic');
-- Search for the column name in output
```

---

## Data Pipeline Issues

### Problem: Deal parent not being set for new platform

**Symptoms:**
- `deal_parent` column is NULL or empty in `NOSEY_PROD.PUBLIC.platform_viewership`
- Lambda logs show `set_deal_parent_generic` was called
- Records exist in `DICTIONARY.PUBLIC.active_deals` for the platform

**Root Cause:** Confusion about which database to check

**Understanding the Flow:**

```
1. UPLOAD_DB_PROD.public.platform_viewership
   ↓ (initial landing - deal_parent IS NULL here)

2. Data copied to NOSEY_PROD.public.platform_viewership
   ↓ (deal_parent gets SET here by set_deal_parent_generic)

3. Final insert to ASSETS.public.EPISODE_DETAILS
   ↓ (deal_parent carries over from NOSEY_PROD)
```

**Check the Right Place:**

```sql
-- ❌ WRONG - This will always be NULL (landing zone)
SELECT deal_parent FROM UPLOAD_DB_PROD.public.platform_viewership
WHERE platform = 'YourPlatform';

-- ✓ CORRECT - Check here for deal_parent
SELECT deal_parent FROM NOSEY_PROD.public.platform_viewership
WHERE platform = 'YourPlatform';

-- ✓ ALSO CORRECT - Final destination
SELECT deal_parent FROM ASSETS.public.EPISODE_DETAILS
WHERE platform = 'YourPlatform';
```

**Verify Active Deals Configuration:**

```sql
SELECT
    platform,
    domain,
    platform_partner_name,
    platform_channel_name,
    platform_territory,
    deal_parent,
    active
FROM DICTIONARY.PUBLIC.active_deals
WHERE platform = 'YourPlatform'
AND active = true;
```

**Matching Requirements:**
The stored procedure matches on:
- `platform` (exact match)
- `domain` (case-insensitive)
- `platform_partner_name` (NULL or case-insensitive match)
- `platform_channel_name` (NULL or case-insensitive match)
- `platform_territory` (NULL or case-insensitive match)
- `active = true`

If no match found, check that your data values exactly match the active_deals configuration.

---

## Common Workflow Issues

### Uncommitted Changes in Production

**Problem:** Features work locally but not in production

**Check:**
```bash
git status
```

If you see modified files, they're not deployed!

**Fix:**
```bash
# Stage changes
git add <files>

# Commit with descriptive message
git commit -m "Description of changes"

# Load SSH key if needed
ssh-add ~/.ssh/work_github

# Push to production
git push origin main

# Restart Streamlit app if needed
```

---

## Quick Reference

### Essential Git Commands
```bash
# Load work SSH key
ssh-add ~/.ssh/work_github

# Check what's uncommitted
git status

# See uncommitted changes
git diff <filename>

# Check recent commits
git log --oneline -10

# Push to production
git push origin main
```

### Essential SQL Checks
```bash
# Connect and run query
python3 -c "
from src.snowflake_utils import SnowflakeConnection
conn = SnowflakeConnection()
conn.cursor.execute('YOUR SQL HERE')
results = conn.cursor.fetchall()
print(results)
conn.close()
"
```

### Deploy Snowflake Procedures
```bash
cd sql
./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql
```

---

## Getting Help

When asking for help, include:

1. **What you're trying to do**
2. **What's happening** (exact error messages, screenshots)
3. **What you've tried**
4. **Relevant data:**
   - Platform/filename
   - Sample of problematic data
   - Database queries showing the issue

This helps avoid re-explaining context every time.
