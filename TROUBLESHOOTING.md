# Troubleshooting Guide

Common issues and solutions for the viewership upload system.

## Table of Contents
- [Git / Deployment Issues](#git--deployment-issues)
- [Date Parsing Issues](#date-parsing-issues)
- [Data Pipeline Issues](#data-pipeline-issues)
- [Recent Fixes (December 2025)](#recent-fixes-december-2025)

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

**Root Causes:**
1. Confusion about which database to check
2. Territory mismatch between data and active_deals configuration

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

### Territory Matching Issues (Dec 8, 2025)

**Specific Problem:** Data has specific territory but active_deals has NULL territory

**Example:**
- Pluto data uploaded with `platform_territory = 'Latin America'`
- active_deals has `platform_territory = NULL`
- Procedure returns: "Successfully set deal_parent for 0 records"

**How NULL Territory Matching Works:**

The procedure uses: `(v.platform_territory IS NULL OR UPPER(v.platform_territory) = UPPER(ad.platform_territory))`

**NULL Behavior:**
- Data NULL + active_deals NULL = MATCH ✓
- Data NULL + active_deals 'Latin America' = MATCH ✓ (data wildcard)
- Data 'Latin America' + active_deals NULL = NO MATCH ✗
- Data 'Latin America' + active_deals 'Latin America' = MATCH ✓

**Key Insight:** NULL territories in active_deals do NOT act as wildcards. Only NULL in DATA acts as a wildcard.

**Solution: Create Territory-Specific active_deals Entries**

1. **Find territories in your data:**
```sql
SELECT DISTINCT platform_territory
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Pluto'
AND filename = 'your-file.csv'
AND deal_parent IS NULL;
```

2. **Find template record:**
```sql
SELECT *
FROM DICTIONARY.PUBLIC.active_deals
WHERE platform = 'Pluto'
AND platform_territory IS NULL
LIMIT 1;
```

3. **Create territory-specific entry:**
```python
from src.snowflake_utils import SnowflakeConnection

conn = SnowflakeConnection()
cursor = conn.cursor

# Get template
cursor.execute('''
SELECT platform, domain, platform_partner_name, platform_channel_name,
       deal_parent, internal_partner, internal_channel, internal_territory,
       internal_channel_id, internal_territory_id, active
FROM DICTIONARY.PUBLIC.active_deals
WHERE platform = 'Pluto' AND platform_territory IS NULL
LIMIT 1
''')
template = cursor.fetchone()

# Insert for specific territory
cursor.execute('''
INSERT INTO DICTIONARY.PUBLIC.active_deals (
    platform, domain, platform_partner_name, platform_channel_name,
    platform_territory, deal_parent, internal_partner, internal_channel,
    internal_territory, internal_channel_id, internal_territory_id, active
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
''', (
    template[0], template[1], template[2], template[3],
    'Latin America',  # The specific territory
    template[4], template[5], template[6], template[7],
    template[8], template[9], template[10]
))

conn.conn.commit()
print('✓ Created active_deals entry for Latin America')
conn.close()
```

**IMPORTANT:** Use `%s` for Snowflake parameter binding, NOT `?` (which is SQLite syntax)

4. **Re-run the procedure:**
```python
conn = SnowflakeConnection()
cursor = conn.cursor
cursor.execute("CALL UPLOAD_DB.PUBLIC.SET_DEAL_PARENT_GENERIC('Pluto', 'your-file.csv')")
result = cursor.fetchone()
print(result[0])  # Should show: "Successfully set deal_parent for N records"
conn.close()
```

**Verification:**
```sql
SELECT COUNT(*) as with_deal_parent
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Pluto'
AND filename = 'your-file.csv'
AND deal_parent IS NOT NULL;
```

**See Also:** `.claude/government/governors/snowflake/README.md` for complete documentation

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

## Template Configuration Issues

### Problem: Multi-Territory Selection Not Working

**Symptoms:**
- Territory multiselect shows "weird caching behavior"
- Selected territories don't persist when switching between create/edit modes
- Can't programmatically set territories from loaded configs

**Root Cause:** Streamlit widget keys conflicting with session_state management

**Fix (Dec 8, 2025):**
- Use separate widget keys (`territories_widget` for create, `territories_widget_edit` for edit)
- Manage `st.session_state.selected_territories` separately from widget key
- Widget reads defaults from session_state and writes back to it

**Files:** `app.py:1274-1289`, `app.py:1456-1472`

**See:** `MULTI_TERRITORY_SUPPORT_2025_12_08.md` for complete details

### Problem: Nested Hardcoded Value SQL Error

**Symptoms:**
```
Using hardcoded value for Channel: '{'hardcoded_value': '{'hardcoded_value': "{'hardcoded_value': 'Nosey'}"}'}'
SQL compilation error: parse error line 2 at position 182 near '39'
```

**Root Cause:** When configs are loaded and resaved, the `hardcoded_value` dict gets wrapped in another dict each time:
- 1st save: `{"hardcoded_value": "Nosey"}`
- 2nd save: `{"hardcoded_value": {"hardcoded_value": "Nosey"}}`
- 3rd save: `{"hardcoded_value": {"hardcoded_value": {"hardcoded_value": "Nosey"}}}`

**Fix (Dec 8, 2025):**
Added recursive unwrapping at `app.py:2652-2654`:
```python
while isinstance(hardcoded_value, dict) and 'hardcoded_value' in hardcoded_value:
    hardcoded_value = hardcoded_value['hardcoded_value']
```

**Prevention:** The fix automatically handles any level of nesting, so configs can be saved/loaded multiple times without issues.

**Verify Fix:**
```bash
# Restart Streamlit to apply the fix
pkill -f streamlit
streamlit run app.py

# Try uploading with the previously failing template
# Should see clean hardcoded values in logs
```

### Problem: Revenue Data with Currency Formatting

**Symptoms:**
```
Error loading data to platform_viewership: Numeric value '$ 0.01' is not recognized
```

**Root Cause:** Revenue columns contain currency formatting (`$ 0.01`, `$ -`, etc.) that Snowflake cannot parse as numeric values.

**Fix (Dec 8, 2025):**

Two-part solution:

1. **Currency Cleaning** (`src/snowflake_utils.py:745-763`)
   - Strips `$`, commas, and spaces from revenue values before insert
   - Converts `-` and empty strings to NULL
   - Validates numeric conversion

2. **Zero-Revenue Filtering** (`app.py:2475-2492`)
   - Filters out records with zero or empty revenue BEFORE loading
   - Records are excluded if revenue is:
     - NULL/NaN
     - Empty string
     - "-"
     - "0"
     - "0.0"
   - Shows message: "Filtered out X zero-revenue records. Loading Y records."

**Impact:** Records with zero revenue (like `$ -`) are now completely excluded from database load, reducing storage and processing overhead.

**Example:**
```
Original file: 1000 records (including 50 with revenue "$ -" or "$0.00")
After filtering: 950 records loaded
Message: "Filtered out 50 zero-revenue records. Loading 950 records."
```

---

## Recent Fixes (December 2025)

### YYYYMMDD Date Format Parsing Issue (Dec 8, 2025)

**Problem:** Dates in YYYYMMDD format (e.g., "20250701") weren't being parsed correctly, resulting in year=1970 in database.

**Symptoms:**
- Query for Q3 2025 returns no results
- Records show year=1970 instead of correct year
- CSV contains dates like "20250701" (July 1, 2025)
- Dates appear as 1970-08-23 (Unix epoch) in database

**Root Cause:**
Two-part issue:
1. Streamlit date parsing didn't explicitly handle 8-digit YYYYMMDD format
2. When parsing failed, Snowflake defaulted to Unix epoch (1970-08-23)
3. SET_DATE_COLUMNS_DYNAMIC extracted year=1970 from these bad dates

**Fix Applied (Dec 8, 2025):**

**Part 1: Enhanced Streamlit Date Parsing** (`src/snowflake_utils.py:716-738`)
```python
# Try YYYYMMDD format first (common in exports)
if len(val) == 8 and val.isdigit():
    parsed_date = pd.to_datetime(val, format='%Y%m%d', errors='coerce')

# If specific format didn't work, use general parsing
if parsed_date is None or pd.isna(parsed_date):
    parsed_date = pd.to_datetime(val)
```

**Part 2: Protected SET_DATE_COLUMNS** (stored procedure lines 18-28)
```javascript
// Only use date column if it has valid data (year >= 2000)
var checkDateQuery = `
    SELECT COUNT(*) as cnt
    FROM TEST_STAGING.public.platform_viewership
    WHERE platform = ''${platform}''
      AND filename = ''${filename}''
      AND date IS NOT NULL
      AND TRIM(date) != ''''
      AND YEAR(TRY_CAST(date AS DATE)) >= 2000  // ← ADDED
`;
```

**Impact:**
- Future uploads with YYYYMMDD dates will parse correctly
- Protection against bad dates overwriting correct year/quarter values
- All existing date formats continue to work

**Files Modified:**
- `src/snowflake_utils.py:716-738` - YYYYMMDD date parsing
- `snowflake/stored_procedures/staging/generic/set_date_columns_dynamic.sql:18-28`
- `snowflake/stored_procedures/production/generic/set_date_columns_dynamic.sql:18-28`

**Verification:**
```python
# Test YYYYMMDD parsing
import pandas as pd
val = "20250701"
if len(val) == 8 and val.isdigit():
    parsed = pd.to_datetime(val, format='%Y%m%d')
    print(parsed)  # 2025-07-01 00:00:00
```

### Multi-Territory Support Implementation (Dec 8, 2025)

**Changes:**
- `COLUMN_MAPPING_CONFIGS.territories` changed from VARCHAR to ARRAY
- UI changed from single-select to multi-select dropdown
- Added 5 new territories: Latin America, Sweden, Norway, Denmark, United Kingdom
- Fixed multiselect caching issue
- Fixed nested hardcoded_value dictionary bug

**Files:** `app.py`, `src/snowflake_utils.py`

**Documentation:** `MULTI_TERRITORY_SUPPORT_2025_12_08.md`

### Issue: Date fields going NULL for monthly/quarterly data

**Symptoms:**
- Tubi VOD or similar monthly aggregated data shows NULL for year, month, quarter
- Data appears correct initially but gets cleared during normalization

**Fixed:** December 2, 2025
- Updated `SET_DATE_COLUMNS_DYNAMIC` to handle NULL date fields
- Now checks if date column has data before trying to derive from it
- For monthly/quarterly data, uses month/year directly instead

**Files:** `snowflake/stored_procedures/generic/set_date_columns_dynamic.sql`

### Issue: asset_series not being set for REF_ID_SERIES bucket

**Symptoms:**
- Records have ref_id and internal_series populated
- REF_ID_SERIES bucket shows 0 updates
- asset_series, content_provider, series_code remain NULL

**Root Cause:** Deployed procedure was missing UNION fallback query

**Fixed:** December 2, 2025
- Redeployed complete `ref_id_series.sql` with UNION fallback
- First tries strict title matching, then falls back to ref_id + series matching
- Handles cases where platform content names don't exactly match metadata titles

**Files:** `snowflake/stored_procedures/-sub-procedures/content_references/generic/ref_id_series.sql`

**See:** `docs/FIXES_2025_12_02.md` for complete details

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

For recent fixes and deployment procedures, see:
- `docs/FIXES_2025_12_02.md` - Latest fixes documentation
- `docs/DEPLOYMENT_GUIDE.md` - How to deploy procedures
