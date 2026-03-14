# Viewership Data Tools

This application provides utilities for managing and validating viewership data in Snowflake.

## Applications

### 1. Data Upload Tool (Original)
**Purpose:** Upload and process viewership data files through the full data pipeline.

**Run with:**
```bash
streamlit run app.py
```

**Features:**
- File upload and validation
- Template configuration
- Column mapping
- Data transformations
- Lambda processing integration

### 2. Utilities Launcher (New)
**Purpose:** Access data validation and diagnostic utilities.

**Run with:**
```bash
streamlit run launcher.py
```

**Available Tools:**
- Discrepancy Checker: Compare staging vs final table data

---

## Discrepancy Checker

The Discrepancy Checker helps identify mismatches between the staging and final viewership tables.

### What it Checks

Compares data between:
- **Staging:** `platform_viewership` (where data lands after upload/normalization)
- **Final:** `episode_details` (where data goes after asset matching)

Metrics compared:
- Record counts
- TOT_HOV (Total Hours of Viewership)
- TOT_MOV (Total Minutes of Viewership)

### How to Use

1. Launch the utilities:
   ```bash
   streamlit run launcher.py
   ```

2. Navigate to "Discrepancy Checker" in the sidebar

3. Select filters:
   - **Platform:** Choose specific platform or All
   - **Year:** Filter by year
   - **Quarter:** Filter by quarter
   - **Filename:** (Optional) Check specific file

4. Click "Check for Discrepancies"

5. Review results:
   - Summary metrics showing match rate
   - Table of files with discrepancies
   - Full comparison table (expandable)

6. Download discrepancies as CSV if needed

### When to Use

- After data processing to verify data integrity
- When investigating data loss or duplication issues
- Before generating reports to ensure data accuracy
- When troubleshooting pipeline issues

### Interpreting Results

- **0% difference:** Perfect match (expected)
- **Small % difference (<1%):** May be due to rounding or valid transformations
- **Large % difference (>5%):** Investigate for data loss or processing errors
- **Record count mismatch:** Check if records were filtered during asset matching

### Common Issues

#### Records Missing in Final Table
**Cause:** Asset matching failed (no ref_id, asset_series, or content_provider)

**Solution:**
1. Query staging table: `SELECT * FROM platform_viewership WHERE filename = 'X' AND (ref_id IS NULL OR asset_series IS NULL)`
2. Check asset metadata in `DICTIONARY.PUBLIC.ASSETS` table
3. Update asset mapping or add missing assets

#### TOT_HOV/TOT_MOV Mismatch
**Cause:** Data transformation error or aggregation issue

**Solution:**
1. Compare raw data in staging vs final
2. Check stored procedure logs for errors
3. Verify date columns are set correctly (affects aggregation)

---

## Environment Selection

Both applications automatically detect the environment based on your `config.py` settings:

- **STAGING:** Uses TEST_STAGING and EPISODE_DETAILS_TEST_STAGING
- **PRODUCTION:** Uses NOSEY_PROD and EPISODE_DETAILS

The current environment is displayed in the app header.

---

## Technical Details

### File Structure

```
new-viewership-uploader/
├── app.py                       # Main upload application
├── launcher.py                  # Multi-utility launcher
├── pages/
│   └── discrepancy_checker.py   # Discrepancy checker utility
├── src/
│   ├── snowflake_utils.py       # Snowflake connection & queries
│   ├── column_mapper.py         # Column mapping logic
│   └── transformations.py       # Data transformation functions
└── config.py                    # Environment configuration
```

### Database Tables

#### Staging Tables
- `TEST_STAGING.PUBLIC.platform_viewership` (staging env)
- `NOSEY_PROD.PUBLIC.platform_viewership` (production env)

#### Final Tables
- `STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING` (staging env)
- `STAGING_ASSETS.PUBLIC.EPISODE_DETAILS` (production env)

### Query Filters

The discrepancy checker supports filtering by:
- Platform (e.g., Pluto, Tubi, Wurl)
- Year (e.g., 2024, 2025)
- Quarter (e.g., Q1, Q2, Q3, Q4)
- Filename (exact match)

All filters are optional and can be combined.

---

## Future Enhancements

Potential additional utilities:
- Asset matching diagnostics
- Pipeline phase tracking
- Data quality reports
- Reprocessing manager
