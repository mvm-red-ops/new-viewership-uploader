# Snowflake Governor

## Domain

The Snowflake Governor owns all aspects of the Snowflake data warehouse:
- Stored procedures (JavaScript and SQL)
- Database schema and tables
- Data pipeline orchestration
- Bucket matching procedures
- Normalization procedures
- Data movement procedures

## Status

**AWAITING CONSTITUTIONAL CONVENTION**

## Delegates

### Phase 1: Data Movement & Normalization
- **MOVE_STREAMLIT_DATA_TO_STAGING Delegate** - Copies data from UPLOAD_DB to TEST_STAGING
- **NORMALIZE_DATA_IN_STAGING Delegate** - Phase 1 normalization orchestrator
- **SET_DATE_COLUMNS Delegate** - Date field calculations
- **SET_DEAL_PARENT Delegate** - Deal parent matching
- **SET_REF_ID Delegate** - Reference ID matching from platform content ID
- **CALCULATE_METRICS Delegate** - Viewership metrics calculations
- **SET_INTERNAL_SERIES Delegate** - Internal series matching

### Phase 2: Content Matching
- **ANALYZE_AND_PROCESS Delegate** - Main orchestrator for bucket routing
- **FULL_DATA Bucket Delegate** - ref_id + series + episode + season
- **REF_ID_SERIES Bucket Delegate** - ref_id + series only
- **REF_ID_ONLY Bucket Delegate** - ref_id only
- **SERIES_SEASON_EPISODE Bucket Delegate** - series + episode + season
- **SERIES_ONLY Bucket Delegate** - series only
- **TITLE_ONLY Bucket Delegate** - title matching only

### Phase 3: Validation & Final Insert
- **HANDLE_FINAL_INSERT Delegate** - Validation and insert to EPISODE_DETAILS

## Critical Red Lines

See `.claude/government/safety-council/veto-rules.md` for complete list.

**Key Red Lines:**
1. ❌ NEVER remove `processed IS NULL OR processed = FALSE` filter in MOVE_STREAMLIT_DATA_TO_STAGING
2. ❌ NEVER remove UNION fallback patterns from bucket procedures
3. ❌ NEVER replace template variables with hardcoded database names
4. ❌ NEVER modify phase transition logic without full testing
5. ❌ NEVER remove GRANT statements from procedures

## Communication Protocol

```
Snowflake Delegates → Snowflake Governor: Always allowed
Snowflake Governor → President: Always allowed
Snowflake Delegate → President: Requires Snowflake Governor approval OR 2+ delegate consensus
```

## Decision Authority

**Snowflake Governor CAN approve:**
- Changes to stored procedure logic within established patterns
- New columns in existing tables (with Testing Governor verification)
- Bug fixes to bucket matching logic
- Performance optimizations
- Documentation updates

**Snowflake Governor CANNOT approve alone:**
- Changes affecting Lambda orchestration (requires Lambda Governor)
- Changes affecting Streamlit data upload (requires Streamlit Governor)
- Breaking changes to table schema (requires all Governors)
- Removal of Safety Council red line protections (requires Citizen)

## Constitutional Convention Questions

### General Architecture
- [ ] What are the absolute no-go zones in stored procedures?
- [ ] Which procedures are most critical and need extra protection?
- [ ] What dependencies exist between procedures that we must preserve?
- [ ] What is the correct order of procedure execution?
- [ ] What happens if a procedure fails mid-pipeline?

### Data Pipeline
- [ ] Can we modify the phase progression (0→1→2→3)?
- [ ] What filters are absolutely critical and must never be removed?
- [ ] When is it safe to reprocess data vs. when must we start fresh?
- [ ] What are the rollback procedures for each phase?

### Bucket Matching
- [ ] When is it acceptable to modify bucket matching criteria?
- [ ] Are UNION fallback patterns required in all bucket procedures?
- [ ] What testing is required before deploying bucket procedure changes?
- [ ] How do we handle cases where no bucket matches?

### Date Handling
- [ ] What date formats must be supported?
- [ ] How should NULL dates be handled?
- [ ] What is the precedence when both `date` and `month/year` are populated?
- [ ] How should quarterly data be handled?

### Template Variables & Environments
- [ ] When can template variables be replaced with hardcoded values (if ever)?
- [ ] What are the differences between STAGING and PROD environments?
- [ ] How do we test changes in staging before promoting to prod?
- [ ] What is the deployment verification process?

### Error Handling
- [ ] What level of logging is required in stored procedures?
- [ ] When should procedures fail vs. continue with partial success?
- [ ] How are errors communicated to Lambda/Streamlit?
- [ ] What triggers an automatic rollback?

### Performance & Optimization
- [ ] When can we modify query patterns for performance?
- [ ] Are there any optimizations that are off-limits?
- [ ] What are acceptable query execution times?
- [ ] When should we use TEMPORARY tables vs. permanent tables?

## Recent Updates

### Revenue Data Handling (Dec 8, 2025)

**Database Write Delegate Enhancement:**

The `load_to_platform_viewership` function in `src/snowflake_utils.py` now includes automatic currency cleaning for REVENUE columns (lines 745-763):

**Currency Cleaning Logic:**
- Strips dollar signs ($), commas, and spaces from revenue values
- Converts special cases ("-", empty strings) to NULL
- Validates numeric conversion before insert
- Prevents "Numeric value '$ 0.01' is not recognized" errors

**Pre-Database Filtering:**
- Zero-revenue records (NULL, empty, "-", "0", "0.0") are filtered in Streamlit BEFORE reaching Snowflake
- This reduces unnecessary data storage and processing overhead
- Only non-zero revenue records are written to `platform_viewership` table

**Impact on Stored Procedures:**
- No changes required to existing stored procedures
- REVENUE column receives clean numeric values or NULL
- Downstream procedures (NORMALIZE_DATA_IN_STAGING, etc.) work unchanged

**Files Modified:**
- `src/snowflake_utils.py:745-763` - Currency cleaning logic
- See Streamlit Governor README for pre-filter logic

### Deal Parent Matching and Territory Handling (Dec 8, 2025)

**Issue:** Records not getting `deal_parent` set when data has specific territory but `active_deals` has NULL territory.

**Root Cause Analysis:**

The `SET_DEAL_PARENT_GENERIC` procedure uses this matching logic:
```sql
AND (v.platform_territory IS NULL OR UPPER(v.platform_territory) = UPPER(ad.platform_territory))
```

**Key Insight:** NULL territories in `active_deals` do NOT act as wildcards for data with specific territories.

**How NULL Matching Works:**
- Data has NULL territory + active_deals has NULL territory = MATCH ✓
- Data has NULL territory + active_deals has specific territory = MATCH ✓ (data wildcard)
- Data has specific territory + active_deals has NULL territory = NO MATCH ✗
- Data has specific territory + active_deals has matching territory = MATCH ✓

**Example Case:**
- Pluto data uploaded with `platform_territory = 'Latin America'`
- active_deals only had entries with `platform_territory = NULL`
- Procedure returned: "Successfully set deal_parent for 0 records"
- **Solution:** Created active_deals entry for Pluto with `platform_territory = 'Latin America'`
- Result: All 120 records matched successfully

**How to Fix Similar Issues:**

1. **Identify Missing Territories:**
```sql
SELECT DISTINCT platform_territory
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'YourPlatform' AND deal_parent IS NULL
```

2. **Find Template in active_deals:**
```sql
SELECT * FROM DICTIONARY.PUBLIC.active_deals
WHERE platform = 'YourPlatform' AND platform_territory IS NULL
```

3. **Create Territory-Specific Entries:**
```python
# Use template values, only change platform_territory
INSERT INTO DICTIONARY.PUBLIC.active_deals (
    platform, domain, platform_partner_name, platform_channel_name,
    platform_territory, deal_parent, internal_partner, internal_channel,
    internal_territory, internal_channel_id, internal_territory_id, active
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# Note: Use %s for Snowflake parameter binding, NOT ?
```

4. **Re-run Procedure:**
```sql
CALL UPLOAD_DB.PUBLIC.SET_DEAL_PARENT_GENERIC('YourPlatform', 'filename.csv')
```

**Files Involved:**
- `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` - SET_DEAL_PARENT_GENERIC procedure (lines ~40-44)
- `DICTIONARY.PUBLIC.active_deals` - Configuration table

## Delegate Knowledge Base

Each delegate will maintain:
- **Procedure source file location**
- **Deployment script**
- **Critical code sections that must not be changed**
- **Dependencies (what other procedures/tables this affects)**
- **Common issues and solutions**
- **Testing checklist**

## Next Steps

- [ ] Hold Constitutional Convention with Citizen
- [ ] Document answers to all questions above
- [ ] Build detailed knowledge bases for each delegate
- [ ] Establish escalation procedures
- [ ] Create testing requirements checklist
- [ ] Document rollback procedures
