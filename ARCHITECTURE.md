# Viewership Upload Pipeline - System Architecture

## Overview

A multi-stage data pipeline that processes viewership data from multiple streaming platforms (Philo, WURL, Pluto, YouTube, etc.), normalizes it, matches it against internal content metadata, and loads it into a final analytics table.

## High-Level Data Flow

```
CSV Upload (Streamlit)
    ↓
Lambda (Async Processing Orchestration)
    ↓
Phase 1: Sanitization & Initial Load → platform_viewership table
    ↓
Phase 2: Normalization & Content Matching
    ├─ Set deal_parent (partner normalization)
    ├─ Set internal_series (series matching)
    ├─ Set territory/channel (fallback normalization)
    ├─ Analyze & Match Content (6-bucket cascade)
    └─ Calculate metrics (TOT_MOV, TOT_HOV)
    ↓
Phase 3: Final Load → episode_details table
    ↓
Post-Processing: Mark as processed, cleanup
```

## Environment Configuration

**Staging:**
- Upload DB: `UPLOAD_DB`
- Staging DB: `TEST_STAGING`
- Assets DB: `STAGING_ASSETS`
- Final Table: `STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING`
- Metadata: `METADATA_MASTER_CLEANED_STAGING`

**Production:**
- Upload DB: `UPLOAD_DB_PROD`
- Staging DB: `NOSEY_PROD`
- Assets DB: `ASSETS`
- Final Table: `ASSETS.PUBLIC.EPISODE_DETAILS`
- Metadata: `METADATA_MASTER`

## Key Design Decisions

### 1. Generic Platform Architecture
**Decision:** Use single `platform_viewership` table instead of platform-specific tables.

**Rationale:**
- Reduces procedure duplication (was 6 platforms × 6 buckets = 36 procedures)
- Now: 6 generic procedures work for all platforms
- Easier maintenance and consistent logic

**Implementation:** Filter by `platform` column instead of table name.

### 2. 6-Bucket Cascade Strategy
**Decision:** Process records through increasingly lenient matching buckets.

**Rationale:**
- Maximize match rate while maintaining data quality
- More specific matches (FULL_DATA) are more trustworthy
- Graceful degradation for incomplete data

**Order matters:** FULL_DATA → REF_ID_SERIES → REF_ID_ONLY → SERIES_SEASON_EPISODE → SERIES_ONLY → TITLE_ONLY

### 3. Template-Based Deployment
**Decision:** Use `{{PLACEHOLDER}}` syntax in SQL templates, generate environment-specific files.

**Rationale:**
- Single source of truth for procedures
- Automatic database name substitution
- Reduces environment-specific errors

### 4. Lambda Orchestration
**Decision:** Use AWS Lambda to orchestrate phases instead of Snowflake tasks.

**Rationale:**
- Better error handling and retry logic
- Detailed logging to CloudWatch
- Email notifications on failures
- Can invoke from Streamlit UI

## Critical Gotchas

### 1. TEMP_UNMATCHED Table Lifecycle
- Created at start of orchestrator
- Records deleted as they're matched by each bucket
- Remaining records at end = truly unmatched
- **MUST BE DROPPED** at end to avoid conflicts on next run

### 2. baseConditions Filtering
- Located in orchestrator at line ~1151
- Controls which records enter processing
- **Key change:** Removed `platform_content_name IS NOT NULL` to allow SERIES_SEASON_EPISODE to process records without titles
- Each bucket adds its own additional filters

### 3. Content Matching WITHOUT Titles
- SERIES_SEASON_EPISODE Pass 1 can match on `series + episode + season` alone
- Pass 2 requires title (falls back to title matching)
- Records with NULL `platform_content_name` skip Pass 2 silently

### 4. Database Name Consistency
- Source files in `snowflake/stored_procedures/` use hardcoded names (for reference)
- **Templates in `sql/templates/`** are the source of truth for deployment
- Templates use `{{STAGING_DB}}` which gets replaced during deployment

### 5. Procedure Naming Convention
- Old: `process_viewership_series_season_episode` (platform-specific)
- New: `process_viewership_series_season_episode_generic` (works for all platforms)
- **Critical:** Orchestrator MUST call with `_generic` suffix

## Data Quality Validation

### Phase 2 Validation (Before Content Matching)
- Checks: `deal_parent`, `territory`, `channel` are set
- **Does NOT block** on missing `content_provider` (expected for unmatched records)

### Phase 3 Validation (Before Final Load)
- Checks: `tot_mov`, `tot_hov`, `week`, `day` are set
- Returns JSON with `matched` and `unmatched` counts
- Pipeline continues with matched records even if some unmatched

### Lambda Final Verification
- Counts records in final table vs. expected
- **Includes unmatched records** from `record_reprocessing_batch_logs`
- Formula: `expected = matched_in_final_table + unmatched_in_logs`

## Unmatched Records Flow

1. Records fail all 6 buckets
2. Remain in `TEMP_UNMATCHED` table
3. Orchestrator inserts into `METADATA_MASTER.public.record_reprocessing_batch_logs`
4. Lambda verification includes these in count
5. Human review required for these records

**Table structure:**
- `id`, `title`, `viewership_id`, `filename`, `platform`, `notes`, `created_at`

## Performance Considerations

### Sequential Bucket Processing
- Buckets run sequentially (not parallel)
- Each bucket queries `TEMP_UNMATCHED` + joins viewership table
- DELETE from `TEMP_UNMATCHED` after each successful bucket
- Remaining buckets see fewer records (optimization)

### Cache and Indexes
- Streamlit caches: `get_cached_platforms()` - 5 min TTL
- Snowflake: Relies on automatic optimization
- No custom indexes defined

## Error Handling Strategy

### Procedure-Level
- Try-catch wraps each bucket execution
- Failures logged to `ERROR_LOG_TABLE`
- Non-blocking failures: Continue to next bucket
- Blocking failures: Throw error, halt orchestrator

### Lambda-Level
- Retries: Up to 3 attempts per phase
- Verification checks after each phase
- Email alerts on final failure
- Logs all queries and results to CloudWatch

### Streamlit-Level
- Shows status messages during upload
- Displays error messages from Lambda
- No automatic retry (user must re-upload)

## Monitoring & Observability

### Key Metrics to Watch
1. **Match rate:** % of records matched vs. unmatched
2. **Bucket distribution:** Which buckets are handling most records
3. **Processing time:** Duration per phase
4. **Failure rate:** % of uploads that fail phase verification

### Log Locations
- **Snowflake:** `UPLOAD_DB_PROD.PUBLIC.ERROR_LOG_TABLE`
- **Lambda:** CloudWatch Logs `/aws/lambda/register-start-viewership-data-processing`
- **Streamlit:** Streamlit Cloud logs (if deployed)

### Verification Queries
```sql
-- Check recent uploads
SELECT filename, COUNT(*) as records, MIN(created_at), MAX(created_at)
FROM NOSEY_PROD.PUBLIC.platform_viewership
WHERE created_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY filename;

-- Check match rates
SELECT
    filename,
    SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END) as matched,
    SUM(CASE WHEN content_provider IS NULL THEN 1 ELSE 0 END) as unmatched
FROM NOSEY_PROD.PUBLIC.platform_viewership
GROUP BY filename;

-- Check unmatched records
SELECT *
FROM METADATA_MASTER.PUBLIC.record_reprocessing_batch_logs
WHERE created_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY created_at DESC;
```

## Deployment Process

### 1. Update Templates
Edit: `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` or `DEPLOY_GENERIC_CONTENT_REFERENCES.sql`

### 2. Deploy Using Python
```bash
# Staging
python3 sql/deploy/deploy.py --env staging --skip-verify

# Production (requires elevated permissions)
python3 sql/deploy/deploy.py --env prod --skip-verify

# Deploy specific migration
python3 sql/deploy/deploy.py --env staging --only "Content Reference"
```

### 3. Verify Deployment
```sql
-- Check procedure exists
SHOW PROCEDURES LIKE '%analyze_and_process_viewership_data_generic%';

-- Check procedure definition
SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(VARCHAR, VARCHAR)');
```

## Recent Fixes & Changes

### November 2025
1. **Fixed orchestrator calling wrong procedures:** Added `_generic` suffix to procedure calls
2. **Fixed database name consistency:** Changed to use `{{STAGING_DB}}` placeholder
3. **Allowed SERIES_SEASON_EPISODE to process records without titles:** Removed `platform_content_name IS NOT NULL` from baseConditions, added it to individual buckets that need it

## Future Improvements

### Short-term
1. Add confidence scoring to matches (FULL_DATA = high, TITLE_ONLY = low)
2. Implement fuzzy matching for title-only bucket
3. Add automated reprocessing for unmatched records when metadata updates

### Medium-term
1. Parallelize bucket processing (if Snowflake supports)
2. Add bucket effectiveness metrics/dashboard
3. Implement A/B testing for matching logic improvements

### Long-term
1. Machine learning model for ambiguous matches
2. Automated metadata enrichment pipeline
3. Real-time processing (instead of batch)

## Related Documentation
- [Services Documentation](./docs/SERVICES.md) - Detailed breakdown of each service
- [Bucket Strategy](./docs/BUCKET_STRATEGY.md) - Deep dive into content matching logic
- [Deployment Guide](./docs/DEPLOYMENT.md) - Step-by-step deployment procedures
