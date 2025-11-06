# Service: Bucket Matching Procedures

## Purpose
Six specialized stored procedures that match viewership records to internal content metadata using different data quality strategies.

## Location
- **Template:** `sql/templates/DEPLOY_GENERIC_CONTENT_REFERENCES.sql`
- **Database:** `UPLOAD_DB` (staging) or `UPLOAD_DB_PROD` (production)
- **All Procedures:** `*.public.process_viewership_*_generic`

## Bucket Strategy Overview

Records cascade through buckets in order of specificity:
1. **FULL_DATA** - Most precise (all fields present)
2. **REF_ID_SERIES** - Has ref_id + series
3. **REF_ID_ONLY** - Has ref_id, no series
4. **SERIES_SEASON_EPISODE** - Has series + ep/season, no ref_id
5. **SERIES_ONLY** - Has series, incomplete ep/season
6. **TITLE_ONLY** - Only has title (last resort)

## Common Function Signature

All bucket procedures share this interface:
```sql
PROCEDURE process_viewership_{BUCKET}_generic(
    platform VARCHAR,
    filename VARCHAR
)
RETURNS STRING
```

## Common Update Pattern

All bucket procedures UPDATE the same fields:
```sql
UPDATE platform_viewership
SET
    series_code = ...,         -- e.g., "CHP"
    content_provider = ...,    -- e.g., "LITTON"
    asset_title = ...,         -- Episode title
    asset_series = ...,        -- Series name
    ref_id = ...               -- Reference ID (e.g., "CHP-101")
WHERE id IN (bucket_ids);
```

**Key:** Setting `content_provider` marks the record as matched.

---

## 1. FULL_DATA Bucket

### Criteria
- Has: `ref_id`, `internal_series`, `episode_number`, `season_number`, `platform_content_name`
- All episode/season fields are numeric

### Matching Logic
```sql
FROM platform_viewership v
JOIN TEMP_${PLATFORM}_FULL_DATA_BUCKET b ON v.id = b.id
JOIN metadata.episode e ON v.ref_id = e.ref_id
JOIN metadata.series s ON e.series_id = s.id
JOIN metadata.metadata m ON e.ref_id = m.ref_id
WHERE
    -- Exact ref_id match
    v.ref_id = e.ref_id
    -- Series name verification
    AND extract_primary_title(s.titles) = v.internal_series
    -- Episode/season number match
    AND CAST(e.episode AS VARCHAR) = v.episode_number
    AND CAST(e.season AS VARCHAR) = v.season_number
    -- Title normalization match
    AND (
        LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9]', '')) =
        LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
        OR
        LOWER(REGEXP_REPLACE(TRIM(m.clean_title), '[^A-Za-z0-9]', '')) =
        LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
    )
    AND lower(s.status) = 'active'
```

### Confidence Level
**Highest** - Multiple verification points reduce false positive risk.

### Typical Match Rate
~10-20% of records (when data quality is high)

---

## 2. REF_ID_SERIES Bucket

### Criteria
- Has: `ref_id`, `internal_series`, `platform_content_name`
- Episode/season numbers not required

### Matching Logic
```sql
FROM platform_viewership v
JOIN metadata.episode e ON v.ref_id = e.ref_id
JOIN metadata.series s ON e.series_id = s.id
JOIN metadata.metadata m ON e.ref_id = m.ref_id
WHERE
    v.ref_id = e.ref_id
    AND LOWER(extract_primary_title(s.titles)) = LOWER(v.internal_series)
    AND [title normalization match]
    AND lower(s.status) = 'active'
```

### Key Difference from FULL_DATA
No episode/season number validation - trusts ref_id + series name + title.

### Confidence Level
**High** - ref_id is authoritative, series name adds verification.

### Typical Match Rate
~5-10% of records

---

## 3. REF_ID_ONLY Bucket

### Criteria
- Has: `ref_id`, `platform_content_name`
- No `internal_series` (or it's wrong)

### Matching Logic
```sql
FROM platform_viewership v
JOIN metadata.episode e ON v.ref_id = e.ref_id
JOIN metadata.series s ON e.series_id = s.id
JOIN metadata.metadata m ON e.ref_id = m.ref_id
WHERE
    v.ref_id = e.ref_id
    AND [title normalization match]
    AND lower(s.status) = 'active'
```

### Use Case
Platform provides ref_id but series name is:
- Missing
- Misspelled
- Different format than internal dictionary

### Confidence Level
**High** - ref_id is authoritative source.

### Typical Match Rate
~5% of records

---

## 4. SERIES_SEASON_EPISODE Bucket

### Criteria
- Has: `internal_series`, `episode_number`, `season_number`
- **No ref_id**
- **Platform_content_name optional** (unique feature!)

### Matching Logic - TWO PASSES

#### Pass 1: Series + Episode + Season (NO TITLE NEEDED)
```sql
FROM platform_viewership v
JOIN metadata.series s ON extract_primary_title(s.titles) = v.internal_series
JOIN metadata.episode e ON s.id = e.series_id
JOIN metadata.metadata m ON e.ref_id = m.ref_id
WHERE
    extract_primary_title(s.titles) = v.internal_series
    AND CAST(e.episode AS VARCHAR) = v.episode_number
    AND CAST(e.season AS VARCHAR) = v.season_number
    AND lower(s.status) = 'active'
```

**Critical:** This pass works even when `platform_content_name IS NULL`!

#### Pass 2: Title Matching Fallback
If Pass 1 fails, tries normalized title matching:
```sql
FROM platform_viewership v
JOIN metadata.metadata m ON [title normalization match]
JOIN metadata.episode e ON m.ref_id = e.ref_id
JOIN metadata.series s ON e.series_id = s.id
WHERE
    v.internal_series IS NOT NULL
    AND [title normalization match]
    AND lower(s.status) = 'active'
```

Pass 2 requires `platform_content_name IS NOT NULL`.

### Confidence Level
- **Pass 1:** High (series + ep/season is specific)
- **Pass 2:** Medium (title matching can be ambiguous)

### Typical Match Rate
~30-50% of records (most common bucket)

### Special Case Handling
Records with NULL titles:
- Enter Pass 1 ✅
- Skip Pass 2 (no error, just no match)
- If Pass 1 succeeds → matched!
- If Pass 1 fails → remains unmatched

---

## 5. SERIES_ONLY Bucket

### Criteria
- Has: `internal_series`, `platform_content_name`
- No ref_id
- Missing or invalid episode/season numbers

### Matching Logic
```sql
FROM platform_viewership v
JOIN metadata.series s ON LOWER(extract_primary_title(s.titles)) = LOWER(v.internal_series)
JOIN metadata.episode e ON s.id = e.series_id
JOIN metadata.metadata m ON e.ref_id = m.ref_id
WHERE
    LOWER(extract_primary_title(s.titles)) = LOWER(v.internal_series)
    AND [title normalization match]
    AND lower(s.status) = 'active'
    -- Series code validation
    AND lower(SPLIT_PART(m.ref_id, '-', 1)) = lower(s.series_code)
```

### Use Case
- Series-level reporting (no episode breakdown)
- Episode/season data unreliable
- Aggregated metrics

### Confidence Level
**Medium** - Series name + title, but no episode verification.

### Typical Match Rate
~10-15% of records

---

## 6. TITLE_ONLY Bucket

### Criteria
- Has: `platform_content_name`
- **No ref_id, no internal_series**
- Last resort!

### Matching Logic
```sql
FROM platform_viewership v
JOIN metadata.metadata m ON [title normalization match]
JOIN metadata.episode e ON m.ref_id = e.ref_id
JOIN metadata.series s ON e.series_id = s.id
WHERE
    (
        LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9]', '')) =
        LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
        OR
        LOWER(REGEXP_REPLACE(TRIM(m.clean_title), '[^A-Za-z0-9]', '')) =
        LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
    )
    AND lower(s.status) = 'active'
```

### Confidence Level
**Low** - Only title matching, high false positive risk.

### Typical Match Rate
~5% of records

### Known Issues
- Can match wrong episode if titles similar
- No series verification
- "Falls back to first available episode" in metadata

### Recommendations
- Flag TITLE_ONLY matches for manual review
- Add confidence score field (future enhancement)
- Consider fuzzy matching thresholds

---

## Title Normalization Strategy

All buckets use consistent normalization:

```sql
-- Remove special characters, spaces, convert to lowercase
LOWER(REGEXP_REPLACE(TRIM(title), '[^A-Za-z0-9]', ''))

-- Example transformations:
-- "The Show: Episode 1" → "theshowepisode1"
-- "Show Name (2023)"    → "showname2023"
-- "Show's Name!"        → "showsname"
```

**Why this works:**
- Ignores punctuation differences
- Case insensitive
- Handles international characters (removed)

**Limitations:**
- "Show One" = "Showone" (loses word boundaries)
- Doesn't handle typos
- No fuzzy matching (exact match after normalization)

---

## Conflict Handling

All bucket procedures insert unmatched records into `flagged_metadata`:

```sql
MERGE INTO upload_db.public.flagged_metadata T
USING (
    SELECT DISTINCT
        v.platform_content_name AS title,
        v.ref_id,
        v.internal_series,
        v.season_number,
        v.episode_number,
        'Platform: ' || v.platform || ', Date: ' || v.month || '/' || year as notes
    FROM platform_viewership v
    JOIN TEMP_${PLATFORM}_UNMATCHED u ON v.id = u.id
    WHERE v.content_provider IS NULL
      AND v.processed IS NULL
      AND [bucket-specific criteria]
) S
ON [match on key fields]
WHEN MATCHED THEN UPDATE SET T.notes = T.notes || '; ' || S.notes
WHEN NOT MATCHED THEN INSERT (...)
```

**Purpose:** Track records that SHOULD match this bucket but didn't.

**Use Cases:**
- Metadata quality issues (missing episodes in dictionary)
- Title mismatches (platform uses different name)
- Data quality problems (invalid ref_id format)

---

## Performance Characteristics

### Join Strategy
Each bucket performs:
1. Bucket table → viewership (already in memory)
2. Viewership → episode/series/metadata (Snowflake-optimized joins)

### Optimization Tips
- Buckets process in parallel within each bucket's UPDATE
- Snowflake auto-optimizes join order
- Temp bucket tables are small (only IDs)

### Execution Time per Bucket
- 100 records: 1-3 seconds
- 1,000 records: 5-15 seconds
- 10,000 records: 30-90 seconds

Varies by:
- Metadata table size
- Join complexity
- Warehouse size

---

## Common Issues & Solutions

### Issue: CAST(e.episode AS VARCHAR) fails
**Cause:** Episode number has non-numeric characters
**Solution:** Pre-filter with `REGEXP_LIKE(episode_number, '^[0-9]+$')`

### Issue: Leading zeros mismatch ("01" vs "1")
**Cause:** String comparison after CAST
**Solution:** Normalize both sides: `LPAD(e.episode, 2, '0') = LPAD(v.episode_number, 2, '0')`
(Not currently implemented - future enhancement)

### Issue: extract_primary_title returns NULL
**Cause:** Series titles format unexpected
**Solution:** Add NULL check or fallback to first title in array

### Issue: Title normalization too aggressive
**Cause:** Removes all punctuation including meaningful separators
**Solution:** Preserve some separators or implement fuzzy matching

---

## Testing Each Bucket

```sql
-- 1. Identify records that match bucket criteria
SELECT
    COUNT(*),
    platform_content_name,
    ref_id,
    internal_series,
    episode_number,
    season_number
FROM NOSEY_PROD.PUBLIC.platform_viewership
WHERE
    platform = 'Philo'
    AND content_provider IS NULL
    AND [bucket-specific criteria]
GROUP BY 2,3,4,5,6
LIMIT 10;

-- 2. Run bucket procedure manually
CALL UPLOAD_DB_PROD.public.process_viewership_series_season_episode_generic(
    'Philo',
    '20251106_103826.csv'
);

-- 3. Check which records matched
SELECT
    platform_content_name,
    content_provider,  -- Should be NOT NULL now
    series_code,
    asset_title,
    asset_series
FROM NOSEY_PROD.PUBLIC.platform_viewership
WHERE
    filename = '20251106_103826.csv'
    AND content_provider IS NOT NULL
ORDER BY id;
```

---

## Maintenance

### Update Metadata Joins
When metadata schema changes, update all bucket procedures:
1. Edit template: `sql/templates/DEPLOY_GENERIC_CONTENT_REFERENCES.sql`
2. Deploy: `python3 sql/deploy/deploy.py --env prod --only "Content Reference"`

### Add New Bucket
To add a new matching strategy:
1. Add bucket name to orchestrator's `bucketOrder` array
2. Add bucket creation SQL in orchestrator
3. Create new `process_viewership_{NEW_BUCKET}_generic` procedure
4. Deploy both orchestrator and content reference procedures

### Monitor Bucket Effectiveness
```sql
-- Track which buckets handle most records
SELECT
    REGEXP_SUBSTR(LOG_MESSAGE, '[A-Z_]+(?=: Successfully updated)') as bucket,
    SUM(CAST(ROWS_AFFECTED AS INT)) as total_matched,
    COUNT(*) as executions
FROM UPLOAD_DB_PROD.PUBLIC.ERROR_LOG_TABLE
WHERE PROCEDURE_NAME LIKE 'process_viewership_%_generic'
  AND STATUS = 'SUCCESS'
  AND LOG_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

## Change History

### 2025-11-06
- SERIES_SEASON_EPISODE Pass 1 now works without `platform_content_name`
- All buckets except SERIES_SEASON_EPISODE require `platform_content_name`

### 2025-11-05
- Renamed all procedures with `_generic` suffix
- Changed to use `{{STAGING_DB}}` placeholder
- Consolidated platform-specific logic into generic procedures

### Earlier
- Original platform-specific procedures (e.g., `process_viewership_wurl_series_only`)
- Separate procedures for each platform × bucket combination
