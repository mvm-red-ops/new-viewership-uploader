# Asset Matching Architecture - Modular Design

## The Problem

Current `analyze_and_process_viewership_data_generic` is a black box:
- ❌ Mixes multiple concerns (deal matching, asset matching, content provider lookup)
- ❌ Hard to diagnose which step failed
- ❌ Confusing naming (people mix up deal_parent vs content_provider)
- ❌ Can't test/deploy individual strategies
- ❌ No clear data flow visualization

## Clear Data Flow

```
Phase 2: Content References
│
├─ STEP 1: Deal Matching
│   ├─ Input:  platform, partner, channel, territory (from upload)
│   ├─ Process: set_deal_parent_generic()
│   ├─ Output: deal_parent (id), channel (normalized), territory (normalized)
│   └─ Fallbacks: set_channel_generic(), set_territory_generic()
│
├─ STEP 2: Internal Series Matching
│   ├─ Input:  platform_series (from upload)
│   ├─ Process: set_internal_series_generic() OR SET_INTERNAL_SERIES_WITH_EXTRACTION()
│   ├─ Output: internal_series (our internal name)
│   └─ Note: Uses internal_series_dictionary table
│
├─ STEP 3: Asset Matching (Multi-Strategy)
│   ├─ Input:  platform_content_name, internal_series, episode/season numbers
│   ├─ Process: analyze_and_process_viewership_data_generic()
│   │   ├─ Strategy 1: FULL_DATA (exact match: ref_id + episode + season + title)
│   │   ├─ Strategy 2: REF_ID_SERIES (match: ref_id + series)
│   │   ├─ Strategy 3: REF_ID_ONLY (match: ref_id alone)
│   │   ├─ Strategy 4: SERIES_SEASON_EPISODE (match: series + season + episode)
│   │   ├─ Strategy 5: SERIES_ONLY (match: series name)
│   │   └─ Strategy 6: TITLE_ONLY (fuzzy match: title)
│   ├─ Output: ref_id, asset_series, asset_title, content_provider
│   └─ Unmatched: Logged to record_reprocessing_batch_logs
│
└─ STEP 4: Content Provider Lookup (Automatic)
    ├─ Input:  ref_id (from Step 3)
    ├─ Process: JOIN to full_data table during asset matching
    ├─ Output: content_provider (e.g., "Nosey", "FilmRise")
    └─ Note: Happens WITHIN asset matching, not separate step
```

## Critical Distinction

### set_deal_parent (STEP 1)
**What it does:** Matches upload metadata → active_deals table
**Input:** `platform`, `partner`, `channel`, `territory` (from CSV)
**Output:** `deal_parent` (integer ID), normalized `channel`, normalized `territory`
**Source table:** `active_deals`
**Purpose:** Business context - which deal/partner/channel is this?

### Asset Matching (STEP 3)
**What it does:** Matches content titles → our content catalog
**Input:** `platform_content_name`, `internal_series`, episode/season numbers
**Output:** `ref_id`, `asset_series`, `asset_title`, `content_provider`
**Source table:** `full_data` (our content catalog)
**Purpose:** What specific episode/movie is this?

### content_provider (Derived in STEP 3)
**What it is:** Owner of the content (e.g., "Nosey", "FilmRise", "Kino Lorber")
**How it's set:** Automatically looked up from `full_data` table during asset matching
**NOT a separate step:** It's a column that comes with `ref_id`
**Common confusion:** People think there's a `set_content_provider()` procedure. There isn't - it's joined from `full_data`

## Modular Architecture

### Each Strategy = Independent Module

```
sql/
└── templates/
    └── procedures/
        ├── asset_matching/
        │   ├── 01_analyze_orchestrator.sql       # Main orchestrator
        │   ├── 02_strategy_full_data.sql         # FULL_DATA bucket
        │   ├── 03_strategy_ref_id_series.sql     # REF_ID_SERIES bucket
        │   ├── 04_strategy_ref_id_only.sql       # REF_ID_ONLY bucket
        │   ├── 05_strategy_series_se.sql         # SERIES_SEASON_EPISODE bucket
        │   ├── 06_strategy_series_only.sql       # SERIES_ONLY bucket
        │   ├── 07_strategy_title_only.sql        # TITLE_ONLY bucket
        │   └── README.md                          # Strategy documentation
        │
        ├── deal_matching/
        │   ├── set_deal_parent.sql               # Primary deal matching
        │   ├── set_channel.sql                   # Fallback channel normalization
        │   ├── set_territory.sql                 # Fallback territory normalization
        │   └── README.md
        │
        └── series_matching/
            ├── set_internal_series.sql           # Dictionary lookup
            ├── set_internal_series_extraction.sql # With extraction logic
            └── README.md
```

## Diagnostic Flow

### When Asset Matching Fails

```bash
# 1. Check which phase/step failed
python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"

# Output shows:
# ✅ Phase 0: Upload complete (80,686 records)
# ✅ Step 1: Deal parent matched (80,686 records have deal_parent)
# ✅ Step 2: Internal series matched (60,000 records have internal_series)
# ⚠️  Step 3: Asset matching incomplete (7,015 matched, 73,671 unmatched)
#     ├─ FULL_DATA: 0 matches
#     ├─ REF_ID_SERIES: 0 matches
#     ├─ REF_ID_ONLY: 0 matches
#     ├─ SERIES_SEASON_EPISODE: 5,000 matches
#     ├─ SERIES_ONLY: 2,000 matches
#     └─ TITLE_ONLY: 15 matches
#
#     💡 DIAGNOSIS: Most records missing ref_id. Check if:
#        - platform_content_id is populated
#        - full_data table has matching content_ids for this platform

# 2. Drill into specific strategy
python sql/diagnostics/diagnose.py --env staging --check asset-matching --strategy TITLE_ONLY

# Output shows:
# TITLE_ONLY Strategy Analysis:
# ├─ Bucket size: 501 records
# ├─ Matches: 15 records (3%)
# ├─ Sample unmatched titles:
# │  - "The Real Housewives of Atlanta - S01E01"
# │  - "Love & Hip Hop - S02E03"
# │
# ├─ Common failure reasons:
# │  ✓ EXTRACT_PRIMARY_TITLE UDF working
# │  ✗ Titles don't match full_data (fuzzy threshold too high?)
# │  ✗ internal_series not set (prerequisite for TITLE_ONLY)
# │
# └─ 💡 SUGGESTED FIX:
#    Edit: sql/templates/procedures/asset_matching/07_strategy_title_only.sql
#    Line 45: Change similarity threshold from 0.8 to 0.7
```

## Clear Failure Points & Fixes

### Failure: "No deal_parent"
**What failed:** Step 1 - Deal Matching
**File to edit:** `sql/templates/procedures/deal_matching/set_deal_parent.sql`
**Common causes:**
- New partner not in `active_deals` table
- Channel/territory name doesn't match active_deals
**Fix:** Add new entry to `active_deals` OR adjust matching logic

### Failure: "No internal_series"
**What failed:** Step 2 - Internal Series Matching
**File to edit:** `sql/templates/procedures/series_matching/set_internal_series.sql`
**Common causes:**
- New series not in `internal_series_dictionary`
- `platform_series` name doesn't match dictionary
**Fix:** Add new entry to dictionary OR adjust fuzzy matching

### Failure: "No ref_id/asset_series" (most records)
**What failed:** Step 3 - Asset Matching
**Which strategy failed:** Run diagnostics to see which buckets matched
**Files to edit:**
- All strategies failing → Data issue (platform_content_id missing, full_data incomplete)
- Specific strategy failing → `sql/templates/procedures/asset_matching/0X_strategy_NAME.sql`
**Common causes:**
- Platform content IDs not mapped to our ref_ids
- Fuzzy matching thresholds too strict
- Missing data in full_data table
**Fix:** Adjust strategy SQL OR populate missing data

### Failure: "No content_provider"
**This shouldn't happen** - content_provider is automatically set when ref_id is set
**If it's NULL:** The ref_id in full_data has NULL content_provider (data quality issue)
**Fix:** Update full_data table to populate content_provider for those ref_ids

## Testing Individual Strategies

```sql
-- Test TITLE_ONLY strategy in isolation
CALL analyze_and_process_viewership_data_generic('Youtube', 'test_file.csv');

-- Check only TITLE_ONLY results
SELECT
    platform_content_name,
    asset_title,
    ref_id,
    content_provider
FROM test_staging.public.platform_viewership
WHERE platform = 'Youtube'
AND filename = 'test_file.csv'
AND ref_id IS NOT NULL  -- Successfully matched
-- Compare to unmatched to see what didn't work
```

## Deployment

### Deploy All Strategies
```bash
python sql/deploy/deploy.py --env staging
```

### Deploy Single Strategy (Future)
```bash
# After modularization
python sql/deploy/deploy.py --env staging --only asset-matching-title-only
```

## Documentation for Each Strategy

Each strategy file should have:
```sql
-- ==============================================================================
-- Strategy: TITLE_ONLY
-- ==============================================================================
-- Purpose: Fuzzy match by content title when no ref_id or series match exists
--
-- Prerequisites:
--   - internal_series must be set (uses series context for matching)
--   - EXTRACT_PRIMARY_TITLE UDF must exist
--
-- Input Requirements:
--   - platform_content_name IS NOT NULL
--   - internal_series IS NOT NULL
--
-- Matching Logic:
--   1. Extract primary title from platform_content_name using UDF
--   2. Fuzzy match against full_data.asset_title
--   3. Filter by internal_series if available (improves accuracy)
--   4. Threshold: 0.8 similarity (adjust if too strict)
--
-- Output:
--   - ref_id, asset_series, asset_title, content_provider
--
-- Common Failures:
--   - internal_series not set → No context for matching
--   - Threshold too high → Too few matches
--   - Threshold too low → False positives
--
-- To Adjust:
--   - Line 45: Change SIMILAR threshold
--   - Line 67: Modify title normalization logic
-- ==============================================================================
```

## Next Steps

1. ⏳ Split monolithic procedure into modular files
2. ⏳ Add clear documentation to each strategy
3. ⏳ Build diagnostic tool with strategy-specific checks
4. ⏳ Create test harness for individual strategies
5. ⏳ Update deployment system to support modular deployment
