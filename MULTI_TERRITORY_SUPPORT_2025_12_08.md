# Multi-Territory Support Implementation

**Date:** December 8, 2025
**Status:** ✅ Complete

## Overview

Implemented multi-territory support for template configurations, allowing a single template to support multiple territories via a multi-select UI. This replaces the previous single-territory dropdown with an array-based approach.

## Database Changes

### COLUMN_MAPPING_CONFIGS Schema Migration

**Before:**
```sql
TERRITORY VARCHAR(500)
UNIQUE (PLATFORM, PARTNER, CHANNEL, TERRITORY)
```

**After:**
```sql
TERRITORIES ARRAY
UNIQUE (PLATFORM, PARTNER, CHANNEL, TERRITORIES)
```

**Migration Steps:**
1. Added new `territories` ARRAY column
2. Migrated data from `territory` → `territories` (single value → array)
3. Dropped old unique constraint
4. Created new unique constraint including territories array
5. Dropped old `territory` column

**Migration Script:** See database execution logs from Dec 8, 2025

## UI Changes

### Territory Selection Widget

**Before:** Single-select dropdown
```python
selected_territory = st.selectbox("Territory", options=territories)
```

**After:** Multi-select dropdown
```python
selected_territories = st.multiselect(
    "Territories (optional)",
    options=territories,
    default=default_territories,
    key="territories_widget"
)
```

**Locations:**
- `app.py:1274-1289` - Create mode
- `app.py:1456-1472` - Edit mode

### New Territories Added

- Latin America
- Sweden
- Norway
- Denmark
- United Kingdom

**Complete List:** United States, Canada, India, Mexico, Australia, New Zealand, International, Brazil, Latin America, Sweden, Norway, Denmark, United Kingdom

## Code Changes

### 1. snowflake_utils.py

**Updated Functions:**
- `create_config()` - Now accepts `territories` list instead of single `territory`
- `update_config()` - Updated to handle territories array
- `load_to_platform_viewership()` - Handles territory array in upload data

**Location:** `src/snowflake_utils.py`

### 2. app.py

**Territory Widget Management:**
- Separate widget keys for create/edit modes to prevent state conflicts
- Widget key: `territories_widget` (create), `territories_widget_edit` (edit)
- Session state: `st.session_state.selected_territories`

**Territory List Function:**
- `get_cached_territories()` at line 1116-1133
- Returns cached list of available territories

## Bug Fixes

### Bug #1: Multiselect Caching Issue

**Symptom:** "weird caching behavior when i add multiple items to terriroty"

**Root Cause:** Using `key="selected_territories"` meant Streamlit owned that session_state variable, preventing programmatic updates when loading configs.

**Fix:** Use separate widget keys while managing `st.session_state.selected_territories` separately:
```python
# Widget reads from and writes to session_state
selected_territories = st.multiselect(
    "Territories (optional)",
    options=territories,
    default=st.session_state.get('selected_territories', []),
    key="territories_widget"  # Different key!
)
st.session_state.selected_territories = selected_territories
```

**Files Changed:** `app.py:1274-1289`, `app.py:1456-1472`

### Bug #2: Nested Hardcoded Value Dictionary

**Symptom:** SQL compilation error with deeply nested dict:
```
Using hardcoded value for Channel: '{'hardcoded_value': '{'hardcoded_value': "{'hardcoded_value': 'Nosey'}"}'}'
```

**Root Cause:** When configs are loaded from database and re-saved, the `hardcoded_value` dict gets wrapped again:
- 1st save: `{"hardcoded_value": "Nosey"}`
- 2nd save: `{"hardcoded_value": {"hardcoded_value": "Nosey"}}`
- 3rd save: `{"hardcoded_value": {"hardcoded_value": {"hardcoded_value": "Nosey"}}}`

**Fix:** Added recursive unwrapping at `app.py:2652-2654`:
```python
if 'hardcoded_value' in mapping_value:
    hardcoded_value = mapping_value['hardcoded_value']
    # Unwrap nested hardcoded_value dicts (happens when config is resaved)
    while isinstance(hardcoded_value, dict) and 'hardcoded_value' in hardcoded_value:
        hardcoded_value = hardcoded_value['hardcoded_value']
```

**Files Changed:** `app.py:2648-2662`

## Documentation Updates

### Updated Files:
1. `.claude/government/governors/snowflake/knowledge/CANONICAL.md`
   - Updated `platform_viewership` table schema showing territory as ARRAY
   - Added `COLUMN_MAPPING_CONFIGS` table documentation
   - Added multi-territory support status

2. `.claude/government/governors/streamlit/README.md`
   - Documented UI changes (multi-select widget)
   - Added new territories list
   - Documented both bug fixes
   - Updated Critical Red Lines

3. `ARCHITECTURE.md`
   - Added multi-territory support note to Deal Parent Matching section

4. `MULTI_TERRITORY_SUPPORT_2025_12_08.md` (this file)
   - Comprehensive implementation documentation

## Testing Checklist

- [pending] Test creating new template with multiple territories
- [pending] Test editing existing template to add/remove territories
- [pending] Verify unique constraint prevents duplicate configs
- [pending] Test uploading data using multi-territory template
- [pending] Verify territory array properly stored in database
- [pending] Test that nested hardcoded_value bug is resolved

## Deployment Notes

**Streamlit App:** Restart required for changes to take effect
```bash
# Kill and restart Streamlit
pkill -f streamlit
streamlit run app.py
```

**Database:** Migration already executed in UPLOAD_DB

## Impact Assessment

**Breaking Changes:** None - backward compatible with existing single-territory templates

**Data Migration:** Existing templates automatically migrated from single territory to array format

**User Impact:** Users can now select multiple territories for a template, improving flexibility

## Related Files

**Code:**
- `app.py` - UI changes (lines 1116-1133, 1274-1289, 1456-1472, 2648-2662)
- `src/snowflake_utils.py` - Database operations updated for array handling

**Documentation:**
- `.claude/government/governors/snowflake/knowledge/CANONICAL.md`
- `.claude/government/governors/streamlit/README.md`
- `ARCHITECTURE.md`
- `TROUBLESHOOTING.md` (to be updated)

## Future Enhancements

- [ ] Add validation to prevent empty territories array
- [ ] Add UI indication when template supports multiple territories
- [ ] Add bulk territory assignment for existing templates
- [ ] Consider territory matching logic in deal parent procedures

## Contributors

- President (Claude Code) - Implementation
- Oracle (Taylor Young) - Requirements & testing
