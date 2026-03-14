# Project Memory

## Architecture Overview
See memory/architecture.md for full details.

**Key databases:**
- `UPLOAD_DB` / `UPLOAD_DB_PROD` — Streamlit writes here first
- `TEST_STAGING` / `NOSEY_PROD` — Normalization/matching happens here
- `STAGING_ASSETS` / `ASSETS` — Final "analytics" EPISODE_DETAILS tables
- `DICTIONARY` — Templates, active_deals config
- `METADATA_MASTER_CLEANED_STAGING` — Metadata for asset matching

**Pipeline phases:** 0 → 1 → 2 → Final insert

## Critical Red Lines
- NEVER remove `processed IS NULL OR processed = FALSE` filter in MOVE_STREAMLIT_DATA_TO_STAGING
- NEVER remove UNION fallback from bucket procedures
- NEVER hardcode database names (use template variables)

## Governance
- `.claude/government/governors/snowflake/` — Snowflake Governor docs
- Constitution: `.claude/government/governors/snowflake/CONSTITUTION.md`
- Canonical KB: `.claude/government/governors/snowflake/knowledge/CANONICAL.md`

## Key Files
- `lambda/snowflake-helpers.js` — Lambda orchestration (verifyPhase, moveToFinalTable, etc.)
- `snowflake/stored_procedures/staging/` — UPLOAD_DB procedures
- `snowflake/stored_procedures/production/` — UPLOAD_DB_PROD procedures
- `handle_final_insert_dynamic_generic.sql` — ONLY in archive (not in staging/production dirs!)
