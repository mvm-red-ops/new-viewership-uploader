# Archive: Pre-Staging/Production Split (2025-12-05)

This directory contains the entire old architecture that existed before the staging/production separation was implemented.

## What Was Archived

All stored procedures and supporting code from before the `staging/` and `production/` directory split:

### Deprecated Architecture Directories

**`-sub-procedures/`** - Platform-specific sub-procedures
- `content_references/` - Old content reference matching (platform-specific + early generic)
- `migrate/` - Old data migration procedures
- `normalize/amagi/`, `normalize/pluto/`, `normalize/wurl/` - Platform-specific normalization
- `sanitize/` - Old sanitization logic

**`1.directory/`** - Old numbered directory structure
- `amagi.sql`, `move_viewership_to_staging_pluto.sql`, `move_viewership_to_staging_wurl.sql`, `move_viewership_to_staging_youtube.sql`

**`2.normalize/`** - Old normalization architecture
- `global/` - Global normalization helpers
- `initial_setters/` - Platform-specific setters (amagi, pluto, wurl)
- `normalize_data_in_staging_amagi.sql`, `normalize_data_in_staging_wurl.sql`, `normalize_data_in_staging_youtube.sql`
- `pluto.sql`, `roku.sql` - Platform-specific procedures
- `set_quarter_dynamic.sql`, `set_viewership_partner.sql`

**`3.content_references/`** - Old content matching
- `analyze_and_process_viewership_data.sql` - Original asset matching coordinator

**`4.table_migrations/`** - Old migration logic
- `handle_final_insert.sql` - Original final insert procedure
- `last_aired.sql` - Air date handling

**`buckets/`** (root level) - Old bucket processing before staging/production split
- `process_viewership_*_generic.sql` - Various bucket processors (full_data, ref_id_only, series_only, etc.)

**`generic/`** (root level) - Old "generic" procedures before environment split
- `move_viewership_to_staging.sql`, `generic_sanitization.sql`, `move_sanitized_data_to_staging_generic.sql`
- `set_phase_generic.sql`, `normalize_data_in_staging.sql`, `normalize_data_in_staging_simple.sql`
- `set_ref_id_from_platform_content_id.sql`, `validate_viewership_for_insert.sql`
- `move_streamlit_data_to_staging.sql`, `set_date_columns_dynamic.sql`
- `helpers/` - Platform-specific helpers (pluto)

**`helpers/`** (root level) - Old helper procedures
- `date_helpers.sql`, `extract_primary_title.sql`, `fuzzy_score.sql`, `set_phase.sql`, `calculate_viewership_metrics.sql`

**Setup Scripts**
- `CREATE_GENERIC_PHASE2_PROCEDURES.sql` - Old Phase 2 setup script
- `CREATE_GENERIC_PHASE3_PROCEDURES.sql` - Old Phase 3 setup script

## Why Archived

### Key Problems with Old Architecture:

1. **No Staging/Production Separation**: All procedures targeted a single database without environment distinction
2. **Platform-Specific Code**: Duplicate logic for each platform (Amagi, Pluto, Wurl, YouTube, Roku)
3. **Scattered Organization**: Procedures spread across numbered directories, generic/, helpers/, buckets/, etc.
4. **Maintenance Burden**: Changes required updating multiple platform-specific versions
5. **Error-Prone**: Easy to miss updating one platform's version during refactoring

## New Architecture (Retained)

```
snowflake/stored_procedures/
├── staging/
│   ├── generic/          # All platforms, targeting UPLOAD_DB
│   ├── buckets/          # Bucket processors for staging
│   └── helpers/          # Helper procedures for staging
├── production/
│   ├── generic/          # All platforms, targeting UPLOAD_DB_PROD
│   ├── buckets/          # Bucket processors for production
│   └── helpers/          # Helper procedures for production
├── deploy_staging.py     # Deploy to UPLOAD_DB
└── deploy_production.py  # Deploy to UPLOAD_DB_PROD
```

### Benefits of New Architecture:

1. **Environment Isolation**: Staging and production procedures are completely separate
2. **Generic Platform Support**: One set of procedures handles all platforms via `platform` parameter
3. **Clear Organization**: All active code in two directories (staging/ and production/)
4. **Single Source of Truth**: Each procedure exists once per environment, not once per platform
5. **Safe Deployment**: Separate deployment scripts with safety prompts for production

## Current Canonical Procedures (Dec 2025)

**Normalization (Phase 0 → 1)**:
- `normalize_data_in_staging_generic.sql` - Streamlit path wrapper (includes SET_REF_ID)
- `normalize_data_in_staging.sql` - S3 upload path wrapper
- `set_deal_parent_generic.sql` - Sets deal/partner/channel/territory
- `set_ref_id_from_platform_content_id.sql` - Maps platform IDs to internal ref_ids
- `calculate_viewership_metrics.sql` - Calculates TOT_MOV from TOT_HOV
- `set_date_columns_dynamic.sql` - Sets date dimensions
- `set_phase_generic.sql` - Updates phase field

**Asset Matching (Phase 1 → 2)**:
- `analyze_and_process_viewership_data_generic.sql` - Main asset matching coordinator
- `set_internal_series_generic.sql` - Matches series names

**Data Movement**:
- `move_viewership_to_staging.sql` - S3 upload: UPLOAD_DB → viewership DB
- `move_streamlit_data_to_staging.sql` - Streamlit upload: UPLOAD_DB → viewership DB
- `handle_final_insert_dynamic_generic.sql` - Moves matched data to EPISODE_DETAILS

## Historical Context

This architecture evolved through several iterations:

1. **Platform-Specific Tables** (2023-2024): `tubi_viewership`, `pluto_viewership`, etc.
2. **First Generic Attempt** (2024): `platform_viewership` table with platform-specific procedures
3. **Generic Procedures** (2024-2025): Single set of generic procedures, but no env separation
4. **Staging/Production Split** (Dec 2025): Current architecture with environment isolation

## References

- Current architecture: `snowflake/stored_procedures/README.md`
- Staging/production separation: `PRODUCTION_FIX_2025_12_03.md`
- Lambda ref_id fix: `LAMBDA_FIX_REF_ID.md` (root directory)
- Government knowledge: `.claude/government/governors/snowflake/knowledge/CANONICAL.md`

## Restoration Notes

If you ever need to reference this old code:

1. **Don't restore it**: The new architecture is superior in every way
2. **Use as reference**: Good for understanding historical decisions and evolution
3. **Pattern matching**: May contain logic patterns worth preserving in new generic code
4. **Platform specifics**: Platform-specific quirks may be documented in old platform procedures

## Archived By

Mr. President (Claude Code) with approval from Oracle (Taylor)
Date: December 5, 2025
Reason: Comprehensive cleanup following staging/production separation completion
