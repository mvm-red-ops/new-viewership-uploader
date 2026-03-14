# Architecture Details

## Streamlit Upload Pipeline (Production)

1. Streamlit UI → `UPLOAD_DB_PROD.public.platform_viewership` (processed=NULL, phase='')
2. `MOVE_STREAMLIT_DATA_TO_STAGING` → `NOSEY_PROD.public.platform_viewership` (phase='0')
3. `NORMALIZE_DATA_IN_STAGING_GENERIC` → phase '1' (includes SET_REF_ID step)
4. `ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC` → phase '2' (asset matching via buckets)
5. `HANDLE_FINAL_INSERT_DYNAMIC_GENERIC` → `ASSETS.public.EPISODE_DETAILS_*` (final analytics table)

## Bucket Matching Order (ANALYZE_AND_PROCESS)
1. FULL_DATA — ref_id + internal_series + episode_number + season_number
2. REF_ID_SERIES — ref_id + internal_series
3. REF_ID_ONLY — ref_id only
4. SERIES_SEASON_EPISODE — internal_series + episode + season (no ref_id)
5. SERIES_ONLY — internal_series only
6. TITLE_ONLY — platform_content_name only

## Database Mapping
| Staging | Production |
|---------|-----------|
| UPLOAD_DB | UPLOAD_DB_PROD |
| TEST_STAGING | NOSEY_PROD |
| STAGING_ASSETS | ASSETS |

## Key Tables
- `platform_viewership` — Central staging table (in TEST_STAGING or NOSEY_PROD)
- `EPISODE_DETAILS_*` — Final analytics tables (in STAGING_ASSETS or ASSETS)
- `record_reprocessing_batch_logs` — Tracks unmatched records
- `DICTIONARY.PUBLIC.VIEWERSHIP_FILE_FORMATS` — Template configs
- `DICTIONARY.PUBLIC.active_deals` — Partner/channel/territory matching

## Production ANALYZE_AND_PROCESS (FIXED)
The production version correctly references `NOSEY_PROD.public.platform_viewership`.
Bug was previously noted; confirmed fixed as of current codebase state.

## handle_final_insert_dynamic_generic
NOT in current staging/production dirs — only in archive:
`_archive/2025-12-05-pre-staging-production-split/stored_procedures/4.table_migrations/handle_final_insert_dynamic_generic.sql`
Lambda calls: `CALL ${databaseName}.public.handle_final_insert_dynamic_generic('platform', 'type', 'filename')`

## Lambda Streamlit Path (setContentReferences)
The Streamlit path in `lambda/snowflake-helpers.js` `setContentReferences()` calls individual procedures:
1. `set_deal_parent_generic`
2. `set_channel_generic`
3. `set_territory_generic`
4. `set_deal_parent_normalized_generic`
5. `send_unmatched_deals_alert`
6. `SET_INTERNAL_SERIES_WITH_EXTRACTION`
7. `set_internal_series_generic`
8. **`SET_REF_ID_FROM_PLATFORM_CONTENT_ID`** ← added to fix Tubi ref_id (recent commit)
9. `analyze_and_process_viewership_data_generic`
10. `set_phase_generic` (phase=2)

Note: LAMBDA_FIX_REF_ID.md suggested using NORMALIZE_DATA_IN_STAGING_GENERIC wrapper instead,
but actual fix was to add SET_REF_ID_FROM_PLATFORM_CONTENT_ID individually.

## Territory Matching (recent fix)
Territory lookup uses CONTAINS for case-insensitive array comparison (recent commits).
NULL territory in active_deals does NOT wildcard — only NULL in DATA is a wildcard.
