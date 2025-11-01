SQL SCRIPTS
===========

👉 RUN THESE IN ORDER:

1. SETUP.sql
   - Creates upload_db.public.platform_viewership table
   - Updates dictionary.public.viewership_file_formats config table

2. setup_staging_table.sql
   - Creates test_staging.public.platform_viewership table

3. CREATE_ALL_PROCEDURES.sql
   - Creates all stored procedures for Lambda post-processing
   - Phase 2: Asset matching (set_internal_series, analyze_and_process_viewership_data, set_phase_generic)
   - Phase 3: Final table migration (handle_final_insert_dynamic)
   - Legacy: S3 upload procedures (move_viewership_to_staging, normalize_data_in_staging)

📄 Reference:
   create_platform_viewership.sql - Template for staging table
   create_platform_viewership_prod.sql - Template for production table

After running all scripts:
1. Deploy Lambda with updated code
2. Restart Streamlit app
3. Test upload
