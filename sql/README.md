# SQL Deployment Files

This directory contains SQL scripts for setting up and deploying the viewership uploader system.

## Initial Setup (Run Once)

### 1. Table Schemas
- `create_platform_viewership.sql` - Creates upload_db.public.platform_viewership table (DEV/STAGING)
- `create_platform_viewership_prod.sql` - Creates upload_db_prod.public.platform_viewership table (PROD)
- `setup_staging_table.sql` - Creates test_staging.public.platform_viewership table

### 2. Initial Configuration
- `SETUP.sql` - Initial database setup (if exists)

## Deployment (Run When Updating)

### Main Deployment File
**`DEPLOY_ALL_GENERIC_PROCEDURES.sql`** - Deploy all generic stored procedures
- Run this file whenever procedures need to be updated
- Contains all 6 generic procedures:
  1. set_phase_generic
  2. calculate_viewership_metrics
  3. set_internal_series_generic
  4. analyze_and_process_viewership_data_generic
  5. move_data_to_final_table_dynamic_generic
  6. handle_final_insert_dynamic_generic

### Legacy Files
- `CREATE_ALL_PROCEDURES.sql` - Old procedures (kept for reference)

## Quick Start

1. First time setup:
   ```sql
   -- Run table creation scripts
   -- Add data_type column to config table:
   ALTER TABLE dictionary.public.viewership_file_formats ADD COLUMN data_type VARCHAR(50);
   ```

2. Deploy/Update procedures:
   ```sql
   -- Run this file:
   DEPLOY_ALL_GENERIC_PROCEDURES.sql
   ```

3. Deploy Lambda with updated code

4. Test upload!
