#!/usr/bin/env python3
"""
Extract working procedures from STAGING and create templated versions for PROD deployment.

This script:
1. Gets DDL from working STAGING procedures
2. Replaces hardcoded database names with template variables
3. Creates deployment-ready SQL files
"""

import snowflake.connector
import toml
import re

# Key procedures that need to be synced
KEY_PROCEDURES = [
    ('CALCULATE_VIEWERSHIP_METRICS', 'STRING, STRING'),
    ('SET_DATE_COLUMNS_DYNAMIC', 'STRING, STRING'),
    ('SET_REF_ID_FROM_PLATFORM_CONTENT_ID', 'STRING, STRING'),
    ('SET_DEAL_PARENT_GENERIC', 'STRING, STRING'),
    ('SET_PHASE_GENERIC', 'STRING, STRING, STRING'),
    ('ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC', 'STRING, STRING'),
    ('HANDLE_FINAL_INSERT_DYNAMIC_GENERIC', 'STRING, STRING, STRING'),
]

# Template replacements
TEMPLATE_MAPPINGS = {
    'test_staging': '{{STAGING_DB}}',
    'TEST_STAGING': '{{STAGING_DB}}',
    'nosey_prod': '{{STAGING_DB}}',
    'NOSEY_PROD': '{{STAGING_DB}}',
    'upload_db_prod': '{{UPLOAD_DB}}',
    'UPLOAD_DB_PROD': '{{UPLOAD_DB}}',
    'upload_db': '{{UPLOAD_DB}}',
    'UPLOAD_DB': '{{UPLOAD_DB}}',
    'metadata_master_cleaned_staging': '{{METADATA_DB}}',
    'METADATA_MASTER_CLEANED_STAGING': '{{METADATA_DB}}',
    'metadata_master': '{{METADATA_DB}}',
    'METADATA_MASTER': '{{METADATA_DB}}',
    'staging_assets': '{{ASSETS_DB}}',
    'STAGING_ASSETS': '{{ASSETS_DB}}',
    'assets': '{{ASSETS_DB}}',
    'ASSETS': '{{ASSETS_DB}}',
}

def template_sql(sql):
    """Replace hardcoded database names with template variables"""
    for old, new in TEMPLATE_MAPPINGS.items():
        # Match database.schema pattern
        sql = re.sub(rf'\b{old}\.', f'{new}.', sql, flags=re.IGNORECASE)
    return sql

def main():
    # Load credentials
    with open('.streamlit/secrets.toml', 'r') as f:
        secrets = toml.load(f)

    # Connect to staging
    conn = snowflake.connector.connect(
        user=secrets['snowflake']['user'],
        password=secrets['snowflake']['password'],
        account=secrets['snowflake']['account'],
        warehouse='COMPUTE_WH',
        database='UPLOAD_DB'
    )

    cursor = conn.cursor()

    print("=" * 80)
    print("Extracting Working Procedures from STAGING")
    print("=" * 80)

    templated_procedures = []

    for proc_name, params in KEY_PROCEDURES:
        print(f"\n{proc_name}:")
        print("-" * 80)

        try:
            # Get DDL from staging
            query = f"SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.{proc_name}({params})')"
            cursor.execute(query)
            ddl = cursor.fetchone()[0]

            # Template it
            templated_ddl = template_sql(ddl)

            # Remove quotes around identifiers (Snowflake DDL adds them but they're not needed)
            templated_ddl = templated_ddl.replace(f'"{proc_name}"', proc_name)
            templated_ddl = templated_ddl.replace('"PLATFORM"', 'PLATFORM')
            templated_ddl = templated_ddl.replace('"FILENAME"', 'FILENAME')
            templated_ddl = templated_ddl.replace('"TYPE"', 'TYPE')

            # Replace the CREATE statement to use template variable
            # The DDL comes back as: create or replace procedure UPLOAD_DB.PUBLIC.PROC_NAME(
            # We want: CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.proc_name(
            templated_ddl = re.sub(
                r'CREATE OR REPLACE PROCEDURE\s+' + proc_name,
                f'CREATE OR REPLACE PROCEDURE {{{{UPLOAD_DB}}}}.public.{proc_name.lower()}',
                templated_ddl,
                flags=re.IGNORECASE
            )

            # IMPORTANT: Replace single quote delimiters with $$ for JavaScript body
            # This prevents execute_string from breaking on semicolons within the JS code
            # Pattern: AS ' ... '; becomes AS $$ ... $$
            # First pass - AS '
            templated_ddl = re.sub(
                r"(AS)\s+'",
                r"\1\n$$",
                templated_ddl,
                flags=re.IGNORECASE
            )
            # Second pass - closing '; at end (but not within JavaScript strings)
            templated_ddl = re.sub(
                r"'\s*;?\s*$",
                "$$",
                templated_ddl.strip()
            )

            templated_procedures.append({
                'name': proc_name,
                'ddl': templated_ddl
            })

            print(f"✅ Extracted and templated")

        except Exception as e:
            print(f"❌ Error: {e}")

    cursor.close()
    conn.close()

    # Write to output file
    output_file = 'sql/templates/DEPLOY_KEY_GENERIC_PROCEDURES_FIXED.sql'

    with open(output_file, 'w') as f:
        f.write("-- ============================================================================\n")
        f.write("-- KEY GENERIC PROCEDURES - TEMPLATED FROM WORKING STAGING VERSIONS\n")
        f.write("-- ============================================================================\n")
        f.write("-- Generated by extract_and_template_procedures.py\n")
        f.write("-- These are the working procedures from staging with database names templated\n")
        f.write("-- ============================================================================\n\n")

        for i, proc in enumerate(templated_procedures):
            f.write(f"-- ============================================================================\n")
            f.write(f"-- {proc['name']}\n")
            f.write(f"-- ============================================================================\n\n")

            # Remove trailing semicolon if exists (DDL from GET_DDL includes it)
            ddl = proc['ddl'].rstrip().rstrip(';')
            f.write(ddl)
            f.write(";\n\n")

            # Add grant statement
            params = KEY_PROCEDURES[i][1]
            f.write(f"GRANT USAGE ON PROCEDURE {{{{UPLOAD_DB}}}}.PUBLIC.{proc['name']}({params}) TO ROLE web_app;\n\n")

    print(f"\n{'=' * 80}")
    print(f"✅ Created templated file: {output_file}")
    print(f"{'=' * 80}")
    print(f"\nNext steps:")
    print(f"1. Review the generated file")
    print(f"2. Deploy to prod: python sql/deploy/deploy.py --env prod")

if __name__ == '__main__':
    main()
