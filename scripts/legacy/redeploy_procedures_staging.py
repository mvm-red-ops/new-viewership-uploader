#!/usr/bin/env python3
"""
Redeploy procedures to staging
"""

import snowflake.connector
from config import load_snowflake_config

def redeploy_procedures():
    """Redeploy all generic procedures to staging"""

    print("=" * 80)
    print("🔧 Redeploying Procedures to Staging")
    print("=" * 80)
    print()

    # Read the template
    with open('sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql', 'r') as f:
        template = f.read()

    # Replace placeholders for STAGING
    sql = template.replace('{{UPLOAD_DB}}', 'UPLOAD_DB')
    sql = sql.replace('{{STAGING_DB}}', 'TEST_STAGING')
    sql = sql.replace('{{ASSETS_DB}}', 'STAGING_ASSETS')
    sql = sql.replace('{{EPISODE_DETAILS_TABLE}}', 'EPISODE_DETAILS_TEST_STAGING')
    sql = sql.replace('{{METADATA_DB}}', 'METADATA_MASTER_CLEANED_STAGING')

    # Remove all comment-only lines and trailing/leading whitespace
    lines = sql.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip empty lines and comment-only lines
        if stripped and not stripped.startswith('--'):
            clean_lines.append(line)
    sql = '\n'.join(clean_lines)

    # Remove trailing whitespace
    sql = sql.strip()

    # Connect to Snowflake
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)

    try:
        print("🚀 Deploying procedures to staging...")
        print("   This may take a few minutes...")
        print()

        # Use execute_string which handles multiple statements properly
        for result_cursor in conn.execute_string(sql):
            pass  # Just iterate through to execute all

        print()
        print("=" * 80)
        print("✅ Procedures deployed successfully to staging!")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == '__main__':
    redeploy_procedures()
