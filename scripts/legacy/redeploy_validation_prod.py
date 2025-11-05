#!/usr/bin/env python3
"""
Redeploy validation procedure to production
"""

import snowflake.connector
from config import load_snowflake_config

def redeploy():
    """Redeploy validation procedure"""

    print("=" * 80)
    print("🔧 Redeploying Validation Procedure to Production")
    print("=" * 80)
    print()

    # Read the template
    with open('sql/templates/CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql', 'r') as f:
        template = f.read()

    # Replace placeholders for PRODUCTION
    sql = template.replace('{{UPLOAD_DB}}', 'upload_db_prod')
    sql = sql.replace('{{STAGING_DB}}', 'nosey_prod')

    # Connect to Snowflake
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)

    try:
        print("🚀 Deploying validation procedure to production...")

        # Use execute_string for multiple statements
        for _ in conn.execute_string(sql):
            pass

        print()
        print("=" * 80)
        print("✅ Validation procedure deployed successfully!")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == '__main__':
    redeploy()
