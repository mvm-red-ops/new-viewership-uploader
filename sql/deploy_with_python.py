#!/usr/bin/env python3
"""
Deploy SQL files to Snowflake using Python
Usage: python3 deploy_with_python.py staging CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql
"""

import sys
import snowflake.connector
from pathlib import Path

# Connection configs
CONFIGS = {
    'staging': {
        'account': '***REMOVED***',
        'user': 'NOSEY_APP',
        'password': '***REMOVED***',
        'warehouse': 'COMPUTE_WH',
        'role': 'ACCOUNTADMIN'
    },
    'prod': {
        'account': '***REMOVED***',
        'user': 'NOSEY_APP',
        'password': '***REMOVED***',
        'warehouse': 'COMPUTE_WH',
        'role': 'ACCOUNTADMIN'
    }
}

def deploy_sql(env, sql_file):
    """Deploy a SQL file to the specified environment"""

    if env not in CONFIGS:
        print(f"❌ Error: Environment must be 'staging' or 'prod'")
        sys.exit(1)

    # Read SQL file
    sql_path = Path('generated') / f"{env}_{sql_file}"
    if not sql_path.exists():
        print(f"❌ Error: File not found: {sql_path}")
        print(f"   Run: ./deploy.sh {env} {sql_file} first")
        sys.exit(1)

    sql = sql_path.read_text()

    print(f"🔗 Connecting to Snowflake ({env})...")
    config = CONFIGS[env]

    try:
        conn = snowflake.connector.connect(**config)
        cursor = conn.cursor()

        print(f"✓ Connected as {config['user']}")
        print(f"📄 Executing SQL from: {sql_path}")

        # Execute SQL (handle multiple statements)
        for result in cursor.execute(sql, num_statements=0):
            pass  # Process all statements

        print(f"✅ Deployment successful!")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Deployment failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 deploy_with_python.py <environment> <sql-file>")
        print("")
        print("Example:")
        print("  python3 deploy_with_python.py staging CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql")
        sys.exit(1)

    env = sys.argv[1]
    sql_file = sys.argv[2]

    deploy_sql(env, sql_file)
