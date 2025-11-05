#!/usr/bin/env python3
"""
Deploy SQL files to Snowflake using Python
Usage: python3 deploy_with_python.py staging CREATE_VALIDATE_VIEWERSHIP_FOR_INSERT.sql

Credentials are loaded from:
1. Environment variables (SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, etc.)
2. deploy_credentials.py file (gitignored)

To set up credentials, create deploy_credentials.py with:
SNOWFLAKE_USER = 'your_user'
SNOWFLAKE_PASSWORD = 'your_password'
SNOWFLAKE_ACCOUNT = 'your_account'
SNOWFLAKE_WAREHOUSE = 'your_warehouse'
SNOWFLAKE_ROLE = 'your_role'
"""

import sys
import os
import snowflake.connector
from pathlib import Path

def load_credentials():
    """Load Snowflake credentials from environment or local config"""
    # Try environment variables first
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')

    # If not in env, try to load from local config file
    if not all([user, password, account]):
        try:
            import deploy_credentials as creds
            user = getattr(creds, 'SNOWFLAKE_USER', user)
            password = getattr(creds, 'SNOWFLAKE_PASSWORD', password)
            account = getattr(creds, 'SNOWFLAKE_ACCOUNT', account)
            warehouse = getattr(creds, 'SNOWFLAKE_WAREHOUSE', warehouse)
            role = getattr(creds, 'SNOWFLAKE_ROLE', role)
        except ImportError:
            pass

    # Validate required fields
    if not all([user, password, account]):
        print("❌ Error: Missing Snowflake credentials!")
        print("   Set environment variables (SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT)")
        print("   Or create deploy_credentials.py with credentials (see file header for format)")
        sys.exit(1)

    return {
        'user': user,
        'password': password,
        'account': account,
        'warehouse': warehouse,
        'role': role
    }

def deploy_sql(env, sql_file):
    """Deploy a SQL file to the specified environment"""

    if env not in ['staging', 'prod']:
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
    config = load_credentials()

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
