#!/usr/bin/env python3
"""
Deploy all stored procedures to PRODUCTION environment (UPLOAD_DB_PROD)

WARNING: This deploys to PRODUCTION. Only run after testing in staging!
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import snowflake.connector
from config import load_snowflake_config

def deploy_file(conn, cursor, file_path, env="production"):
    """Deploy a single SQL file (handles multi-statement files with $$ delimiters)"""
    with open(file_path, 'r') as f:
        sql = f.read()

    try:
        cursor.execute('USE DATABASE UPLOAD_DB_PROD')

        # Use execute_string to handle multiple statements (CREATE + GRANT)
        for result in conn.execute_string(sql):
            pass

        return True, None
    except Exception as e:
        return False, str(e)

def main():
    # Safety check
    print()
    print("=" * 80)
    print("⚠️  WARNING: YOU ARE ABOUT TO DEPLOY TO PRODUCTION")
    print("=" * 80)
    print()
    response = input("Type 'DEPLOY TO PRODUCTION' to continue: ")

    if response != "DEPLOY TO PRODUCTION":
        print("❌ Deployment cancelled")
        sys.exit(1)

    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(
        user=sf_config['user'],
        password=sf_config['password'],
        account=sf_config['account'],
        warehouse=sf_config['warehouse'],
        role=sf_config['role']
    )

    cursor = conn.cursor()

    print()
    print("=" * 80)
    print("DEPLOYING TO PRODUCTION (UPLOAD_DB_PROD)")
    print("=" * 80)
    print()

    # Collect all SQL files in production/
    sql_files = []
    for root, dirs, files in os.walk('production'):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))

    sql_files.sort()

    success_count = 0
    fail_count = 0

    for sql_file in sql_files:
        proc_name = os.path.basename(sql_file).replace('.sql', '')
        success, error = deploy_file(conn, cursor, sql_file)

        if success:
            print(f"✓ {proc_name}")
            success_count += 1
        else:
            print(f"✗ {proc_name}")
            print(f"  Error: {error}")
            fail_count += 1

    print()
    print("=" * 80)
    print(f"DEPLOYMENT COMPLETE: {success_count} succeeded, {fail_count} failed")
    print("=" * 80)

    cursor.close()
    conn.close()

    sys.exit(0 if fail_count == 0 else 1)

if __name__ == '__main__':
    main()
