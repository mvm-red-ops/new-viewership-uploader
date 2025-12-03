#!/usr/bin/env python3
"""
Deploy all stored procedures to STAGING environment (UPLOAD_DB)
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import snowflake.connector
from config import load_snowflake_config

def deploy_file(cursor, file_path, env="staging"):
    """Deploy a single SQL file"""
    with open(file_path, 'r') as f:
        sql = f.read()

    try:
        cursor.execute('USE DATABASE UPLOAD_DB')
        cursor.execute(sql)
        return True, None
    except Exception as e:
        return False, str(e)

def main():
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(
        user=sf_config['user'],
        password=sf_config['password'],
        account=sf_config['account'],
        warehouse=sf_config['warehouse'],
        role=sf_config['role']
    )

    cursor = conn.cursor()

    print("=" * 80)
    print("DEPLOYING TO STAGING (UPLOAD_DB)")
    print("=" * 80)
    print()

    # Collect all SQL files in staging/
    sql_files = []
    for root, dirs, files in os.walk('staging'):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))

    sql_files.sort()

    success_count = 0
    fail_count = 0

    for sql_file in sql_files:
        proc_name = os.path.basename(sql_file).replace('.sql', '')
        success, error = deploy_file(cursor, sql_file)

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
