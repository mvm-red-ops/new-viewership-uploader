#!/usr/bin/env python3
"""
Drop and redeploy the analyze_and_process_viewership_data_generic procedure to production.
This fixes the database reference issue causing matching failures.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import deploy module
sys.path.insert(0, str(Path(__file__).parent))

from deploy_with_python import load_credentials
import snowflake.connector

def redeploy():
    """Drop existing procedure and deploy new one with correct database references"""

    print("🔧 Redeploying analyze_and_process_viewership_data_generic to production...")
    print()

    # Load credentials
    print("🔗 Loading credentials...")
    config = load_credentials()

    # Read the generated SQL file
    sql_path = Path(__file__).parent / 'generated' / 'prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql'
    if not sql_path.exists():
        print(f"❌ Error: File not found: {sql_path}")
        print("   Run: ./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql first")
        sys.exit(1)

    sql = sql_path.read_text()

    try:
        # Connect to Snowflake
        print(f"🔗 Connecting to Snowflake (production)...")
        conn = snowflake.connector.connect(**config)
        cursor = conn.cursor()
        print(f"✓ Connected as {config['user']}")
        print()

        # Step 1: Drop existing procedures
        print("🗑️  Dropping existing procedures...")
        drop_statements = [
            "DROP PROCEDURE IF EXISTS upload_db_prod.public.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR, ARRAY);",
            "DROP PROCEDURE IF EXISTS upload_db_prod.public.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR);"
        ]

        for drop_sql in drop_statements:
            try:
                cursor.execute(drop_sql)
                print(f"   ✓ Executed: {drop_sql}")
            except Exception as e:
                print(f"   ⚠️  Warning: {str(e)}")

        print()
        print("✓ Old procedures dropped")
        print()

        # Step 2: Deploy new procedures
        print("📄 Deploying new procedures from: prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql")
        print("   (This will take 30-60 seconds...)")
        print()

        # Execute SQL (handle multiple statements)
        for result in cursor.execute(sql, num_statements=0):
            pass  # Process all statements

        print("✅ Deployment successful!")
        print()

        # Step 3: Verify the deployment
        print("🔍 Verifying deployment...")
        verify_sql = """
        SELECT
            procedure_name,
            argument_signature,
            created
        FROM upload_db_prod.information_schema.procedures
        WHERE procedure_name = 'ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC'
        ORDER BY created DESC
        LIMIT 1;
        """

        cursor.execute(verify_sql)
        result = cursor.fetchone()

        if result:
            print(f"   ✓ Procedure: {result[0]}")
            print(f"   ✓ Signature: {result[1]}")
            print(f"   ✓ Created: {result[2]}")
        else:
            print("   ⚠️  Warning: Could not verify procedure deployment")

        print()
        print("✅ Redeployment complete!")
        print()
        print("📝 Next steps:")
        print("   1. Test with a small file upload")
        print("   2. Check that records are matching correctly")
        print("   3. Verify data flows to EPISODE_DETAILS table")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Deployment failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    redeploy()
