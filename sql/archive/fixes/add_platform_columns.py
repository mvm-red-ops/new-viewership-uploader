#!/usr/bin/env python3
"""
Add PLATFORM_PARTNER_NAME, PLATFORM_CHANNEL_NAME, PLATFORM_TERRITORY columns
to both staging and prod EPISODE_DETAILS tables
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
from config import load_snowflake_config

def add_columns():
    """Add missing columns to EPISODE_DETAILS tables"""

    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)

    try:
        cursor = conn.cursor()

        print("=" * 80)
        print("Adding Platform Columns to EPISODE_DETAILS Tables")
        print("=" * 80)
        print()

        # Add to STAGING
        print("📝 Adding columns to STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING...")

        columns = [
            'PLATFORM_PARTNER_NAME',
            'PLATFORM_CHANNEL_NAME',
            'PLATFORM_TERRITORY'
        ]

        for col in columns:
            try:
                cursor.execute(f"""
                    ALTER TABLE STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING
                    ADD COLUMN {col} VARCHAR(500)
                """)
                print(f"  ✅ Added {col}")
            except Exception as e:
                if 'already exists' in str(e).lower():
                    print(f"  ⏭️  {col} already exists")
                else:
                    raise
        print()

        # Add to PROD
        print("📝 Adding columns to ASSETS.PUBLIC.EPISODE_DETAILS...")

        for col in columns:
            try:
                cursor.execute(f"""
                    ALTER TABLE ASSETS.PUBLIC.EPISODE_DETAILS
                    ADD COLUMN {col} VARCHAR(500)
                """)
                print(f"  ✅ Added {col}")
            except Exception as e:
                if 'already exists' in str(e).lower():
                    print(f"  ⏭️  {col} already exists")
                else:
                    raise
        print()

        print("=" * 80)
        print("✅ All columns added successfully!")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    add_columns()
