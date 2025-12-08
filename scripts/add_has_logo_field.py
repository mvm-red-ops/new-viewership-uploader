#!/usr/bin/env python3
"""
Add has_logo field to viewership_file_formats table

This script adds a new column to support auto-detection and removal of
logo/banner rows in uploaded files (e.g., Pluto revenue files).
"""

import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
from config import load_snowflake_config

def add_has_logo_field():
    """Add has_logo column to viewership_file_formats table"""

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**load_snowflake_config())
    cursor = conn.cursor()

    try:
        # Add the has_logo column
        alter_sql = """
        ALTER TABLE dictionary.public.viewership_file_formats
        ADD COLUMN IF NOT EXISTS has_logo BOOLEAN DEFAULT FALSE
        """

        print("Adding has_logo column to viewership_file_formats table...")
        cursor.execute(alter_sql)
        conn.commit()
        print("✅ Successfully added has_logo column!")

        # Verify the column was added
        print("\nVerifying column exists...")
        cursor.execute("DESCRIBE TABLE dictionary.public.viewership_file_formats")
        columns = cursor.fetchall()

        has_logo_found = False
        for col in columns:
            if col[0] == 'HAS_LOGO':
                has_logo_found = True
                print(f"✅ Verified: {col[0]} ({col[1]}) - Default: {col[3]}")
                break

        if not has_logo_found:
            print("⚠️ Warning: has_logo column not found in table description")

        # Show current table schema
        print("\nCurrent table schema:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]}")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    add_has_logo_field()
