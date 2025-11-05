#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
from config import load_snowflake_config

sf_config = load_snowflake_config()
conn = snowflake.connector.connect(**sf_config)
cursor = conn.cursor()

print("=" * 80)
print("Checking YouTube filename")
print("=" * 80)

# Get actual filename in database
cursor.execute("""
    SELECT DISTINCT filename
    FROM test_staging.public.platform_viewership
    WHERE platform = 'Youtube'
    AND phase = '3'
""")
result = cursor.fetchone()
actual_filename = result[0] if result else None

print(f"\nActual filename in database:")
print(f"  '{actual_filename}'")
print(f"\nLowercase:")
print(f"  '{actual_filename.lower()}'")

# Check if INSERT query would match
test_filename = 'youtube_daily_2025-07-01_to_2025-09-30 (1).csv'
print(f"\nFilename from Lambda:")
print(f"  '{test_filename}'")
print(f"\nLowercase:")
print(f"  '{test_filename.lower()}'")

print(f"\nMatch: {actual_filename == test_filename}")
print(f"Lowercase match: {actual_filename.lower() == test_filename.lower()}")

# Test the INSERT WHERE clause
cursor.execute(f"""
    SELECT COUNT(*)
    FROM test_staging.public.platform_viewership
    WHERE platform = 'Youtube'
    AND deal_parent is not null
    AND processed is null
    AND ref_id is not null
    AND asset_series is not null
    AND tot_mov is not null
    AND tot_hov is not null
    AND LOWER(filename) = LOWER('{test_filename}')
""")
count = cursor.fetchone()[0]

print(f"\nRecords matching INSERT WHERE clause: {count:,}")

conn.close()
