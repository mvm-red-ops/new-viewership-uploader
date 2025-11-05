#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
from config import load_snowflake_config

sf_config = load_snowflake_config()
conn = snowflake.connector.connect(**sf_config)
cursor = conn.cursor()

filename = 'youtube_daily_2025-07-01_to_2025-09-30 (1).csv'

print("=" * 80)
print("Checking NULL columns in Phase 3")
print("=" * 80)
print()

# Check each required column for NULLs
checks = [
    ("deal_parent IS NULL", "deal_parent"),
    ("ref_id IS NULL", "ref_id"),
    ("asset_series IS NULL", "asset_series"),
    ("tot_mov IS NULL", "tot_mov"),
    ("tot_hov IS NULL", "tot_hov"),
]

print("Required columns with NULL values in phase 3:")
for condition, col_name in checks:
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM TEST_STAGING.PUBLIC.platform_viewership
        WHERE platform = 'Youtube'
        AND filename = '{filename}'
        AND processed IS NULL
        AND phase = '3'
        AND {condition}
    """)
    count = cursor.fetchone()[0]
    status = "❌" if count > 0 else "✅"
    print(f"{status} {col_name:20s}: {count:,} NULL records")

print()
print("-" * 80)

# Sample some records to see what's actually in them
cursor.execute(f"""
    SELECT
        deal_parent,
        ref_id,
        asset_series,
        tot_mov,
        tot_hov
    FROM TEST_STAGING.PUBLIC.platform_viewership
    WHERE platform = 'Youtube'
    AND filename = '{filename}'
    AND phase = '3'
    LIMIT 5
""")
samples = cursor.fetchall()

print("\nSample of 5 records in phase 3:")
for i, (deal, ref, series, mov, hov) in enumerate(samples, 1):
    print(f"\nRecord {i}:")
    print(f"  deal_parent:  {deal}")
    print(f"  ref_id:       {ref}")
    print(f"  asset_series: {series}")
    print(f"  tot_mov:      {mov}")
    print(f"  tot_hov:      {hov}")

conn.close()
