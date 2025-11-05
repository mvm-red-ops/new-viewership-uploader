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
print("Checking what happened with YouTube INSERT")
print("=" * 80)
print()

# Check current phase
cursor.execute(f"""
    SELECT
        phase,
        processed,
        COUNT(*) as count
    FROM TEST_STAGING.PUBLIC.platform_viewership
    WHERE platform = 'Youtube'
    AND filename = '{filename}'
    GROUP BY phase, processed
    ORDER BY phase
""")
phase_dist = cursor.fetchall()

print("Current phase distribution:")
for phase, processed, count in phase_dist:
    proc_status = "PROCESSED" if processed else "NOT PROCESSED"
    print(f"  Phase {phase}, {proc_status}: {count:,}")
print()

# Check error logs for the INSERT
cursor.execute(f"""
    SELECT
        log_time,
        log_message,
        error_message
    FROM UPLOAD_DB.public.error_log_table
    WHERE platform = 'Youtube'
    AND procedure_name = 'move_data_to_final_table_dynamic_generic'
    AND log_time >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
    ORDER BY log_time DESC
    LIMIT 5
""")
errors = cursor.fetchall()

if errors:
    print("Recent INSERT procedure logs:")
    for log_time, msg, err in errors:
        print(f"  [{log_time}] {msg}")
        if err:
            print(f"     ERROR: {err}")
print()

# Check if any final unmatched logging happened
cursor.execute(f"""
    SELECT
        log_message
    FROM UPLOAD_DB.public.error_log_table
    WHERE platform = 'Youtube'
    AND log_message LIKE '%final unmatched%'
    AND log_time >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
    ORDER BY log_time DESC
    LIMIT 3
""")
unmatched_logs = cursor.fetchall()

if unmatched_logs:
    print("Unmatched logging messages:")
    for (msg,) in unmatched_logs:
        print(f"  {msg}")
else:
    print("⚠️  No 'final unmatched' logging found - the new code may not have run")
print()

# Check how many records match INSERT criteria in phase 3
cursor.execute(f"""
    SELECT COUNT(*)
    FROM TEST_STAGING.PUBLIC.platform_viewership
    WHERE platform = 'Youtube'
    AND filename = '{filename}'
    AND processed IS NULL
    AND phase = '3'
    AND deal_parent IS NOT NULL
    AND ref_id IS NOT NULL
    AND asset_series IS NOT NULL
    AND tot_mov IS NOT NULL
    AND tot_hov IS NOT NULL
""")
eligible_phase3 = cursor.fetchone()[0]

print(f"Records in phase 3 eligible for INSERT: {eligible_phase3:,}")

conn.close()
