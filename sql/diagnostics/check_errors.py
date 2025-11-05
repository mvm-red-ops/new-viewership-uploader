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
print("Checking error logs for YouTube upload")
print("=" * 80)

# Check error logs
cursor.execute("""
    SELECT
        log_time,
        procedure_name,
        log_message,
        error_message,
        status
    FROM UPLOAD_DB.public.error_log_table
    WHERE platform = 'Youtube'
    AND log_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
    ORDER BY log_time DESC
    LIMIT 20
""")

rows = cursor.fetchall()
if rows:
    for row in rows:
        log_time, proc, msg, err, status = row
        print(f"\n[{log_time}] {proc}")
        print(f"  Status: {status}")
        print(f"  Message: {msg}")
        if err:
            print(f"  ERROR: {err}")
else:
    print("\nNo error logs found")

conn.close()
