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
print("Checking bucket procedure errors")
print("=" * 80)
print()

# Get all error messages from bucket procedures
cursor.execute("""
    SELECT
        log_time,
        procedure_name,
        log_message,
        error_message
    FROM UPLOAD_DB.public.error_log_table
    WHERE platform = 'Youtube'
    AND log_time >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
    AND (status = 'ERROR' OR error_message IS NOT NULL)
    ORDER BY log_time DESC
    LIMIT 20
""")

errors = cursor.fetchall()

if errors:
    for log_time, proc, msg, err in errors:
        print(f"[{log_time}] {proc}")
        print(f"  Message: {msg}")
        if err:
            print(f"  ERROR: {err}")
        print()
else:
    print("No errors found")

conn.close()
