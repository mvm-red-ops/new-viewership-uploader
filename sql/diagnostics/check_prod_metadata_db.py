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
print("Searching for metadata database in prod")
print("=" * 80)
print()

# Search for databases with 'metadata' in name
cursor.execute("SHOW DATABASES LIKE '%METADATA%'")
dbs = cursor.fetchall()

if dbs:
    print("Found databases with 'METADATA':")
    for db in dbs:
        print(f"  - {db[1]}")
    print()

    # Check for record_reprocessing_batch_logs table in each
    print("Checking for record_reprocessing_batch_logs table:")
    for db in dbs:
        db_name = db[1]
        try:
            cursor.execute(f"SHOW TABLES LIKE 'record_reprocessing_batch_logs' IN DATABASE {db_name}")
            tables = cursor.fetchall()
            if tables:
                print(f"  ✅ Found in {db_name}.PUBLIC")
        except:
            pass
else:
    print("No databases found with 'METADATA' in name")

conn.close()
