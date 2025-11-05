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
print("Checking EXTRACT_PRIMARY_TITLE in different databases")
print("=" * 80)
print()

databases = ['UPLOAD_DB', 'UPLOAD_DB_PROD']

for db in databases:
    print(f"Checking {db}...")
    try:
        cursor.execute(f"USE DATABASE {db}")
        cursor.execute("SHOW USER FUNCTIONS LIKE 'EXTRACT_PRIMARY_TITLE' IN SCHEMA PUBLIC")
        functions = cursor.fetchall()

        if functions:
            print(f"  ✅ Found in {db}.PUBLIC")
            for func in functions:
                print(f"     {func[8]}")
        else:
            print(f"  ❌ NOT FOUND in {db}.PUBLIC")
    except Exception as e:
        print(f"  ❌ Error checking {db}: {e}")
    print()

# Get the actual definition from UPLOAD_DB
print("=" * 80)
print("Getting function definition from UPLOAD_DB")
print("=" * 80)
print()

try:
    cursor.execute("USE DATABASE UPLOAD_DB")
    cursor.execute("SELECT GET_DDL('FUNCTION', 'UPLOAD_DB.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR)')")
    ddl = cursor.fetchone()
    if ddl:
        print(ddl[0])
except Exception as e:
    print(f"Error getting DDL: {e}")

conn.close()
