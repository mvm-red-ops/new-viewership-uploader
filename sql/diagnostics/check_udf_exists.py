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
print("Checking for EXTRACT_PRIMARY_TITLE function")
print("=" * 80)
print()

# Search for the function in all schemas
cursor.execute("""
    SHOW USER FUNCTIONS LIKE 'EXTRACT_PRIMARY_TITLE'
""")

functions = cursor.fetchall()

if functions:
    print("Found EXTRACT_PRIMARY_TITLE function(s):")
    for func in functions:
        print(f"  {func}")
    print()

    # Get the function definition
    for func in functions:
        func_name = func[1]  # Function name
        schema = func[2]  # Schema
        database = func[8] if len(func) > 8 else 'unknown'  # Database

        print(f"\nFunction: {database}.{schema}.{func_name}")

        try:
            cursor.execute(f"DESC FUNCTION {database}.{schema}.{func_name}(VARCHAR)")
            desc = cursor.fetchall()
            print("  Signature:", desc)
        except Exception as e:
            print(f"  Could not get signature: {e}")
else:
    print("❌ EXTRACT_PRIMARY_TITLE function NOT FOUND in any database/schema")
    print()
    print("Searching for similar functions...")

    cursor.execute("""
        SHOW USER FUNCTIONS LIKE '%PRIMARY%'
    """)

    similar = cursor.fetchall()
    if similar:
        print("Found similar functions:")
        for func in similar:
            print(f"  {func[8]}.{func[2]}.{func[1]}")
    else:
        print("No similar functions found")

conn.close()
