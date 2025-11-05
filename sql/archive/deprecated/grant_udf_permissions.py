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
print("Granting EXECUTE permissions on EXTRACT_PRIMARY_TITLE")
print("=" * 80)
print()

# Grant for STAGING
print("📝 Granting EXECUTE on UPLOAD_DB.PUBLIC.EXTRACT_PRIMARY_TITLE to WEB_APP...")
try:
    cursor.execute("""
        GRANT USAGE ON FUNCTION UPLOAD_DB.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR) TO ROLE WEB_APP
    """)
    print("✅ Staging permissions granted")
except Exception as e:
    print(f"⚠️  Staging: {e}")
print()

# Grant for PROD
print("📝 Granting EXECUTE on UPLOAD_DB_PROD.PUBLIC.EXTRACT_PRIMARY_TITLE to WEB_APP...")
try:
    cursor.execute("""
        GRANT USAGE ON FUNCTION UPLOAD_DB_PROD.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR) TO ROLE WEB_APP
    """)
    print("✅ Prod permissions granted")
except Exception as e:
    print(f"⚠️  Prod: {e}")
print()

print("=" * 80)
print("✅ Permissions granted!")
print("=" * 80)

conn.close()
