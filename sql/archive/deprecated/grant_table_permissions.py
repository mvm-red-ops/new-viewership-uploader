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
print("Granting permissions on record_reprocessing_batch_logs")
print("=" * 80)
print()

# Grant for STAGING
print("📝 Granting INSERT/SELECT on METADATA_MASTER_CLEANED_STAGING.PUBLIC.record_reprocessing_batch_logs...")
try:
    cursor.execute("""
        GRANT INSERT, SELECT ON TABLE METADATA_MASTER_CLEANED_STAGING.PUBLIC.record_reprocessing_batch_logs TO ROLE WEB_APP
    """)
    print("✅ Staging permissions granted")
except Exception as e:
    print(f"⚠️  Staging: {e}")
print()

# Grant for PROD
print("📝 Granting INSERT/SELECT on METADATA_MASTER_CLEANED.PUBLIC.record_reprocessing_batch_logs...")
try:
    cursor.execute("""
        GRANT INSERT, SELECT ON TABLE METADATA_MASTER_CLEANED.PUBLIC.record_reprocessing_batch_logs TO ROLE WEB_APP
    """)
    print("✅ Prod permissions granted")
except Exception as e:
    print(f"⚠️  Prod: {e}")
print()

print("=" * 80)
print("✅ Permissions granted!")
print("=" * 80)

conn.close()
