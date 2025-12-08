import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("Checking validation procedure definition...")

cursor.execute("""
    SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(VARCHAR, VARCHAR, VARCHAR)')
""")

ddl = cursor.fetchone()[0]

# Check if it has the old week check or new year/quarter check
if 'Missing week' in ddl:
    print("❌ FOUND OLD VERSION - Still checking for 'week'")
elif 'Missing year' in ddl and 'Missing quarter' in ddl:
    print("✅ FOUND NEW VERSION - Checking for year/quarter/year_month_day")
else:
    print("⚠️ UNKNOWN VERSION")

# Show relevant lines
lines = ddl.split('\n')
for i, line in enumerate(lines):
    if 'WHEN' in line and ('week' in line.lower() or 'year' in line.lower() or 'quarter' in line.lower()):
        print(f"Line {i}: {line}")

cursor.close()
conn.close()
