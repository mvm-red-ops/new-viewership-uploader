import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("Checking VALIDATE_VIEWERSHIP_FOR_INSERT...")
cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(STRING, STRING, STRING)')")
ddl = cursor.fetchone()[0]

# Check for revenue logic
if "includes('revenue')" in ddl:
    print("✅ Has revenue check")
else:
    print("❌ Missing revenue check")
    # Show a snippet
    if 'lowerDataType' in ddl:
        idx = ddl.find('lowerDataType')
        print(f"Found lowerDataType at position {idx}")
        print(ddl[idx:idx+200])

print("\nChecking SET_DATE_COLUMNS_DYNAMIC...")
cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(STRING, STRING)')")
ddl = cursor.fetchone()[0]

# Check for month conversion
if 'january' in ddl.lower():
    print("✅ Has month conversion")
else:
    print("❌ Missing month conversion")
    
# Check for quarter
if "'q1'" in ddl or '"q1"' in ddl:
    print("✅ Has lowercase quarters")
else:
    print("❌ Missing lowercase quarters")
    # Check if has uppercase
    if "'Q1'" in ddl or '"Q1"' in ddl:
        print("   (Has uppercase Q1 instead)")

conn.close()
