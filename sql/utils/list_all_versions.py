import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("VALIDATE_VIEWERSHIP_FOR_INSERT versions:")
cursor.execute("SHOW PROCEDURES LIKE 'VALIDATE_VIEWERSHIP_FOR_INSERT' IN UPLOAD_DB_PROD.PUBLIC")
for proc in cursor.fetchall():
    print(f"  {proc[8]}")

print("\nSET_DATE_COLUMNS_DYNAMIC versions:")
cursor.execute("SHOW PROCEDURES LIKE 'SET_DATE_COLUMNS_DYNAMIC' IN UPLOAD_DB_PROD.PUBLIC")
for proc in cursor.fetchall():
    print(f"  {proc[8]}")

# Check the DDL of the 3-parameter version
print("\nChecking VALIDATE_VIEWERSHIP_FOR_INSERT(STRING, STRING, STRING)...")
try:
    cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(STRING, STRING, STRING)')")
    ddl = cursor.fetchone()[0]
    
    # Look for the data_type parameter default
    if "DATA_TYPE STRING DEFAULT 'Viewership'" in ddl:
        print("  Has default parameter: 'Viewership'")
    elif "DATA_TYPE STRING" in ddl:
        print("  Has DATA_TYPE parameter (checking default...)")
        if "DEFAULT" in ddl:
            idx = ddl.find("DATA_TYPE")
            print(ddl[idx:idx+100])
    
    # Check the JavaScript code
    if ".includes('revenue')" in ddl:
        print("  ✅ Has revenue check")
    else:
        print("  ❌ No revenue check")
        # Search for lowerDataType
        if 'lowerDataType' in ddl:
            idx = ddl.find('lowerDataType')
            print(f"\n  Found lowerDataType code:")
            print(ddl[idx:idx+300])
            
except Exception as e:
    print(f"  Error: {e}")

conn.close()
