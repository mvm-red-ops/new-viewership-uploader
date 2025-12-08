import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(STRING, STRING, STRING)')")
ddl = cursor.fetchone()[0]

# Find where the validation query building happens
idx = ddl.find('lowerDataType')
if idx > 0:
    # Show 2000 characters starting from lowerDataType
    snippet = ddl[idx:idx+2000]
    print("Code around lowerDataType:")
    print("="*80)
    print(snippet)
    print("="*80)
    
    # Check if revenue check exists anywhere
    if "includes('revenue')" in ddl:
        print("\n✅ Revenue check EXISTS in full DDL")
    else:
        print("\n❌ Revenue check does NOT exist anywhere in DDL")
        
    # Count occurrences of certain strings
    print(f"\nOccurrences:")
    print(f"  'revenue': {ddl.count('revenue')}")
    print(f"  'includes': {ddl.count('includes')}")
    print(f"  'lowerDataType': {ddl.count('lowerDataType')}")

conn.close()
