import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(STRING, STRING)')")
ddl = cursor.fetchone()[0]

print("Checking SET_DATE_COLUMNS_DYNAMIC features:\n")

# Check for month conversion
if "''january''" in ddl.lower():
    print("✅ Has month name conversion")
    # Show snippet
    idx = ddl.lower().find("''january''")
    print(f"   Found at position {idx}")
else:
    print("❌ Missing month name conversion")
    
# Check for lowercase quarters  
if "''q1''" in ddl:
    print("✅ Has lowercase quarters")
else:
    print("❌ Missing lowercase quarters")
    # Check if has uppercase
    if "''Q1''" in ddl:
        print("   (Has uppercase Q1 instead)")
    
# Check for quarter CASE statement
if 'CASE' in ddl and ('month AS INT' in ddl or 'CAST(month AS INT)' in ddl):
    print("✅ Has quarter calculation CASE statement")
else:
    print("❌ Missing quarter CASE statement")
    
# Check if it handles month/year (no date column)
if 'hasDateColumn' in ddl:
    print("✅ Has logic to handle files with month/year only (no date column)")
else:
    print("❌ Missing month/year handling logic")

conn.close()
