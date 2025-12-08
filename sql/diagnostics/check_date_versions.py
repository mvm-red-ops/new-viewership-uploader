import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("SET_DATE_COLUMNS_DYNAMIC versions in UPLOAD_DB_PROD:")
cursor.execute("SHOW PROCEDURES LIKE 'SET_DATE_COLUMNS_DYNAMIC' IN UPLOAD_DB_PROD.PUBLIC")
for proc in cursor.fetchall():
    print(f"  {proc[8]}")

# Check the 2-parameter version
print("\nChecking (STRING, STRING) version...")
cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB_PROD.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(STRING, STRING)')")
ddl = cursor.fetchone()[0]

# Look for month conversion code
if 'january' in ddl.lower():
    idx = ddl.lower().find('january')
    print(f"Found 'january' at position {idx}")
    print("Snippet:")
    print(ddl[max(0, idx-100):idx+200])
else:
    print("'january' not found anywhere in procedure")
    
# Check what the procedure actually does
if 'SET full_date' in ddl:
    print("\nThis appears to be the OLD version (uses full_date)")
elif 'hasDateColumn' in ddl:
    print("\nThis appears to be the NEW version (checks hasDateColumn)")
else:
    print("\nCannot determine version")

conn.close()
