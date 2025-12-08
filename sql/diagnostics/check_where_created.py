import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Check current database/schema context
cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
result = cursor.fetchone()
print(f"Current context: {result[0]}.{result[1]}")

# Check where the procedure was created
for db in ['UPLOAD_DB', 'UPLOAD_DB_PROD']:
    print(f"\n{db}.PUBLIC:")
    try:
        cursor.execute(f"SHOW PROCEDURES LIKE 'SET_DATE_COLUMNS_DYNAMIC' IN {db}.PUBLIC")
        procs = cursor.fetchall()
        for proc in procs:
            print(f"  {proc[8]}")
            
            # Get the DDL and check for january
            params = proc[8].split('(')[1].split(')')[0]
            try:
                cursor.execute(f"SELECT GET_DDL('PROCEDURE', '{db}.PUBLIC.SET_DATE_COLUMNS_DYNAMIC({params})')")
                ddl = cursor.fetchone()[0]
                if 'january' in ddl.lower():
                    print(f"    ✅ Has month conversion")
                else:
                    print(f"    ❌ Missing month conversion")
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")

conn.close()
