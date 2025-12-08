import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Check what SET_DEAL_PARENT_GENERIC does
cursor.execute("SHOW PROCEDURES LIKE 'SET_DEAL_PARENT_GENERIC' IN UPLOAD_DB.PUBLIC")
rows = cursor.fetchall()
if rows:
    print("SET_DEAL_PARENT_GENERIC procedure exists")
    # Get the procedure definition
    cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.SET_DEAL_PARENT_GENERIC(STRING, STRING)')")
    ddl = cursor.fetchone()[0]
    print("\nProcedure definition:")
    print(ddl[:2000])  # First 2000 chars
else:
    print("SET_DEAL_PARENT_GENERIC procedure not found")

cursor.close()
conn.close()
