import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("SHOW PROCEDURES IN UPLOAD_DB.PUBLIC")
procs = cursor.fetchall()
matching_procs = [p for p in procs if 'MATCH' in p[1].upper() and 'CONTENT' in p[1].upper()]
print("Matching procedures:")
for p in matching_procs:
    print(f"  {p[1]}")

cursor.close()
conn.close()
