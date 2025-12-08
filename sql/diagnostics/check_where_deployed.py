import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("Checking where procedures were deployed...\n")

for db in ['UPLOAD_DB', 'UPLOAD_DB_PROD']:
    print(f"\n{db}.PUBLIC:")
    try:
        cursor.execute(f"SHOW PROCEDURES IN {db}.PUBLIC")
        procs = cursor.fetchall()
        
        target_names = ['VALIDATE_VIEWERSHIP_FOR_INSERT', 'SET_DATE_COLUMNS_DYNAMIC', 
                       'PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC', 'PROCESS_VIEWERSHIP_REF_ID_ONLY_GENERIC']
        
        for proc in procs:
            proc_name = proc[1]
            for target in target_names:
                if target in proc_name:
                    print(f"  ✅ {proc_name}")
                    break
    except Exception as e:
        print(f"  ❌ Error: {e}")

conn.close()
