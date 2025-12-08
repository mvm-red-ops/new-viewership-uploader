import snowflake.connector
import toml

# Load Snowflake secrets
with open('/Users/tayloryoung/work/nosey/nosey-tools/new-viewership-uploader/.streamlit/secrets.toml', 'r') as f:
    secrets = toml.load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=secrets['snowflake']['user'],
    password=secrets['snowflake']['password'],
    account=secrets['snowflake']['account'],
    warehouse=secrets['snowflake']['warehouse'],
    database='UPLOAD_DB',
    schema='PUBLIC'
)

cursor = conn.cursor()

print("=" * 80)
print("RECENT ERROR LOG ENTRIES")
print("=" * 80)

cursor.execute("""
    SELECT log_time, procedure_name, log_message, error_message, query_text
    FROM upload_db.public.error_log_table
    WHERE log_time > DATEADD(minute, -10, CURRENT_TIMESTAMP())
    ORDER BY log_time DESC
    LIMIT 20
""")

results = cursor.fetchall()
if results:
    for row in results:
        print(f"\n[{row[0]}] {row[1]}")
        print(f"  Message: {row[2]}")
        if row[3]:
            print(f"  Error: {row[3]}")
        if row[4]:
            print(f"  Query: {row[4][:200]}")
else:
    print("\nNo recent errors")

cursor.close()
conn.close()
