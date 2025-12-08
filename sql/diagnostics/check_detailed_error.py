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
print("DETAILED ERROR FOR REF_ID_SERIES")
print("=" * 80)

cursor.execute("""
    SELECT log_time, log_message, error_message
    FROM upload_db.public.error_log_table
    WHERE procedure_name = 'process_viewership_ref_id_series_generic'
      AND platform = 'Tubi'
      AND status = 'ERROR'
    ORDER BY log_time DESC
    LIMIT 1
""")

result = cursor.fetchone()
if result:
    print(f"Time: {result[0]}")
    print(f"Message: {result[1]}")
    print(f"\nError Details:\n{result[2]}")
else:
    print("No detailed error found")

cursor.close()
conn.close()
