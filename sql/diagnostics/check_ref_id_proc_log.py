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
print("ERROR LOG FOR set_ref_id_from_platform_content_id")
print("=" * 80)

cursor.execute("""
    SELECT log_time, log_message, error_message
    FROM upload_db.public.error_log_table
    WHERE procedure_name = 'set_ref_id_from_platform_content_id'
       OR log_message LIKE '%ref_id%'
    ORDER BY log_time DESC
    LIMIT 10
""")

results = cursor.fetchall()
if results:
    for row in results:
        print(f"\n[{row[0]}]")
        print(f"  {row[1]}")
        if row[2]:
            print(f"  ERROR: {row[2]}")
else:
    print("\nNo log entries found - procedure may not have been called!")

print("\n" + "=" * 80)
print("CHECK: Does platform_content_id have data?")
print("=" * 80)

cursor.execute("""
    SELECT platform_content_id, COUNT(*)
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY platform_content_id
    LIMIT 5
""")

results = cursor.fetchall()
for row in results:
    print(f"  platform_content_id: {row[0]}, count: {row[1]}")

cursor.close()
conn.close()
