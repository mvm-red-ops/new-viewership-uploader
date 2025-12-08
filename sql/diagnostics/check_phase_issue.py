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
print("CHECKING TEST_STAGING DATA")
print("=" * 80)

cursor.execute("""
    SELECT phase, processed, COUNT(*) as cnt
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY phase, processed
""")

results = cursor.fetchall()
if results:
    print("\nRecords in test_staging:")
    for row in results:
        print(f"  phase: {row[0]}, processed: {row[1]}, count: {row[2]}")
else:
    print("\nNo records found in test_staging!")

print("\n" + "=" * 80)
print("CHECKING UPLOAD_DB DATA")
print("=" * 80)

cursor.execute("""
    SELECT phase, processed, COUNT(*) as cnt
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY phase, processed
""")

results = cursor.fetchall()
if results:
    print("\nRecords in upload_db:")
    for row in results:
        print(f"  phase: {row[0]}, processed: {row[1]}, count: {row[2]}")
else:
    print("\nNo records found in upload_db!")

cursor.close()
conn.close()
