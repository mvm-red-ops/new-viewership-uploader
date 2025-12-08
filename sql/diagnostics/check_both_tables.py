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
print("UPLOAD_DB.PUBLIC.PLATFORM_VIEWERSHIP")
print("=" * 80)

cursor.execute("""
    SELECT deal_parent, month, filename, year_month_day, ref_id, phase, processed, COUNT(*)
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY ALL
""")

results = cursor.fetchall()
for row in results:
    print(f"deal_parent: {row[0]}, month: {row[1]}, ref_id: {row[4]}, phase: {row[5]}, processed: {row[6]}, count: {row[7]}")

print("\n" + "=" * 80)
print("TEST_STAGING.PUBLIC.PLATFORM_VIEWERSHIP")
print("=" * 80)

cursor.execute("""
    SELECT deal_parent, month, filename, year_month_day, ref_id, phase, processed, COUNT(*)
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY ALL
""")

results = cursor.fetchall()
if results:
    for row in results:
        print(f"deal_parent: {row[0]}, month: {row[1]}, ref_id: {row[4]}, phase: {row[5]}, processed: {row[6]}, count: {row[7]}")
else:
    print("NO DATA IN TEST_STAGING - This is the problem!")

cursor.close()
conn.close()
