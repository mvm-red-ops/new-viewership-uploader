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
print("LATEST ASSET MATCHING LOGS")
print("=" * 80)

cursor.execute("""
    SELECT log_time, procedure_name, log_message, status, rows_affected, error_message
    FROM upload_db.public.error_log_table
    WHERE log_time > DATEADD(minute, -5, CURRENT_TIMESTAMP())
      AND (procedure_name LIKE '%ref_id%' OR procedure_name LIKE '%analyze%')
    ORDER BY log_time DESC
    LIMIT 30
""")

results = cursor.fetchall()
for row in results:
    print(f"\n[{row[0]}] {row[1]} - {row[3]}")
    print(f"  {row[2]}")
    if row[4]:
        print(f"  Rows: {row[4]}")
    if row[5]:
        print(f"  ERROR: {row[5]}")

print("\n" + "=" * 80)
print("CHECK: What's in test_staging now?")
print("=" * 80)

cursor.execute("""
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN ref_id IS NOT NULL THEN 1 END) as has_ref_id,
        COUNT(CASE WHEN internal_series IS NOT NULL THEN 1 END) as has_internal_series,
        COUNT(CASE WHEN content_provider IS NOT NULL THEN 1 END) as has_provider,
        COUNT(CASE WHEN asset_series IS NOT NULL THEN 1 END) as has_asset_series
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
""")

result = cursor.fetchone()
print(f"\nTotal: {result[0]}")
print(f"Has ref_id: {result[1]}")
print(f"Has internal_series: {result[2]}")
print(f"Has content_provider: {result[3]}")
print(f"Has asset_series: {result[4]}")

cursor.close()
conn.close()
