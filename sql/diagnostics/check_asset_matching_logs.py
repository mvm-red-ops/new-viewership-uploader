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
print("ASSET MATCHING LOGS FOR TUBI")
print("=" * 80)

cursor.execute("""
    SELECT log_time, procedure_name, log_message, status, rows_affected
    FROM upload_db.public.error_log_table
    WHERE (procedure_name LIKE '%analyze%' OR procedure_name LIKE '%ref_id%')
      AND platform = 'Tubi'
    ORDER BY log_time DESC
    LIMIT 20
""")

results = cursor.fetchall()
if results:
    for row in results:
        print(f"\n[{row[0]}] {row[1]}")
        print(f"  Status: {row[3]}")
        print(f"  {row[2]}")
        if row[4]:
            print(f"  Rows: {row[4]}")
else:
    print("\nNo logs found")

print("\n" + "=" * 80)
print("CURRENT STATE OF TUBI DATA")
print("=" * 80)

cursor.execute("""
    SELECT
        phase,
        COUNT(*) as total,
        COUNT(CASE WHEN ref_id IS NOT NULL THEN 1 END) as has_ref_id,
        COUNT(CASE WHEN content_provider IS NOT NULL THEN 1 END) as has_provider,
        COUNT(CASE WHEN asset_series IS NOT NULL THEN 1 END) as has_series
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY phase
""")

results = cursor.fetchall()
for row in results:
    print(f"\nPhase {row[0]}: {row[1]} records")
    print(f"  Has ref_id: {row[2]}")
    print(f"  Has content_provider: {row[3]}")
    print(f"  Has asset_series: {row[4]}")

cursor.close()
conn.close()
