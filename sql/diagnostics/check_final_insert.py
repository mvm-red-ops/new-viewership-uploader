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
print("TEST_STAGING.PUBLIC.PLATFORM_VIEWERSHIP STATUS")
print("=" * 80)

cursor.execute("""
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN ref_id IS NOT NULL THEN 1 END) as has_ref_id,
        COUNT(CASE WHEN asset_series IS NOT NULL THEN 1 END) as has_asset_series,
        COUNT(CASE WHEN content_provider IS NOT NULL THEN 1 END) as has_content_provider,
        COUNT(CASE WHEN deal_parent IS NOT NULL THEN 1 END) as has_deal_parent,
        COUNT(CASE WHEN processed IS NULL THEN 1 END) as unprocessed,
        MAX(phase) as max_phase
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
""")

result = cursor.fetchone()
print(f"Total records: {result[0]}")
print(f"Has ref_id: {result[1]}")
print(f"Has asset_series: {result[2]}")
print(f"Has content_provider: {result[3]}")
print(f"Has deal_parent: {result[4]}")
print(f"Unprocessed: {result[5]}")
print(f"Max phase: {result[6]}")

print("\n" + "=" * 80)
print("STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(*)
    FROM staging_assets.public.episode_details_test_staging
    WHERE filename = 'tubi_vod_july.csv'
""")

count = cursor.fetchone()[0]
print(f"Records in final table: {count}")

print("\n" + "=" * 80)
print("REPROCESSING LOG")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(*)
    FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
    WHERE filename = 'tubi_vod_july.csv'
""")

unmatched = cursor.fetchone()[0]
print(f"Unmatched records: {unmatched}")

print("\n" + "=" * 80)
print("ERROR LOG FOR FINAL INSERT")
print("=" * 80)

cursor.execute("""
    SELECT log_time, log_message, error_message
    FROM upload_db.public.error_log_table
    WHERE procedure_name LIKE '%final%'
       OR log_message LIKE '%final%'
       OR log_message LIKE '%episode_details%'
    ORDER BY log_time DESC
    LIMIT 5
""")

results = cursor.fetchall()
if results:
    for row in results:
        print(f"\n[{row[0]}]")
        print(f"  {row[1]}")
        if row[2]:
            print(f"  ERROR: {row[2]}")
else:
    print("\nNo errors found")

cursor.close()
conn.close()
