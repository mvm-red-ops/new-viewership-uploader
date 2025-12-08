import snowflake.connector
import yaml

# Load Snowflake secrets
with open('/Users/tayloryoung/work/nosey/nosey-tools/new-viewership-uploader/.streamlit/secrets.toml', 'r') as f:
    import toml
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
print("TABLES IN METADATA_MASTER_CLEANED_STAGING:")
print("=" * 80)
cursor.execute("""
    SHOW TABLES IN METADATA_MASTER_CLEANED_STAGING.PUBLIC
""")
tables = cursor.fetchall()
for table in tables:
    print(f"  {table[1]}")  # table name is usually in column 1

print("\n" + "=" * 80)
print("\nChecking reprocessing log:")
cursor.execute("""
    SELECT filename, COUNT(*) as unmatched_count
    FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY filename
""")

result = cursor.fetchone()
if result:
    print(f"Filename: {result[0]}, Unmatched records: {result[1]}")
else:
    print("No records in reprocessing log")

print("\n" + "=" * 80)
print("\nChecking test_staging for sample records:")
cursor.execute("""
    SELECT ref_id, asset_title, asset_series, content_provider, deal_parent
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 5
""")

results = cursor.fetchall()
print(f"Found {len(results)} records in test_staging")
for row in results:
    print(f"  ref_id: {row[0]} | title: {row[1][:50] if row[1] else None} | series: {row[2]} | provider: {row[3]} | deal_parent: {row[4]}")

print("\n" + "=" * 80)
print("\nChecking upload_db for comparison:")
cursor.execute("""
    SELECT ref_id, asset_title, asset_series, content_provider, deal_parent, partner, channel
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 5
""")

results = cursor.fetchall()
print(f"Found {len(results)} records in upload_db")
for row in results:
    print(f"  ref_id: {row[0]} | title: {row[1][:50] if row[1] else None} | partner: {row[5]} | channel: {row[6]} | deal_parent: {row[4]}")

cursor.close()
conn.close()
