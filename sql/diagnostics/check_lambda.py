import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Just manually run the asset matching steps from test_full_end_to_end.py
print("Running asset matching buckets...")

# Set phase to 2 first
cursor.execute("""
    UPDATE test_staging.public.platform_viewership
    SET phase = '2'
    WHERE filename = 'tubi_vod_july.csv'
    AND platform = 'Tubi'
    AND processed IS NULL
""")
print(f"Set phase to 2 for {cursor.rowcount} records")

# Run the bucket procedures
buckets = [
    'FULL_DATA',
    'REF_ID_SERIES', 
    'REF_ID_ONLY',
    'SERIES_SEASON_EPISODE',
    'SERIES_ONLY',
    'TITLE_ONLY'
]

for bucket in buckets:
    proc_name = f'update_content_by_{bucket.lower()}_generic'
    try:
        cursor.execute(f"CALL upload_db.public.{proc_name}('Tubi', '{bucket}', 'tubi_vod_july.csv')")
        result = cursor.fetchone()[0]
        print(f"  {bucket}: {result}")
    except Exception as e:
        print(f"  {bucket}: Error - {e}")

# Check how many matched
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN content_provider IS NOT NULL THEN 1 ELSE 0 END) as matched
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
""")
row = cursor.fetchone()
print(f"\nMatching results: {row[1]}/{row[0]} records matched")

cursor.close()
conn.close()
