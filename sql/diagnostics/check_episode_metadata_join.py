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
    database='METADATA_MASTER_CLEANED_STAGING',
    schema='PUBLIC'
)

cursor = conn.cursor()

print("=" * 80)
print("CHECKING EPISODE TABLE STRUCTURE")
print("=" * 80)

cursor.execute("""
    DESCRIBE TABLE metadata_master_cleaned_staging.public.episode
""")

results = cursor.fetchall()
print("\nEpisode table columns:")
for row in results:
    if 'metadata' in row[0].lower() or 'title' in row[0].lower() or 'ref' in row[0].lower() or 'id' in row[0].lower():
        print(f"  {row[0]}: {row[1]}")

print("\n" + "=" * 80)
print("SAMPLE EPISODE RECORD")
print("=" * 80)

cursor.execute("""
    SELECT e.ref_id, m.title, e.id, e.series_id, m.id as metadata_id
    FROM metadata_master_cleaned_staging.public.episode e
    JOIN metadata_master_cleaned_staging.public.metadata m ON (e.ref_id = m.ref_id)
    WHERE e.ref_id = 'MS-1132'
    LIMIT 1
""")

result = cursor.fetchone()
if result:
    print(f"ref_id: {result[0]}")
    print(f"title: {result[1]}")
    print(f"episode.id: {result[2]}")
    print(f"series_id: {result[3]}")
    print(f"metadata.id: {result[4]}")
else:
    print("No record found for MS-1132")

print("\n" + "=" * 80)
print("CHECKING METADATA TABLE")
print("=" * 80)

cursor.execute("""
    DESCRIBE TABLE metadata_master_cleaned_staging.public.metadata
""")

results = cursor.fetchall()
print("\nMetadata table columns:")
for row in results:
    if 'episode' in row[0].lower() or 'title' in row[0].lower() or 'ref' in row[0].lower() or 'id' in row[0].lower():
        print(f"  {row[0]}: {row[1]}")

cursor.close()
conn.close()
