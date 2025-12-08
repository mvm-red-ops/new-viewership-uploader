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
print("ALL COLUMNS WITH DATA FROM ONE TUBI RECORD:")
print("=" * 80)

cursor.execute("""
    SELECT *
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 1
""")

# Get column names
columns = [desc[0] for desc in cursor.description]
row = cursor.fetchone()

# Print all non-null columns
for i, col in enumerate(columns):
    value = row[i]
    if value is not None:
        print(f"  {col}: {value}")

cursor.close()
conn.close()
