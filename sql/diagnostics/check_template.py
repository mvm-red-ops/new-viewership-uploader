import snowflake.connector
import toml
import json

# Load Snowflake secrets
with open('/Users/tayloryoung/work/nosey/nosey-tools/new-viewership-uploader/.streamlit/secrets.toml', 'r') as f:
    secrets = toml.load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=secrets['snowflake']['user'],
    password=secrets['snowflake']['password'],
    account=secrets['snowflake']['account'],
    warehouse=secrets['snowflake']['warehouse'],
    database='DICTIONARY',
    schema='PUBLIC'
)

cursor = conn.cursor()

cursor.execute("""
    SELECT platform, data_type, column_mappings
    FROM dictionary.public.viewership_file_formats
    WHERE platform = 'Tubi' AND data_type = 'Revenue-by-Episode'
""")

result = cursor.fetchone()
if result:
    platform, data_type, mappings_json = result
    mappings = json.loads(mappings_json)

    print(f"Platform: {platform}, Data Type: {data_type}")
    print("\nColumn Mappings:")
    print("=" * 80)

    # Look for ref_id and asset-related mappings
    for col, mapping in mappings.items():
        if 'ref' in col.lower() or 'asset' in col.lower() or 'content' in col.lower() or 'series' in col.lower():
            print(f"{col}: {mapping}")

cursor.close()
conn.close()
