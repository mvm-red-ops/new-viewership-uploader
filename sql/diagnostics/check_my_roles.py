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
print("MY ROLES AND PERMISSIONS")
print("=" * 80)

# Check current role
cursor.execute("SELECT CURRENT_ROLE()")
print(f"\nCurrent role: {cursor.fetchone()[0]}")

# Show all my roles
print("\nAll roles granted to me:")
cursor.execute("SHOW GRANTS TO USER NOSEY_APP")
results = cursor.fetchall()
for row in results:
    if 'ROLE' in str(row):
        print(f"  {row}")

# Try using web_app role
print("\nTrying to switch to WEB_APP role...")
try:
    cursor.execute("USE ROLE WEB_APP")
    print("✓ SUCCESS")

    cursor.execute("SELECT CURRENT_ROLE()")
    print(f"Current role now: {cursor.fetchone()[0]}")

    # Now try to create the table
    print("\nTrying to create TEMP_TUBI_UNMATCHED with WEB_APP role...")
    cursor.execute("""
        CREATE OR REPLACE TABLE UPLOAD_DB.PUBLIC.TEMP_TUBI_UNMATCHED AS
        SELECT DISTINCT id
        FROM test_staging.public.platform_viewership
        WHERE platform = 'Tubi'
        AND processed IS NULL
        AND content_provider IS NULL
        AND platform_content_name IS NOT NULL
        AND filename = 'tubi_vod_july.csv'
    """)
    print(f"✓ SUCCESS - Created table")

    cursor.execute("DROP TABLE UPLOAD_DB.PUBLIC.TEMP_TUBI_UNMATCHED")

except Exception as e:
    print(f"✗ FAILED: {e}")

cursor.close()
conn.close()
