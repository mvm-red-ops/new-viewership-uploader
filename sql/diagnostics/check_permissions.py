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
print("CHECKING PERMISSIONS")
print("=" * 80)

# Check current role
cursor.execute("SELECT CURRENT_ROLE()")
print(f"\nCurrent role: {cursor.fetchone()[0]}")

# Check current user
cursor.execute("SELECT CURRENT_USER()")
print(f"Current user: {cursor.fetchone()[0]}")

# Try to create a temp table
print("\nTrying to create a temp table in UPLOAD_DB.PUBLIC...")
try:
    cursor.execute("CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.TEST_TEMP_TABLE AS SELECT 1 as test_col")
    print("✓ SUCCESS: Can create temp tables")
    cursor.execute("DROP TABLE UPLOAD_DB.PUBLIC.TEST_TEMP_TABLE")
except Exception as e:
    print(f"✗ FAILED: {e}")

# Check grants on schema
print("\nChecking grants on UPLOAD_DB.PUBLIC schema...")
try:
    cursor.execute("SHOW GRANTS ON SCHEMA UPLOAD_DB.PUBLIC")
    results = cursor.fetchall()
    for row in results:
        print(f"  {row}")
except Exception as e:
    print(f"Error checking grants: {e}")

cursor.close()
conn.close()
