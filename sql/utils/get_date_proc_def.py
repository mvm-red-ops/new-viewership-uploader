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

# Get the procedure definition
cursor.execute("""
    SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(VARCHAR, VARCHAR)')
""")

result = cursor.fetchone()
if result:
    print(result[0])
else:
    print("Procedure not found")

cursor.close()
conn.close()
