import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECKING DEPLOYED PROCEDURES")
print("=" * 80)

# Check what procedures were just deployed
cursor.execute("SHOW PROCEDURES IN UPLOAD_DB.PUBLIC")
procedures = cursor.fetchall()

target_procs = [
    'VALIDATE_VIEWERSHIP_FOR_INSERT',
    'SET_DATE_COLUMNS_DYNAMIC',
    'PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC',
    'PROCESS_VIEWERSHIP_REF_ID_ONLY_GENERIC'
]

print("\nDeployed procedures:")
for proc in procedures:
    proc_name = proc[1]  # Procedure name
    for target in target_procs:
        if target in proc_name.upper():
            print(f"  ✅ {proc_name}")
            # Get the signature
            print(f"     Arguments: {proc[8]}")  # Arguments column
            break

# Now verify the content of one procedure to check database references
print("\n" + "=" * 80)
print("CHECKING SET_DATE_COLUMNS_DYNAMIC content:")
print("=" * 80)

cursor.execute("SELECT GET_DDL('PROCEDURE', 'UPLOAD_DB.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(STRING, STRING)')")
ddl = cursor.fetchone()[0]

if 'STAGING.public.platform_viewership' in ddl:
    print("✅ Uses STAGING database")
elif 'TEST_STAGING.public.platform_viewership' in ddl:
    print("❌ Still uses TEST_STAGING database")

if "'q1'" in ddl.lower():
    print("✅ Has lowercase quarter (q1, q2, q3, q4)")
    
if "'january'" in ddl.lower():
    print("✅ Has month name to number conversion")

cursor.close()
conn.close()
