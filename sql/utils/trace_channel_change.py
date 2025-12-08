import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("TRACE: Where does channel change from VOD to Nosey?")
print("=" * 80)

# Step 1: Check upload_db BEFORE any processing
cursor.execute("""
    SELECT 
        channel,
        partner,
        platform_channel_name,
        platform_partner_name,
        deal_parent
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 1
""")
row = cursor.fetchone()
print("\n1. upload_db.public.platform_viewership (initial upload):")
print(f"   channel: {row[0]}, partner: {row[1]}")
print(f"   platform_channel_name: {row[2]}, platform_partner_name: {row[3]}")
print(f"   deal_parent: {row[4]}")

# Step 2: Check test_staging AFTER move_streamlit_data_to_staging
cursor.execute("""
    SELECT 
        channel,
        partner,
        platform_channel_name,
        platform_partner_name,
        deal_parent,
        phase
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 1
""")
row = cursor.fetchone()
print("\n2. test_staging.public.platform_viewership (after move):")
print(f"   channel: {row[0]}, partner: {row[1]}")
print(f"   platform_channel_name: {row[2]}, platform_partner_name: {row[3]}")
print(f"   deal_parent: {row[4]}, phase: {row[5]}")

print("\n" + "=" * 80)
print("CONCLUSION:")
if row[0] != 'VOD':
    print("❌ Channel changed during move_streamlit_data_to_staging!")
    print("   Need to check that procedure.")
else:
    print("✓ Channel was still VOD after move.")
    print("   Change must happen during normalization (SET_DEAL_PARENT_GENERIC)")

cursor.close()
conn.close()
