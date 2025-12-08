import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECK DEAL MATCHING")
print("=" * 80)

# Check what's in test_staging
cursor.execute("""
    SELECT DISTINCT
        platform,
        channel,
        deal_parent,
        partner,
        platform_partner_name,
        platform_channel_name,
        domain
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 5
""")

print("\nData in test_staging:")
for row in cursor.fetchall():
    print(f"  platform: {row[0]}, channel: {row[1]}, deal_parent: {row[2]}, partner: {row[3]}")
    print(f"  platform_partner_name: {row[4]}, platform_channel_name: {row[5]}, domain: {row[6]}")

# Check what's in upload_db before normalization
cursor.execute("""
    SELECT DISTINCT
        platform,
        channel,
        deal_parent,
        partner,
        platform_partner_name,
        platform_channel_name,
        domain
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    LIMIT 5
""")

print("\nData in upload_db (before normalization):")
for row in cursor.fetchall():
    print(f"  platform: {row[0]}, channel: {row[1]}, deal_parent: {row[2]}, partner: {row[3]}")
    print(f"  platform_partner_name: {row[4]}, platform_channel_name: {row[5]}, domain: {row[6]}")

# Check what deals exist that could match
cursor.execute("""
    SELECT
        deal_parent,
        platform,
        platform_partner_name,
        platform_channel_name,
        platform_territory,
        internal_partner,
        internal_channel,
        internal_territory,
        domain,
        active
    FROM dictionary.public.active_deals
    WHERE platform = 'Tubi'
    AND active = true
""")

print("\nActive deals for Tubi:")
for row in cursor.fetchall():
    print(f"  Deal: {row[0]}, Platform: {row[1]}")
    print(f"  platform_partner_name: {row[2]}, platform_channel_name: {row[3]}, platform_territory: {row[4]}")
    print(f"  internal_partner: {row[5]}, internal_channel: {row[6]}, internal_territory: {row[7]}")
    print(f"  domain: {row[8]}, active: {row[9]}")
    print()

cursor.close()
conn.close()
