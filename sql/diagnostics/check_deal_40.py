import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("""
    SELECT
        deal_parent,
        platform_partner_name,
        platform_channel_name,
        platform_territory,
        internal_partner,
        internal_channel,
        internal_territory,
        domain
    FROM dictionary.public.active_deals
    WHERE deal_parent = 40
    AND platform = 'Tubi'
""")

print("Deal 40 (VOD) entries:")
for row in cursor.fetchall():
    print(f"  platform_partner: {row[1]}, platform_channel: {row[2]}, platform_territory: {row[3]}")
    print(f"  internal_partner: {row[4]}, internal_channel: {row[5]}, internal_territory: {row[6]}")
    print(f"  domain: {row[7]}")
    print()

cursor.close()
conn.close()
