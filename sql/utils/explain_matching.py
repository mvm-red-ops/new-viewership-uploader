import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("EXPLAINING THE MATCHING LOGIC")
print("=" * 80)

print("\n1. What's in active_deals for Zype O&O:")
cursor.execute("""
    SELECT
        deal_parent,
        platform,
        domain,
        platform_partner_name,
        platform_channel_name,
        platform_territory,
        active
    FROM dictionary.public.active_deals
    WHERE platform = 'Zype'
      AND domain = 'Owned and Operated'
""")

row = cursor.fetchone()
print(f"   Deal Parent: {row[0]}")
print(f"   Platform: {row[1]}")
print(f"   Domain: {row[2]}")
print(f"   Platform Partner Name: {row[3]} <- NULL means 'match any'")
print(f"   Platform Channel Name: {row[4]} <- NULL means 'match any'")
print(f"   Platform Territory: {row[5]} <- NULL means 'match any'")
print(f"   Active: {row[6]}")

print("\n2. What's in your uploaded data:")
cursor.execute("""
    SELECT DISTINCT
        platform,
        domain,
        platform_partner_name,
        platform_channel_name,
        platform_territory
    FROM test_staging.public.platform_viewership
    WHERE filename = 'july_2025_daily_by_platform.csv'
    LIMIT 3
""")

print("   Sample uploaded records:")
for row in cursor.fetchall():
    print(f"   Platform: {row[0]}")
    print(f"   Domain: {row[1]}")
    print(f"   Partner: {row[2]}")
    print(f"   Channel: {row[3]}")
    print(f"   Territory: {row[4]}")
    print()

print("=" * 80)
print("MATCHING LOGIC EXPLAINED")
print("=" * 80)
print("""
The SET_DEAL_PARENT_GENERIC procedure matches on:

WHERE ad.platform = v.platform              <- Must match: 'Zype' = 'Zype' ✓
  AND UPPER(ad.domain) = UPPER(v.domain)    <- Must match: 'Owned and Operated' = 'Owned and Operated' ✓
  AND ad.active = 'true'                     <- Must be active ✓
  AND (v.platform_partner_name IS NULL
       OR UPPER(v.platform_partner_name) = UPPER(ad.platform_partner_name))
       ^ This condition: Since ad.platform_partner_name IS NULL,
         the whole condition is TRUE regardless of what's in v.platform_partner_name
         So it IGNORES partner!

  AND (v.platform_channel_name IS NULL
       OR UPPER(v.platform_channel_name) = UPPER(ad.platform_channel_name))
       ^ Same here - ad.platform_channel_name IS NULL, so it IGNORES channel!

  AND (v.platform_territory IS NULL
       OR UPPER(v.platform_territory) = UPPER(ad.platform_territory))
       ^ Same here - ad.platform_territory IS NULL, so it IGNORES territory!

SUMMARY:
✓ Matches on: platform = 'Zype' AND domain = 'Owned and Operated'
✓ Ignores: partner, channel, territory (because they're NULL in active_deals)

This means ANY Zype record with domain='Owned and Operated' will match,
regardless of what partner/channel/territory values it has!
""")

cursor.close()
conn.close()
