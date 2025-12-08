import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("COMPARING MATCHING LOGIC BY DOMAIN")
print("=" * 80)

# Check Owned and Operated deals
print("\n1. OWNED AND OPERATED deals (Zype):")
cursor.execute("""
    SELECT
        deal_parent,
        platform,
        domain,
        platform_partner_name,
        platform_channel_name,
        platform_territory
    FROM dictionary.public.active_deals
    WHERE domain = 'Owned and Operated'
    LIMIT 5
""")

for row in cursor.fetchall():
    print(f"   Platform: {row[1]}, Deal: {row[0]}")
    print(f"   Partner: {row[3]}, Channel: {row[4]}, Territory: {row[5]}")
    print(f"   -> Match on: platform + domain ONLY (ignores partner/channel/territory)")
    print()

# Check Distribution Partners deals
print("\n2. DISTRIBUTION PARTNERS deals (examples):")
cursor.execute("""
    SELECT
        deal_parent,
        platform,
        domain,
        platform_partner_name,
        platform_channel_name,
        platform_territory
    FROM dictionary.public.active_deals
    WHERE domain = 'Distribution Partners'
    LIMIT 5
""")

for row in cursor.fetchall():
    print(f"   Platform: {row[1]}, Deal: {row[0]}")
    print(f"   Partner: {row[3]}, Channel: {row[4]}, Territory: {row[5]}")
    has_specific = row[3] is not None or row[4] is not None or row[5] is not None
    if has_specific:
        print(f"   -> Match on: platform + domain + SPECIFIC partner/channel/territory")
    else:
        print(f"   -> Match on: platform + domain only")
    print()

print("=" * 80)
print("KEY DIFFERENCE")
print("=" * 80)
print("""
OWNED AND OPERATED (O&O):
- We set platform_partner_name = NULL
- We set platform_channel_name = NULL
- We set platform_territory = NULL
- Result: Matches ANY Zype data with domain='Owned and Operated'
- This is a "catch-all" for O&O content

DISTRIBUTION PARTNERS:
- Usually has SPECIFIC values for partner/channel/territory
- Example: platform_partner_name = 'Tubi', platform_channel_name = 'Nosey'
- Result: Only matches when the uploaded data has those EXACT values
- This ensures each distribution partner deal is matched precisely

WHY?
- O&O content belongs to us - we don't need to differentiate by partner
- Distribution Partners content needs precise matching to the right deal
  because each partner may have different terms/rates
""")

cursor.close()
conn.close()
