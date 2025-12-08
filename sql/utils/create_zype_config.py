import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CREATING ZYPE OWNED AND OPERATED CONFIGURATION")
print("=" * 80)

# Step 1: Create entry in dictionary.public.platforms (note: plural)
print("\n1. Creating entry in dictionary.public.platforms...")
try:
    cursor.execute("""
        INSERT INTO dictionary.public.platforms (name, active)
        VALUES ('Zype', TRUE)
    """)
    print("   ✓ Platform entry created")
except Exception as e:
    if "Duplicate key" in str(e) or "already exists" in str(e) or "Duplicate" in str(e):
        print("   ℹ Platform 'Zype' already exists, skipping...")
    else:
        print(f"   ✗ Error: {e}")

# Step 2: Create entry in dictionary.public.deals
# Note: deals table doesn't have domain column - that's only in active_deals
# channelids is VARIANT type, use SELECT with PARSE_JSON
print("\n2. Creating entry in dictionary.public.deals...")
try:
    cursor.execute("""
        INSERT INTO dictionary.public.deals (
            partner,
            platform,
            channelids,
            territory_ids,
            viewership_type
        )
        SELECT
            'Nosey',
            'Zype',
            PARSE_JSON('["11"]'),
            '1',
            'VOD'
    """)
    print("   ✓ Deal entry created")
except Exception as e:
    print(f"   Error: {e}")
    # If it fails, try to continue anyway

# Step 3: Get the generated deal ID
print("\n3. Getting deal ID...")
cursor.execute("""
    SELECT id, partner, platform, channelids, territory_ids, viewership_type
    FROM dictionary.public.deals
    WHERE platform = 'Zype'
    ORDER BY id DESC
    LIMIT 1
""")
deal_row = cursor.fetchone()
deal_id = deal_row[0]
print(f"   ✓ Deal ID: {deal_id}")
print(f"     Partner: {deal_row[1]}")
print(f"     Platform: {deal_row[2]}")
print(f"     Channel IDs: {deal_row[3]}")
print(f"     Territory IDs: {deal_row[4]}")
print(f"     Viewership Type: {deal_row[5]}")

# Step 4: Create entry in dictionary.public.active_deals
print(f"\n4. Creating entry in dictionary.public.active_deals (deal_parent={deal_id})...")
cursor.execute("""
    INSERT INTO dictionary.public.active_deals (
        deal_parent,
        platform,
        domain,
        platform_partner_name,
        platform_channel_name,
        platform_territory,
        internal_partner,
        internal_channel,
        internal_territory,
        internal_channel_id,
        internal_territory_id,
        active
    ) VALUES (
        %s,
        'Zype',
        'Owned and Operated',
        NULL,
        NULL,
        NULL,
        NULL,
        'VOD',
        'US',
        11,
        1,
        TRUE
    )
""", (deal_id,))
print("   ✓ Active deal entry created")

# Verify the configuration
print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

print("\n✓ Platform:")
cursor.execute("SELECT id, name, active FROM dictionary.public.platforms WHERE name = 'Zype'")
for row in cursor.fetchall():
    print(f"  ID: {row[0]}, Name: {row[1]}, Active: {row[2]}")

print("\n✓ Deals:")
cursor.execute("""
    SELECT id, partner, platform, viewership_type
    FROM dictionary.public.deals
    WHERE platform = 'Zype'
""")
for row in cursor.fetchall():
    print(f"  ID: {row[0]}")
    print(f"  Partner: {row[1]}")
    print(f"  Platform: {row[2]}")
    print(f"  Viewership Type: {row[3]}")

print("\n✓ Active Deals:")
cursor.execute("""
    SELECT deal_parent, platform, domain, internal_channel, internal_territory, active
    FROM dictionary.public.active_deals
    WHERE platform = 'Zype' AND domain = 'Owned and Operated'
""")
for row in cursor.fetchall():
    print(f"  Deal Parent: {row[0]}")
    print(f"  Platform: {row[1]}")
    print(f"  Domain: {row[2]}")
    print(f"  Internal Channel: {row[3]}")
    print(f"  Internal Territory: {row[4]}")
    print(f"  Active: {row[5]}")

# Commit changes
conn.commit()
print("\n" + "=" * 80)
print("✓ ALL CHANGES COMMITTED SUCCESSFULLY")
print("=" * 80)

cursor.close()
conn.close()
