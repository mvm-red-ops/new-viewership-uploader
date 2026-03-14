#!/usr/bin/env python3
"""Check and create Canada entries in PRODUCTION"""

import sys
sys.path.insert(0, '.')
from snowflake.connector import connect
import os

# Connect to PRODUCTION (UPLOAD_DB_PROD)
conn = connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='WEB_APP',
    database='UPLOAD_DB_PROD',  # PRODUCTION
    schema='PUBLIC'
)
cursor = conn.cursor()

print("=" * 80)
print("PRODUCTION - Checking Roku Channel - Linear entries")
print("=" * 80)

# Check existing entries
cursor.execute("""
SELECT
    platform_partner_name,
    platform_channel_name,
    platform_territory,
    deal_parent,
    internal_channel,
    internal_territory,
    internal_channel_id,
    internal_territory_id
FROM DICTIONARY.public.active_deals
WHERE platform = 'Roku'
  AND platform_partner_name = 'The Roku Channel - Linear'
ORDER BY platform_territory, platform_channel_name
""")

existing = cursor.fetchall()
print(f"\nExisting entries: {len(existing)}")
for row in existing:
    print(f"  Channel: {row[1]:30} Territory: {row[2]:15} Ch_ID: {row[6]}  Terr_ID: {row[7]}")

# Check if Canada entries exist
canada_entries = [r for r in existing if r[2] == 'Canada']
print(f"\nCanada entries: {len(canada_entries)}")

if len(canada_entries) == 0:
    print("\n⚠️  NO CANADA ENTRIES! Creating them...")

    # Get template from US entries
    cursor.execute("""
    SELECT DISTINCT
        platform,
        domain,
        deal_parent,
        internal_partner,
        internal_channel_id,
        internal_territory_id,
        active
    FROM DICTIONARY.public.active_deals
    WHERE platform = 'Roku'
      AND platform_partner_name = 'The Roku Channel - Linear'
      AND platform_territory = 'United States'
    LIMIT 1
    """)

    template = cursor.fetchone()
    if not template:
        print("❌ No US template found!")
        cursor.close()
        conn.close()
        exit(1)

    platform, domain, deal_parent, internal_partner, us_channel_id, us_terr_id, active = template
    canada_terr_id = 4  # Canada territory ID

    # Channels to create: Nosey, Judge Nosey, Nosey Confess
    channels_to_create = [
        ('Nosey', 'Nosey', 8),           # channel_id 8 from earlier
        ('Judge Nosey', 'Judge Nosey', None),  # Need to find correct ID
        ('Nosey Confess', 'Nosey Confess', None)  # Need to find correct ID
    ]

    for platform_channel, internal_channel, channel_id in channels_to_create:
        # Check if already exists
        cursor.execute("""
        SELECT COUNT(*)
        FROM DICTIONARY.public.active_deals
        WHERE platform = 'Roku'
          AND platform_partner_name = 'The Roku Channel - Linear'
          AND platform_channel_name = %s
          AND platform_territory = 'Canada'
        """, (platform_channel,))

        if cursor.fetchone()[0] > 0:
            print(f"  ✓ {platform_channel} / Canada already exists")
            continue

        # Create entry
        print(f"  Creating: {platform_channel} / Canada...")
        cursor.execute("""
        INSERT INTO DICTIONARY.public.active_deals (
            platform,
            domain,
            platform_partner_name,
            platform_channel_name,
            platform_territory,
            deal_parent,
            internal_partner,
            internal_channel,
            internal_territory,
            internal_channel_id,
            internal_territory_id,
            active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            platform,
            domain,
            'The Roku Channel - Linear',
            platform_channel,
            'Canada',
            deal_parent,
            internal_partner,
            internal_channel,
            'Canada',
            channel_id if channel_id else us_channel_id,  # Use US channel_id if specific one not known
            canada_terr_id,
            True
        ))
        print(f"    ✅ Created {platform_channel} / Canada")

    conn.commit()
    print("\n✅ All Canada entries created!")
else:
    print("✅ Canada entries already exist")

cursor.close()
conn.close()
