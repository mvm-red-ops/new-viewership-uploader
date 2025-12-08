import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECKING NORMALIZATION LOGS")
print("=" * 80)

# Check error logs for this file
print("\n1. Checking error logs for july_2025_daily_by_platform.csv...")
cursor.execute("""
    SELECT
        log_time,
        procedure_name,
        log_message,
        status,
        error_message
    FROM upload_db.public.error_log_table
    WHERE log_message LIKE '%july_2025_daily_by_platform%'
       OR procedure_name LIKE '%deal_parent%'
    ORDER BY log_time DESC
    LIMIT 20
""")

logs = cursor.fetchall()
if logs:
    for row in logs:
        print(f"\n   Time: {row[0]}")
        print(f"   Procedure: {row[1]}")
        print(f"   Message: {row[2]}")
        print(f"   Status: {row[3]}")
        if row[4]:
            print(f"   Error: {row[4]}")
        print("   " + "-" * 76)
else:
    print("   No logs found")

# Check if deal matching worked for ANY Zype records ever
print("\n2. Checking if Zype deal matching has EVER worked...")
cursor.execute("""
    SELECT COUNT(*) as count
    FROM test_staging.public.platform_viewership
    WHERE platform = 'Zype'
      AND deal_parent IS NOT NULL
""")

row = cursor.fetchone()
print(f"   Zype records with deal_parent set: {row[0]}")

# Check active_deals to ensure our entry exists
print("\n3. Verifying active_deals entry...")
cursor.execute("""
    SELECT
        deal_parent,
        platform,
        domain,
        internal_channel,
        internal_territory,
        active
    FROM dictionary.public.active_deals
    WHERE platform = 'Zype'
      AND domain = 'Owned and Operated'
""")

deals = cursor.fetchall()
print(f"   Found {len(deals)} matching active_deals:")
for row in deals:
    print(f"     Deal Parent: {row[0]}")
    print(f"     Platform: {row[1]}")
    print(f"     Domain: {row[2]}")
    print(f"     Channel: {row[3]}")
    print(f"     Territory: {row[4]}")
    print(f"     Active: {row[5]}")

# Test the matching query manually
print("\n4. Testing deal matching query manually...")
cursor.execute("""
    SELECT
        v.id,
        v.platform,
        v.domain,
        v.platform_partner_name,
        v.platform_channel_name,
        v.platform_territory,
        ad.deal_parent,
        ad.internal_channel,
        ad.internal_territory
    FROM test_staging.public.platform_viewership v
    LEFT JOIN dictionary.public.active_deals ad
        ON ad.platform = v.platform
        AND UPPER(ad.domain) = UPPER(v.domain)
        AND ad.active = 'true'
        AND (v.platform_partner_name IS NULL OR UPPER(v.platform_partner_name) = UPPER(ad.platform_partner_name))
        AND (v.platform_channel_name IS NULL OR UPPER(v.platform_channel_name) = UPPER(ad.platform_channel_name))
        AND (v.platform_territory IS NULL OR UPPER(v.platform_territory) = UPPER(ad.platform_territory))
    WHERE v.filename = 'july_2025_daily_by_platform.csv'
    LIMIT 3
""")

print("\n   Manual join results:")
for row in cursor.fetchall():
    print(f"   Record ID: {row[0]}")
    print(f"   Platform: {row[1]}, Domain: {row[2]}")
    print(f"   Partner: {row[3]}, Channel: {row[4]}, Territory: {row[5]}")
    print(f"   Matched Deal: {row[6]}, Channel: {row[7]}, Territory: {row[8]}")
    print()

cursor.close()
conn.close()
