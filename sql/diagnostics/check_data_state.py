import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECK DATA STATE")
print("=" * 80)

# Check test_staging
cursor.execute("""
    SELECT COUNT(*)
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
""")
count = cursor.fetchone()[0]
print(f"\nRecords in test_staging: {count}")

if count > 0:
    cursor.execute("""
        SELECT
            phase,
            COUNT(*) as cnt,
            COUNT(ref_id) as has_ref_id,
            COUNT(year_month_day) as has_ymd,
            COUNT(week) as has_week,
            COUNT(content_provider) as has_provider
        FROM test_staging.public.platform_viewership
        WHERE filename = 'tubi_vod_july.csv'
        GROUP BY phase
    """)

    print("\nBreakdown by phase:")
    for row in cursor.fetchall():
        print(f"  Phase {row[0]}: {row[1]} records")
        print(f"    has_ref_id: {row[2]}")
        print(f"    has_year_month_day: {row[3]}")
        print(f"    has_week: {row[4]}")
        print(f"    has_content_provider: {row[5]}")

cursor.close()
conn.close()
