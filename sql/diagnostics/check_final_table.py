import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECK FINAL TABLE")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(*)
    FROM staging_assets.public.episode_details_test_staging
    WHERE filename = 'tubi_vod_july.csv'
    AND label = 'Revenue'
""")

count = cursor.fetchone()[0]
print(f"\nRevenue records in final table: {count}")

if count > 0:
    cursor.execute("""
        SELECT
            ref_id,
            asset_series,
            content_provider,
            revenue,
            year,
            quarter,
            year_month_day,
            week,
            day
        FROM staging_assets.public.episode_details_test_staging
        WHERE filename = 'tubi_vod_july.csv'
        AND label = 'Revenue'
        LIMIT 3
    """)

    print("\nSample records:")
    for row in cursor.fetchall():
        print(f"\n  ref_id: {row[0]}")
        print(f"  asset_series: {row[1]}")
        print(f"  content_provider: {row[2]}")
        print(f"  revenue: {row[3]}")
        print(f"  year: {row[4]}")
        print(f"  quarter: {row[5]}")
        print(f"  year_month_day: {row[6]}")
        print(f"  week: {row[7]}")
        print(f"  day: {row[8]}")

cursor.close()
conn.close()
