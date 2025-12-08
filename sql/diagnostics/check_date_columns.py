import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECK DATE COLUMNS IN TEST_STAGING")
print("=" * 80)

cursor.execute("""
    SELECT
        filename,
        year_month_day,
        year,
        month,
        day,
        week,
        COUNT(*) as count
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY year_month_day, year, month, day, week, filename
    LIMIT 5
""")

print("\nDate columns in test_staging:")
for row in cursor.fetchall():
    print(f"  filename: {row[0]}")
    print(f"  year_month_day: {row[1]}")
    print(f"  year: {row[2]}")
    print(f"  month: {row[3]}")
    print(f"  day: {row[4]}")
    print(f"  week: {row[5]}")
    print(f"  count: {row[6]}")
    print()

print("=" * 80)
print("CHECK ORIGINAL UPLOADED DATA")
print("=" * 80)

cursor.execute("""
    SELECT TOP 3
        filename,
        original_date,
        year_month_day,
        year,
        month
    FROM upload_db.public.platform_viewership_uploads
    WHERE filename = 'tubi_vod_july.csv'
""")

print("\nOriginal upload data:")
for row in cursor.fetchall():
    print(f"  filename: {row[0]}")
    print(f"  original_date: {row[1]}")
    print(f"  year_month_day: {row[2]}")
    print(f"  year: {row[3]}")
    print(f"  month: {row[4]}")
    print()

cursor.close()
conn.close()
