import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("LATEST ERROR LOGS")
print("=" * 80)

cursor.execute("""
    SELECT log_time, procedure_name, log_message, status, error_message
    FROM upload_db.public.error_log_table
    WHERE status IN ('ERROR', 'FAILED')
    ORDER BY log_time DESC
    LIMIT 10
""")

for row in cursor.fetchall():
    print(f"\n[{row[0]}] {row[1]} - {row[3]}")
    print(f"  Message: {row[2]}")
    if row[4]:
        print(f"  Error: {row[4]}")

print("\n" + "=" * 80)
print("CHECK CURRENT DATA STATE")
print("=" * 80)

cursor.execute("""
    SELECT
        phase,
        COUNT(*) as cnt,
        COUNT(ref_id) as has_ref_id,
        COUNT(year_month_day) as has_ymd,
        COUNT(quarter) as has_quarter,
        COUNT(content_provider) as has_provider
    FROM test_staging.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
    GROUP BY phase
""")

print("\nData by phase:")
for row in cursor.fetchall():
    print(f"  Phase {row[0]}: {row[1]} records")
    print(f"    ref_id: {row[2]}, ymd: {row[3]}, quarter: {row[4]}, provider: {row[5]}")

cursor.close()
conn.close()
