import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("CHECKING ZYPE TEMPLATE AND NORMALIZATION ISSUES")
print("=" * 80)

# 1. Check the Zype template
print("\n1. Checking Zype template in viewership_templates:")
cursor.execute("""
    SELECT *
    FROM dictionary.public.viewership_templates
    WHERE platform = 'Zype'
""")

columns = [desc[0] for desc in cursor.description]
row = cursor.fetchone()
if row:
    for i, col in enumerate(columns):
        print(f"   {col}: {row[i]}")
else:
    print("   No Zype template found!")

# 2. Check current data state
print("\n2. Current data state after deal_parent was set:")
cursor.execute("""
    SELECT
        deal_parent,
        partner,
        channel,
        territory,
        tot_mov,
        tot_hov,
        week,
        quarter,
        year,
        month,
        year_month_day
    FROM test_staging.public.platform_viewership
    WHERE filename = 'july_2025_daily_by_platform.csv'
    LIMIT 3
""")

print("   Sample records:")
for row in cursor.fetchall():
    print(f"   Deal: {row[0]}, Partner: {row[1]}, Channel: {row[2]}, Territory: {row[3]}")
    print(f"   TOT_MOV: {row[4]}, TOT_HOV: {row[5]}")
    print(f"   Week: {row[6]}, Quarter: {row[7]}, Year: {row[8]}, Month: {row[9]}, YMD: {row[10]}")
    print()

# 3. Check normalization procedure logs
print("\n3. Checking what normalization procedures ran:")
cursor.execute("""
    SELECT log_time, procedure_name, log_message
    FROM upload_db.public.error_log_table
    WHERE log_time > DATEADD(minute, -10, CURRENT_TIMESTAMP())
      AND (procedure_name LIKE '%date%'
           OR procedure_name LIKE '%calculate%'
           OR procedure_name LIKE '%metric%')
    ORDER BY log_time DESC
    LIMIT 10
""")

logs = cursor.fetchall()
if logs:
    print("   Recent date/metric procedures:")
    for row in logs:
        print(f"   {row[0]} - {row[1]}")
        print(f"      {row[2]}")
else:
    print("   No date/metric procedure logs found!")

# 4. Check platform_config for Zype
print("\n4. Checking Zype platform_config:")
cursor.execute("""
    SELECT
        platform_name,
        has_custom_date_handling,
        date_handling_procedure,
        additional_normalizers
    FROM dictionary.public.platform_config
    WHERE platform_name = 'Zype'
""")

row = cursor.fetchone()
if row:
    print(f"   Platform: {row[0]}")
    print(f"   Custom date handling: {row[1]}")
    print(f"   Date procedure: {row[2]}")
    print(f"   Additional normalizers: {row[3]}")
else:
    print("   No platform_config for Zype!")

cursor.close()
conn.close()
