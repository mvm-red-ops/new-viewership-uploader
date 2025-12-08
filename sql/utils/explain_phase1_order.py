import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

print("=" * 80)
print("UNDERSTANDING PHASE 1 EXECUTION ORDER")
print("=" * 80)

# Check the normalization procedure order
print("\n1. Phase 1 (Normalization) procedures run in this order:")
print("""
   a) SET_DEAL_PARENT_GENERIC
      - Sets: deal_parent, partner, channel, territory, channel_id, territory_id
      - This MUST run first!

   b) CALCULATE_VIEWERSHIP_METRICS
      - Calculates tot_mov from tot_hov (or vice versa)
      - Requires: tot_hov OR tot_mov to exist

   c) SET_DATE_COLUMNS_DYNAMIC
      - Sets: week, quarter, year, month, etc.

   d) SET_INTERNAL_SERIES_GENERIC
      - Matches platform_series to internal dictionary series names

   e) ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC (Phase 2)
      - Matches content to assets
      - Sets: ref_id, asset_series, content_provider
      - This depends on deal_parent being set!
""")

# Check the lambda logs for the actual execution
print("\n2. What happened in YOUR upload:")
cursor.execute("""
    SELECT
        log_time,
        procedure_name,
        log_message
    FROM upload_db.public.error_log_table
    WHERE log_message LIKE '%july_2025_daily_by_platform%'
    ORDER BY log_time ASC
    LIMIT 15
""")

print("\n   Execution timeline:")
for row in cursor.fetchall():
    print(f"   {row[0]} - {row[1]}")
    print(f"      {row[2]}")
    print()

# Check if tot_mov calculation was attempted
print("\n3. Checking if calculate_viewership_metrics ran:")
cursor.execute("""
    SELECT
        log_time,
        log_message
    FROM upload_db.public.error_log_table
    WHERE procedure_name = 'calculate_viewership_metrics'
      AND log_message LIKE '%july_2025%'
    ORDER BY log_time DESC
    LIMIT 3
""")

calc_logs = cursor.fetchall()
if calc_logs:
    print("   Yes, it ran:")
    for row in calc_logs:
        print(f"   {row[0]}: {row[1]}")
else:
    print("   ❌ NO - calculate_viewership_metrics did NOT run for this file!")
    print("   This is why tot_mov is still NULL!")

print("\n" + "=" * 80)
print("THE PROBLEM")
print("=" * 80)
print("""
When deal_parent is NULL (because active_deals had broken entries):

1. SET_DEAL_PARENT_GENERIC runs but matches 0 records ❌
   -> deal_parent stays NULL

2. Phase 1 continues anyway (no error checking)
   -> calculate_viewership_metrics might not run OR
   -> it runs but doesn't help

3. Phase 2 (asset matching) runs
   -> But content matching REQUIRES deal_parent!
   -> Why? Because content_provider lookup uses deal_parent to
      determine which content library to search

4. Without deal_parent:
   -> Content matching fails
   -> ref_id, asset_series, content_provider all stay NULL

5. Phase 3 final insert checks:
   WHERE deal_parent IS NOT NULL
     AND ref_id IS NOT NULL
     AND asset_series IS NOT NULL
     AND tot_mov IS NOT NULL
   -> ALL conditions fail, so 0 records inserted!
""")

cursor.close()
conn.close()
