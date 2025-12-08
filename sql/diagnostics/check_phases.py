import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

# Check all phases
for phase in [None, '0', '1', '2', '3']:
    phase_filter = "phase IS NULL" if phase is None else f"phase = '{phase}'"
    cursor.execute(f"""
        SELECT COUNT(*),
               MIN(channel) as channel,
               MIN(partner) as partner,
               MIN(deal_parent) as deal_parent
        FROM test_staging.public.platform_viewership
        WHERE filename = 'tubi_vod_july.csv'
        AND {phase_filter}
    """)
    row = cursor.fetchone()
    if row[0] > 0:
        print(f"Phase {phase}: {row[0]} records")
        print(f"  channel: {row[1]}, partner: {row[2]}, deal_parent: {row[3]}")

cursor.close()
conn.close()
