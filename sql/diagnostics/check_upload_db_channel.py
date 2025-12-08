import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("""
    SELECT DISTINCT platform, channel, partner, deal_parent
    FROM upload_db.public.platform_viewership
    WHERE filename = 'tubi_vod_july.csv'
""")

print('Data in upload_db.public.platform_viewership:')
for row in cursor.fetchall():
    print(f'  platform: {row[0]}, channel: {row[1]}, partner: {row[2]}, deal_parent: {row[3]}')

cursor.close()
conn.close()
