import snowflake.connector
from config import load_snowflake_config

conn = snowflake.connector.connect(**load_snowflake_config())
cursor = conn.cursor()

cursor.execute("""
    SELECT
        platform_name,
        has_custom_channel_mapping,
        channel_procedure,
        deal_parent_procedure,
        deal_parent_columns
    FROM dictionary.public.platform_config
    WHERE UPPER(platform_name) = 'TUBI'
""")

row = cursor.fetchone()
if row:
    print('Tubi platform config:')
    print(f'  Platform: {row[0]}')
    print(f'  Has custom channel: {row[1]}')
    print(f'  Channel procedure: {row[2]}')
    print(f'  Deal parent procedure: {row[3]}')
    print(f'  Deal parent columns: {row[4]}')
else:
    print('No config found for Tubi')

cursor.close()
conn.close()
