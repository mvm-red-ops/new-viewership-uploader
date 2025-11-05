"""Check database schema and table structure"""

def check_schema(cursor, env_config):
    """Check if required tables and columns exist"""
    print("=" * 80)
    print("Schema Checks")
    print("=" * 80)
    print()

    assets_db = env_config['ASSETS_DB']
    episode_details_table = env_config['EPISODE_DETAILS_TABLE']

    # Check if EPISODE_DETAILS table exists
    print(f"Checking {assets_db}.PUBLIC.{episode_details_table}...")
    cursor.execute(f"""
        SHOW TABLES LIKE '{episode_details_table}' IN {assets_db}.PUBLIC
    """)

    tables = cursor.fetchall()
    if not tables:
        print(f"  ❌ Table {episode_details_table} NOT FOUND")
        return False

    print(f"  ✅ Table exists")

    # Check for required columns
    required_columns = [
        'PLATFORM_PARTNER_NAME',
        'PLATFORM_CHANNEL_NAME',
        'PLATFORM_TERRITORY',
        'START_TIME',
        'END_TIME',
        'TOT_COMPLETIONS'
    ]

    cursor.execute(f"""
        DESC TABLE {assets_db}.PUBLIC.{episode_details_table}
    """)
    columns = cursor.fetchall()
    existing_columns = [col[0] for col in columns]

    missing_columns = []
    for col in required_columns:
        if col in existing_columns:
            print(f"  ✅ Column {col} exists")
        else:
            print(f"  ❌ Column {col} MISSING")
            missing_columns.append(col)

    if missing_columns:
        print()
        print("  💡 Deploy schema updates: python sql/deploy/deploy.py --env staging --only 001_schema_tables")
        return False

    # Check record_reprocessing_batch_logs table permissions
    metadata_db = env_config['METADATA_DB']
    print()
    print(f"Checking {metadata_db}.PUBLIC.record_reprocessing_batch_logs permissions...")

    try:
        cursor.execute(f"""
            SHOW GRANTS ON TABLE {metadata_db}.PUBLIC.record_reprocessing_batch_logs
        """)
        grants = cursor.fetchall()

        has_webapp_insert = any('WEB_APP' in str(grant) and 'INSERT' in str(grant) for grant in grants)
        if has_webapp_insert:
            print("  ✅ WEB_APP role has INSERT permission")
        else:
            print("  ❌ WEB_APP role missing INSERT permission")
            print("     💡 Deploy permissions: python sql/deploy/deploy.py --env staging --only 006_permissions")
            return False
    except Exception as e:
        print(f"  ⚠️  Could not check permissions: {e}")

    print()
    return True
