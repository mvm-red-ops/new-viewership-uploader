"""Check asset matching strategies and bucket performance"""

def check_asset_matching(cursor, env_config, platform=None, filename=None):
    """Analyze asset matching strategy performance"""
    print("=" * 80)
    print("Asset Matching Analysis")
    print("=" * 80)
    print()

    upload_db = env_config['UPLOAD_DB']

    if not platform or not filename:
        print("  ⚠️  Need --platform and --filename to check asset matching")
        return True

    # Check recent error logs from bucket procedures
    print("Recent Errors from Asset Matching:")
    cursor.execute(f"""
        SELECT
            log_time,
            procedure_name,
            log_message,
            error_message
        FROM {upload_db}.public.error_log_table
        WHERE platform = '{platform}'
        AND log_time >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
        AND (status = 'ERROR' OR error_message IS NOT NULL)
        AND procedure_name LIKE '%bucket%'
        ORDER BY log_time DESC
        LIMIT 10
    """)

    errors = cursor.fetchall()

    if errors:
        for log_time, proc, msg, err in errors:
            print(f"  [{log_time}] {proc}")
            print(f"    Message: {msg}")
            if err:
                print(f"    ERROR: {err}")
            print()
    else:
        print("  ✅ No recent errors from bucket procedures")
    print()

    # Check bucket strategy logs to see which strategies ran and matched
    print("Asset Matching Strategy Performance:")
    cursor.execute(f"""
        SELECT
            log_message
        FROM {upload_db}.public.error_log_table
        WHERE platform = '{platform}'
        AND log_time >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
        AND (log_message LIKE '%bucket matched%' OR log_message LIKE '%Strategy%')
        ORDER BY log_time
        LIMIT 20
    """)

    strategy_logs = cursor.fetchall()

    if strategy_logs:
        for (msg,) in strategy_logs:
            if 'matched' in msg.lower():
                # Extract match count if possible
                print(f"  {msg}")
    else:
        print("  ⚠️  No strategy performance logs found")
        print("     Asset matching may not have run")
    print()

    # Check unmatched records
    staging_db = env_config['STAGING_DB']
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND ref_id IS NULL
        AND phase IS NOT NULL
    """)
    unmatched_count = cursor.fetchone()[0]

    if unmatched_count > 0:
        print(f"Unmatched Records: {unmatched_count:,}")
        print()

        # Sample unmatched records to see common patterns
        print("Sample of unmatched records:")
        cursor.execute(f"""
            SELECT
                platform_content_name,
                internal_series,
                deal_parent
            FROM {staging_db}.PUBLIC.platform_viewership
            WHERE platform = '{platform}'
            AND filename = '{filename}'
            AND ref_id IS NULL
            LIMIT 5
        """)
        samples = cursor.fetchall()

        for i, (content_name, series, deal) in enumerate(samples, 1):
            print(f"  {i}. {content_name}")
            print(f"     internal_series: {series}")
            print(f"     deal_parent: {deal}")
        print()

        # Check if unmatched records were logged to reprocessing table
        metadata_db = env_config['METADATA_DB']
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {metadata_db}.public.record_reprocessing_batch_logs
            WHERE platform = '{platform}'
            AND filename = '{filename}'
        """)
        logged_count = cursor.fetchone()[0]

        if logged_count > 0:
            print(f"  ✅ {logged_count:,} unmatched records logged to record_reprocessing_batch_logs")
        else:
            print(f"  ❌ 0 unmatched records logged")
            print("     💡 Check if stored procedure has unmatched logging enabled")
        print()

    # Common failure patterns diagnosis
    print("Common Failure Patterns:")

    # Check if platform_content_id is populated (needed for ref_id matching)
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND platform_content_id IS NULL
    """)
    missing_content_id = cursor.fetchone()[0]

    if missing_content_id > 0:
        print(f"  ⚠️  {missing_content_id:,} records missing platform_content_id")
        print("     This blocks ref_id matching strategies")
    else:
        print(f"  ✅ All records have platform_content_id")

    # Check if internal_series is populated (needed for series-based matching)
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND internal_series IS NULL
    """)
    missing_series = cursor.fetchone()[0]

    if missing_series > 0:
        print(f"  ⚠️  {missing_series:,} records missing internal_series")
        print("     This limits series-based matching strategies")
    else:
        print(f"  ✅ All records have internal_series")

    print()

    return True
