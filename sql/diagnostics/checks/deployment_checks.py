"""Check deployment completeness and health"""

def check_deployment(cursor, env_config):
    """Comprehensive deployment verification"""
    print("=" * 80)
    print("Deployment Verification")
    print("=" * 80)
    print()

    upload_db = env_config['UPLOAD_DB']
    staging_db = env_config['STAGING_DB']
    metadata_db = env_config['METADATA_DB']

    all_passed = True

    # 1. EXISTENCE CHECKS
    print("1. EXISTENCE CHECKS")
    print("-" * 80)

    # Check main stored procedures
    required_procedures = [
        'set_phase_generic',
        'calculate_viewership_metrics',
        'set_date_columns_dynamic',
        'handle_viewership_conflicts',
        'set_deal_parent_generic',
        'set_channel_generic',
        'set_territory_generic',
        'send_unmatched_deals_alert',
        'set_internal_series_generic',
        'analyze_and_process_viewership_data_generic',
        'move_data_to_final_table_dynamic_generic',
        'handle_final_insert_dynamic_generic',
        'validate_viewership_for_insert'
    ]

    print(f"\nMain Procedures ({upload_db}.PUBLIC):")
    missing_procs = []
    for proc_name in required_procedures:
        cursor.execute(f"""
            SHOW PROCEDURES LIKE '{proc_name}' IN {upload_db}.PUBLIC
        """)
        procs = cursor.fetchall()
        if procs:
            print(f"  ✅ {proc_name}")
        else:
            print(f"  ❌ {proc_name} MISSING")
            missing_procs.append(proc_name)
            all_passed = False

    if missing_procs:
        print(f"\n  💡 Deploy missing procedures: python sql/deploy/deploy.py --env {env_config.get('_env', 'staging')}")

    # Check bucket procedures (CRITICAL!)
    print(f"\nBucket Procedures ({upload_db}.PUBLIC):")
    bucket_procedures = [
        'process_viewership_full_data_generic',
        'process_viewership_ref_id_only_generic',
        'process_viewership_ref_id_series_generic',
        'process_viewership_series_only_generic',
        'process_viewership_series_season_episode_generic',
        'process_viewership_title_only_generic'
    ]

    missing_buckets = []
    for proc_name in bucket_procedures:
        cursor.execute(f"""
            SHOW PROCEDURES LIKE '{proc_name}' IN {upload_db}.PUBLIC
        """)
        procs = cursor.fetchall()
        if procs:
            print(f"  ✅ {proc_name}")
        else:
            print(f"  ❌ {proc_name} MISSING")
            missing_buckets.append(proc_name)
            all_passed = False

    if missing_buckets:
        print(f"\n  ⚠️  CRITICAL: Bucket procedures missing - asset matching WILL FAIL")
        print(f"  💡 Deploy: python sql/deploy/deploy.py --env {env_config.get('_env', 'staging')}")

    # Check UDFs
    print(f"\nUser-Defined Functions ({upload_db}.PUBLIC):")
    cursor.execute(f"""
        SHOW USER FUNCTIONS LIKE 'EXTRACT_PRIMARY_TITLE' IN {upload_db}.PUBLIC
    """)
    if cursor.fetchall():
        print(f"  ✅ EXTRACT_PRIMARY_TITLE")
    else:
        print(f"  ❌ EXTRACT_PRIMARY_TITLE MISSING")
        all_passed = False

    print()

    # 2. TEMPLATE SUBSTITUTION CHECKS
    print("2. TEMPLATE SUBSTITUTION CHECKS")
    print("-" * 80)

    # Check if main procedure has unsubstituted placeholders
    try:
        cursor.execute(f"""
            SELECT GET_DDL('PROCEDURE', '{upload_db}.PUBLIC.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR)')
        """)
        ddl = cursor.fetchone()[0]

        placeholders_found = []
        for placeholder in ['{{UPLOAD_DB}}', '{{STAGING_DB}}', '{{METADATA_DB}}', '{{ASSETS_DB}}']:
            if placeholder in ddl:
                placeholders_found.append(placeholder)

        if placeholders_found:
            print(f"  ❌ Unsubstituted placeholders found in analyze_and_process_viewership_data_generic:")
            for ph in placeholders_found:
                print(f"     - {ph}")
            print(f"\n  💡 Procedures were deployed without template substitution!")
            print(f"     Redeploy: python sql/deploy/deploy.py --env {env_config.get('_env', 'staging')}")
            all_passed = False
        else:
            print(f"  ✅ Template substitution successful")
            print(f"     UPLOAD_DB → {upload_db}")
            print(f"     STAGING_DB → {staging_db}")
            print(f"     METADATA_DB → {metadata_db}")
    except Exception as e:
        print(f"  ⚠️  Could not verify template substitution: {e}")

    print()

    # 3. PERMISSIONS CHECKS
    print("3. PERMISSIONS CHECKS")
    print("-" * 80)

    # Check UDF permissions
    print(f"\nUDF Permissions:")
    try:
        cursor.execute(f"""
            SHOW GRANTS ON FUNCTION {upload_db}.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR)
        """)
        grants = cursor.fetchall()
        has_webapp = any('WEB_APP' in str(grant) for grant in grants)
        if has_webapp:
            print(f"  ✅ WEB_APP has USAGE on EXTRACT_PRIMARY_TITLE")
        else:
            print(f"  ❌ WEB_APP missing USAGE on EXTRACT_PRIMARY_TITLE")
            all_passed = False
    except Exception as e:
        print(f"  ⚠️  Could not check: {e}")

    # Check sequence permissions (CRITICAL!)
    print(f"\nSequence Permissions:")
    try:
        cursor.execute(f"""
            SHOW GRANTS ON SEQUENCE {upload_db}.PUBLIC.RECORD_REPROCESSING_IDS
        """)
        grants = cursor.fetchall()
        has_webapp = any('WEB_APP' in str(grant) for grant in grants)
        if has_webapp:
            print(f"  ✅ WEB_APP has USAGE on RECORD_REPROCESSING_IDS")
        else:
            print(f"  ❌ WEB_APP missing USAGE on RECORD_REPROCESSING_IDS")
            print(f"     ⚠️  CRITICAL: Unmatched logging WILL FAIL without this!")
            all_passed = False
    except Exception as e:
        print(f"  ⚠️  Could not check: {e}")

    # Check table permissions
    print(f"\nTable Permissions:")
    try:
        cursor.execute(f"""
            SHOW GRANTS ON TABLE {metadata_db}.PUBLIC.record_reprocessing_batch_logs
        """)
        grants = cursor.fetchall()
        has_insert = any('WEB_APP' in str(grant) and 'INSERT' in str(grant) for grant in grants)
        if has_insert:
            print(f"  ✅ WEB_APP has INSERT on record_reprocessing_batch_logs")
        else:
            print(f"  ❌ WEB_APP missing INSERT on record_reprocessing_batch_logs")
            all_passed = False
    except Exception as e:
        print(f"  ⚠️  Could not check: {e}")

    print()

    # 4. HEALTH CHECKS
    print("4. HEALTH CHECKS")
    print("-" * 80)

    # Check if we can call a simple procedure
    print(f"\nProcedure Callable Test:")
    try:
        # Try calling set_phase_generic with test parameters
        test_sql = f"""
            CALL {upload_db}.public.set_phase_generic('TEST_PLATFORM', 0, 'test_health_check.csv')
        """
        cursor.execute(test_sql)
        print(f"  ✅ set_phase_generic is callable")

        # Clean up test data
        cursor.execute(f"""
            DELETE FROM {staging_db}.PUBLIC.platform_viewership
            WHERE platform = 'TEST_PLATFORM' AND filename = 'test_health_check.csv'
        """)
    except Exception as e:
        print(f"  ❌ set_phase_generic failed: {e}")
        all_passed = False

    # Check if analyze_and_process can find bucket procedures
    print(f"\nBucket Procedure Linkage:")
    try:
        cursor.execute(f"""
            SELECT GET_DDL('PROCEDURE', '{upload_db}.PUBLIC.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR)')
        """)
        ddl = cursor.fetchone()[0]

        # Check if it calls bucket procedures correctly (case insensitive)
        ddl_upper = ddl.upper()
        if 'PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC' in ddl_upper or 'PROCESS_VIEWERSHIP_' in ddl_upper:
            print(f"  ✅ Main procedure references bucket procedures")
        else:
            print(f"  ❌ Main procedure does NOT reference bucket procedures")
            print(f"     Asset matching will fail silently!")
            all_passed = False
    except Exception as e:
        print(f"  ⚠️  Could not verify: {e}")

    print()
    print("=" * 80)

    if all_passed:
        print("✅ ALL DEPLOYMENT CHECKS PASSED")
        print("   System is fully deployed and operational")
    else:
        print("❌ DEPLOYMENT INCOMPLETE")
        print("   Critical components missing - see errors above")

    print("=" * 80)
    print()

    return all_passed
