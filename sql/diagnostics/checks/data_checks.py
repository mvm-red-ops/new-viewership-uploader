"""Check data flow through pipeline phases"""

def check_data_flow(cursor, env_config, platform=None, filename=None):
    """Check data progression through phases"""
    print("=" * 80)
    print("Data Flow Checks")
    print("=" * 80)
    print()

    staging_db = env_config['STAGING_DB']

    if not platform or not filename:
        print("  ⚠️  Need --platform and --filename to check data flow")
        return True

    # Phase 0: Upload
    print("Phase 0: Upload")
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
    """)
    total_uploaded = cursor.fetchone()[0]

    if total_uploaded == 0:
        print(f"  ❌ No records found for {platform} / {filename}")
        print("     💡 Check if file was uploaded successfully")
        return False

    print(f"  ✅ {total_uploaded:,} records uploaded")
    print()

    # Phase distribution
    print("Phase Distribution:")
    cursor.execute(f"""
        SELECT
            phase,
            processed,
            COUNT(*) as count
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        GROUP BY phase, processed
        ORDER BY phase
    """)
    phase_dist = cursor.fetchall()

    for phase, processed, count in phase_dist:
        proc_status = "PROCESSED" if processed else "NOT PROCESSED"
        print(f"  Phase {phase}, {proc_status}: {count:,}")
    print()

    # Step 1: Deal matching (deal_parent)
    print("Step 1: Deal Matching")
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND deal_parent IS NOT NULL
    """)
    matched_deal = cursor.fetchone()[0]

    if matched_deal == 0:
        print(f"  ❌ 0 records have deal_parent")
        print("     💡 Check active_deals table has entry for this platform/partner/channel/territory")
        return False
    elif matched_deal < total_uploaded:
        print(f"  ⚠️  {matched_deal:,} / {total_uploaded:,} records have deal_parent ({matched_deal/total_uploaded*100:.1f}%)")
    else:
        print(f"  ✅ {matched_deal:,} records have deal_parent")
    print()

    # Step 2: Internal series matching
    print("Step 2: Internal Series Matching")
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND internal_series IS NOT NULL
    """)
    matched_series = cursor.fetchone()[0]

    if matched_series == 0:
        print(f"  ⚠️  0 records have internal_series")
        print("     💡 Check internal_series_dictionary table has entries for this platform_series")
    else:
        print(f"  ✅ {matched_series:,} records have internal_series ({matched_series/total_uploaded*100:.1f}%)")
    print()

    # Step 3: Asset matching (ref_id, asset_series, content_provider)
    print("Step 3: Asset Matching")
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND ref_id IS NOT NULL
        AND asset_series IS NOT NULL
    """)
    matched_assets = cursor.fetchone()[0]

    if matched_assets == 0:
        print(f"  ❌ 0 records matched to assets")
        print("     💡 Check asset matching diagnostics with --check asset-matching")
        return False
    elif matched_assets < total_uploaded:
        unmatched = total_uploaded - matched_assets
        print(f"  ⚠️  {matched_assets:,} / {total_uploaded:,} records matched ({matched_assets/total_uploaded*100:.1f}%)")
        print(f"     {unmatched:,} unmatched records")
    else:
        print(f"  ✅ {matched_assets:,} records matched to assets")
    print()

    # Check Phase 3 INSERT eligibility
    print("Phase 3: INSERT Eligibility")
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}'
        AND filename = '{filename}'
        AND processed IS NULL
        AND phase = '3'
        AND deal_parent IS NOT NULL
        AND ref_id IS NOT NULL
        AND asset_series IS NOT NULL
        AND tot_mov IS NOT NULL
        AND tot_hov IS NOT NULL
    """)
    eligible_insert = cursor.fetchone()[0]

    print(f"  Records eligible for INSERT: {eligible_insert:,}")

    # Check for NULL values blocking INSERT
    required_fields = ['deal_parent', 'ref_id', 'asset_series', 'tot_mov', 'tot_hov']
    null_counts = {}

    for field in required_fields:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {staging_db}.PUBLIC.platform_viewership
            WHERE platform = '{platform}'
            AND filename = '{filename}'
            AND processed IS NULL
            AND phase = '3'
            AND {field} IS NULL
        """)
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            null_counts[field] = null_count

    if null_counts:
        print()
        print("  Fields with NULL values blocking INSERT:")
        for field, count in null_counts.items():
            print(f"    ❌ {field}: {count:,} NULL records")
    print()

    return True
