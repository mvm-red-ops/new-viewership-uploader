#!/usr/bin/env python3
"""
Check exactly what's blocking YouTube records from INSERT
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
from config import load_snowflake_config

def check_blocking():
    """Check what columns are NULL for YouTube records"""

    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)

    try:
        cursor = conn.cursor()

        filename = 'youtube_daily_2025-07-01_to_2025-09-30 (1).csv'

        print("=" * 80)
        print("🔍 Checking YouTube Records - What's Blocking INSERT?")
        print("=" * 80)
        print()

        # Check phase distribution
        cursor.execute(f"""
            SELECT
                phase,
                processed,
                COUNT(*) as count
            FROM test_staging.public.platform_viewership
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
            GROUP BY phase, processed
            ORDER BY phase, processed
        """)
        phase_dist = cursor.fetchall()

        print("📊 Phase Distribution:")
        for row in phase_dist:
            phase, processed, count = row
            proc_status = "PROCESSED" if processed else "NOT PROCESSED"
            print(f"   Phase {phase}, {proc_status}: {count:,} records")
        print()

        # Total count in phase 2
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM test_staging.public.platform_viewership
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
            AND processed IS NULL
            AND phase = '2'
        """)
        total = cursor.fetchone()[0]
        print(f"📊 Total records in phase 2 (unprocessed): {total:,}")
        print()

        # Check each required column
        checks = [
            ("deal_parent IS NULL", "deal_parent"),
            ("ref_id IS NULL", "ref_id"),
            ("asset_series IS NULL", "asset_series"),
            ("tot_mov IS NULL", "tot_mov"),
            ("tot_hov IS NULL", "tot_hov"),
            ("week IS NULL", "week"),
            ("day IS NULL", "day"),
        ]

        print("Missing Required Columns:")
        print("-" * 80)

        for condition, col_name in checks:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM test_staging.public.platform_viewership
                WHERE platform = 'Youtube'
                AND filename = '{filename}'
                AND processed IS NULL
                AND phase = '2'
                AND {condition}
            """)
            count = cursor.fetchone()[0]
            status = "❌" if count > 0 else "✅"
            print(f"{status} {col_name:20s}: {count:,} NULL records")

        print()
        print("-" * 80)

        # Check how many pass ALL requirements
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM test_staging.public.platform_viewership
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
            AND processed IS NULL
            AND phase = '2'
            AND deal_parent IS NOT NULL
            AND ref_id IS NOT NULL
            AND asset_series IS NOT NULL
            AND tot_mov IS NOT NULL
            AND tot_hov IS NOT NULL
        """)
        eligible = cursor.fetchone()[0]

        print()
        print(f"✅ Records that SHOULD be inserted: {eligible:,}")
        print(f"❌ Records blocked from INSERT: {total - eligible:,}")
        print()

        # Check what was actually inserted (any phase, any processed status)
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
        """)
        inserted_total = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT
                phase,
                processed,
                label,
                COUNT(*) as count
            FROM STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
            GROUP BY phase, processed, label
            ORDER BY phase, processed, label
        """)
        inserted_breakdown = cursor.fetchall()

        print(f"📥 Records in EPISODE_DETAILS_TEST_STAGING:")
        print(f"   Total: {inserted_total:,}")
        if inserted_breakdown:
            for row in inserted_breakdown:
                phase, processed, label, count = row
                proc_status = "PROCESSED" if processed else "NOT PROCESSED"
                print(f"   Phase {phase}, {proc_status}, {label}: {count:,}")
        print()

        # Check how many in phase 3 meet INSERT requirements
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM test_staging.public.platform_viewership
            WHERE platform = 'Youtube'
            AND filename = '{filename}'
            AND processed IS NULL
            AND phase = '3'
            AND deal_parent IS NOT NULL
            AND ref_id IS NOT NULL
            AND asset_series IS NOT NULL
            AND tot_mov IS NOT NULL
            AND tot_hov IS NOT NULL
        """)
        phase3_eligible = cursor.fetchone()[0]

        print(f"🔍 Phase 3 records that SHOULD have been inserted: {phase3_eligible:,}")
        print()

        if eligible > 0 and inserted == 0:
            print("⚠️  PROBLEM: Records are eligible but NONE were inserted!")
            print()
            print("Checking for other potential issues:")
            print("-" * 80)

            # Check filename and case sensitivity
            cursor.execute(f"""
                SELECT DISTINCT filename
                FROM test_staging.public.platform_viewership
                WHERE platform = 'Youtube'
                AND phase = '3'
                LIMIT 5
            """)
            filenames = cursor.fetchall()
            if filenames:
                actual_filename = filenames[0][0]
                print(f"Filename in source table: '{actual_filename}'")
                print(f"Filename we're checking: '{filename}'")
                print(f"Match: {actual_filename == filename}")
                print(f"LOWER match: {actual_filename.lower() == filename.lower()}")
                print()

                # Try the INSERT query manually to see what happens
                print("🔬 Testing INSERT query:")
                print("-" * 80)

                test_insert = f"""
                    SELECT COUNT(*)
                    FROM test_staging.public.platform_viewership
                    WHERE platform = 'Youtube'
                    AND deal_parent is not null
                    AND processed is null
                    AND ref_id is not null
                    AND asset_series is not null
                    AND tot_mov is not null
                    AND tot_hov is not null
                    AND LOWER(filename) = LOWER('{filename}')
                """

                cursor.execute(test_insert)
                count = cursor.fetchone()[0]
                print(f"Records matching INSERT WHERE clause: {count:,}")
                print()

                # Check content_provider
                cursor.execute(f"""
                    SELECT
                        CASE WHEN content_provider IS NULL THEN 'NULL' ELSE 'NOT NULL' END as cp_status,
                        COUNT(*) as count
                    FROM test_staging.public.platform_viewership
                    WHERE platform = 'Youtube'
                    AND filename = '{actual_filename}'
                    AND phase = '3'
                    GROUP BY 1
                """)
                cp_results = cursor.fetchall()
                print("content_provider status:")
                for row in cp_results:
                    status, count = row
                    print(f"   {status}: {count:,} records")

    finally:
        conn.close()

if __name__ == '__main__':
    check_blocking()
