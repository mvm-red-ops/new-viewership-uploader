#!/usr/bin/env python3
"""
Cleanup Utilities for Viewership Upload Pipeline

Common cleanup operations for data and testing
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import snowflake.connector
from config import load_snowflake_config


def cleanup_file(platform, filename, env='staging'):
    """Clean up all records for a specific file across all tables"""

    print("=" * 80)
    print(f"Cleaning up {platform} - {filename}")
    print(f"Environment: {env}")
    print("=" * 80)
    print()

    # Environment-specific table names
    tables = {
        'staging': {
            'staging_viewership': 'TEST_STAGING.PUBLIC.platform_viewership',
            'upload_viewership': 'UPLOAD_DB.PUBLIC.platform_viewership',
            'episode_details': 'STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING',
            'reprocessing_logs': 'METADATA_MASTER_CLEANED_STAGING.PUBLIC.record_reprocessing_batch_logs',
        },
        'prod': {
            'staging_viewership': 'NOSEY_PROD.PUBLIC.platform_viewership',
            'upload_viewership': 'UPLOAD_DB_PROD.PUBLIC.platform_viewership',
            'episode_details': 'ASSETS.PUBLIC.EPISODE_DETAILS',
            'reprocessing_logs': 'METADATA_MASTER.PUBLIC.record_reprocessing_batch_logs',
        }
    }[env]

    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    try:
        # Clean staging viewership
        cursor.execute(f"""
            DELETE FROM {tables['staging_viewership']}
            WHERE platform = '{platform}'
            AND filename = '{filename}'
        """)
        staging_deleted = cursor.rowcount
        print(f"✅ Deleted {staging_deleted:,} records from staging viewership")

        # Clean upload viewership
        cursor.execute(f"""
            DELETE FROM {tables['upload_viewership']}
            WHERE platform = '{platform}'
            AND filename = '{filename}'
        """)
        upload_deleted = cursor.rowcount
        print(f"✅ Deleted {upload_deleted:,} records from upload viewership")

        # Clean episode details
        cursor.execute(f"""
            DELETE FROM {tables['episode_details']}
            WHERE platform = '{platform}'
            AND filename = '{filename}'
        """)
        episode_deleted = cursor.rowcount
        print(f"✅ Deleted {episode_deleted:,} records from episode details")

        # Clean reprocessing logs
        cursor.execute(f"""
            DELETE FROM {tables['reprocessing_logs']}
            WHERE filename = '{filename}'
        """)
        logs_deleted = cursor.rowcount
        print(f"✅ Deleted {logs_deleted:,} records from reprocessing logs")

        print()
        print("=" * 80)
        print(f"✅ Cleanup complete!")
        print(f"Total deleted: {staging_deleted + upload_deleted + episode_deleted + logs_deleted:,} records")
        print("=" * 80)

    finally:
        conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Clean up test data')
    parser.add_argument('--platform', required=True, help='Platform name (e.g., Youtube, Roku)')
    parser.add_argument('--filename', required=True, help='Filename to clean up')
    parser.add_argument('--env', default='staging', choices=['staging', 'prod'],
                        help='Environment (default: staging)')

    args = parser.parse_args()

    cleanup_file(args.platform, args.filename, args.env)
