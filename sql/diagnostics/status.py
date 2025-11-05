#!/usr/bin/env python3
"""
Quick status of recent uploads and common issues

Usage:
    python sql/diagnostics/status.py --env prod
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import snowflake.connector
from config import load_snowflake_config
import yaml
from pathlib import Path
import argparse
from datetime import datetime, timedelta


def get_recent_uploads(cursor, env_config):
    """Get recent uploads and their status"""
    staging_db = env_config['STAGING_DB']

    # Get uploads from last 24 hours
    cursor.execute(f"""
        SELECT
            platform,
            filename,
            COUNT(*) as total,
            COUNT(CASE WHEN ref_id IS NOT NULL THEN 1 END) as matched,
            MAX(phase) as max_phase,
            MIN(CAST(id AS VARCHAR)) as first_upload
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE id IN (
            SELECT id FROM {staging_db}.PUBLIC.platform_viewership
            GROUP BY filename, platform
            HAVING MAX(id) >= (SELECT MAX(id) - 100000 FROM {staging_db}.PUBLIC.platform_viewership)
        )
        GROUP BY platform, filename
        ORDER BY first_upload DESC
        LIMIT 10
    """)

    return cursor.fetchall()


def check_common_issues(cursor, env_config, platform, filename):
    """Check for common issues with a specific upload"""
    staging_db = env_config['STAGING_DB']
    metadata_db = env_config['METADATA_DB']
    upload_db = env_config['UPLOAD_DB']

    issues = []

    # Check 1: All unmatched?
    cursor.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN ref_id IS NULL THEN 1 END) as unmatched
        FROM {staging_db}.PUBLIC.platform_viewership
        WHERE platform = '{platform}' AND filename = '{filename}'
    """)
    total, unmatched = cursor.fetchone()

    if total > 0 and unmatched == total:
        issues.append("ALL RECORDS UNMATCHED - Check bucket procedures")
    elif unmatched > total * 0.5:
        issues.append(f"HIGH UNMATCHED RATE ({unmatched}/{total})")

    # Check 2: Are unmatched records logged?
    if unmatched > 0:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {metadata_db}.PUBLIC.record_reprocessing_batch_logs
            WHERE filename = '{filename}'
        """)
        logged = cursor.fetchone()[0]

        if logged == 0:
            issues.append("UNMATCHED NOT LOGGED - Check sequence permissions")
        elif logged < unmatched:
            issues.append(f"PARTIAL LOGGING ({logged}/{unmatched})")

    # Check 3: Recent errors?
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {upload_db}.public.error_log_table
        WHERE platform = '{platform}'
        AND log_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        AND (status = 'ERROR' OR error_message IS NOT NULL)
    """)
    error_count = cursor.fetchone()[0]

    if error_count > 0:
        issues.append(f"{error_count} ERRORS IN LAST HOUR")

    return issues


def main():
    parser = argparse.ArgumentParser(description='Quick upload status')
    parser.add_argument('--env', required=True, choices=['staging', 'prod'])
    args = parser.parse_args()

    # Load config
    config_path = Path(__file__).parent.parent / 'deploy' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    env_config = config['environments'][args.env]

    # Connect
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    print("=" * 80)
    print(f"📊 RECENT UPLOADS - {args.env.upper()}")
    print("=" * 80)

    uploads = get_recent_uploads(cursor, env_config)

    if not uploads:
        print("\n  No recent uploads found")
    else:
        for platform, filename, total, matched, max_phase, _ in uploads:
            match_rate = (matched / total * 100) if total > 0 else 0
            status = "✅" if match_rate > 90 else "⚠️" if match_rate > 50 else "❌"

            print(f"\n{status} {platform} / {filename}")
            print(f"   {total:,} records | {matched:,} matched ({match_rate:.0f}%) | Phase {max_phase}")

            # Check for issues
            issues = check_common_issues(cursor, env_config, platform, filename)
            if issues:
                for issue in issues:
                    print(f"   ⚠️  {issue}")

    print("\n" + "=" * 80)

    conn.close()


if __name__ == '__main__':
    main()
