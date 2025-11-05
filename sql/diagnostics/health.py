#!/usr/bin/env python3
"""
Single command to check everything

Usage:
    python sql/diagnostics/health.py              # Check both environments
    python sql/diagnostics/health.py --env prod   # Check prod only
    python sql/diagnostics/health.py --fix        # Show fix commands for issues
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import snowflake.connector
from config import load_snowflake_config
import yaml
from pathlib import Path
import argparse


class HealthCheck:
    def __init__(self, conn, env_config, env_name):
        self.conn = conn
        self.cursor = conn.cursor()
        self.env_config = env_config
        self.env_name = env_name
        self.issues = []

    def check(self, category, name, condition, fix_cmd=None):
        """Check a condition and track issues"""
        if condition:
            print(f"  ✅ {name}")
            return True
        else:
            print(f"  ❌ {name}")
            self.issues.append({
                'category': category,
                'name': name,
                'fix': fix_cmd
            })
            return False

    def run_all(self):
        """Run all health checks"""
        upload_db = self.env_config['UPLOAD_DB']

        # 1. Critical Procedures
        print(f"\n🔧 PROCEDURES ({upload_db}):")
        self.cursor.execute(f"SHOW PROCEDURES IN {upload_db}.PUBLIC")
        procs = {p[1] for p in self.cursor.fetchall()}

        critical = [
            'ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC',
            'MOVE_DATA_TO_FINAL_TABLE_DYNAMIC_GENERIC',
            'HANDLE_FINAL_INSERT_DYNAMIC_GENERIC'
        ]

        for proc in critical:
            self.check('procedures', proc, proc in procs, f'python sql/deploy/deploy.py --env {self.env_name}')

        # 2. Bucket Procedures (CRITICAL)
        print(f"\n🪣 BUCKETS ({upload_db}):")
        buckets = [
            'PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC',
            'PROCESS_VIEWERSHIP_FULL_DATA_GENERIC',
            'PROCESS_VIEWERSHIP_TITLE_ONLY_GENERIC'
        ]

        all_buckets_ok = True
        for bucket in buckets:
            if not self.check('buckets', bucket, bucket in procs, f'python sql/deploy/deploy.py --env {self.env_name}'):
                all_buckets_ok = False

        if not all_buckets_ok:
            print("     ⚠️  ASSET MATCHING WILL FAIL")

        # 3. Template Substitution
        print(f"\n📝 CONFIG:")
        try:
            self.cursor.execute(f"""
                SELECT GET_DDL('PROCEDURE', '{upload_db}.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(VARCHAR, VARCHAR)')
            """)
            ddl = self.cursor.fetchone()[0]
            has_placeholders = '{{' in ddl
            self.check('config', 'Templates substituted', not has_placeholders, f'python sql/deploy/deploy.py --env {self.env_name}')
        except:
            self.check('config', 'Templates substituted', False, f'python sql/deploy/deploy.py --env {self.env_name}')

        # 4. Permissions
        print(f"\n🔐 PERMISSIONS:")

        # Sequence permission
        try:
            self.cursor.execute(f"SHOW GRANTS ON SEQUENCE {upload_db}.PUBLIC.RECORD_REPROCESSING_IDS")
            grants = {str(g) for g in self.cursor.fetchall()}
            has_seq = any('WEB_APP' in g for g in grants)
            self.check('permissions', 'Sequence (unmatched logging)', has_seq,
                      f'python sql/deploy/deploy.py --env {self.env_name} --only permissions')
        except:
            self.check('permissions', 'Sequence (unmatched logging)', False,
                      f'python sql/deploy/deploy.py --env {self.env_name} --only permissions')

        # UDF permission
        try:
            self.cursor.execute(f"SHOW GRANTS ON FUNCTION {upload_db}.PUBLIC.EXTRACT_PRIMARY_TITLE(VARCHAR)")
            grants = {str(g) for g in self.cursor.fetchall()}
            has_udf = any('WEB_APP' in g for g in grants)
            self.check('permissions', 'UDF (asset matching)', has_udf,
                      f'python sql/deploy/deploy.py --env {self.env_name} --only permissions')
        except:
            self.check('permissions', 'UDF (asset matching)', False,
                      f'python sql/deploy/deploy.py --env {self.env_name} --only permissions')

        return len(self.issues) == 0


def compare_environments(conn):
    """Quick comparison"""
    cursor = conn.cursor()

    config_path = Path(__file__).parent.parent / 'deploy' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    staging = config['environments']['staging']['UPLOAD_DB']
    prod = config['environments']['prod']['UPLOAD_DB']

    # Get bucket procedures in each
    cursor.execute(f"SHOW PROCEDURES LIKE 'PROCESS_VIEWERSHIP%GENERIC' IN {staging}.PUBLIC")
    staging_buckets = len(cursor.fetchall())

    cursor.execute(f"SHOW PROCEDURES LIKE 'PROCESS_VIEWERSHIP%GENERIC' IN {prod}.PUBLIC")
    prod_buckets = len(cursor.fetchall())

    print(f"\n🔄 STAGING vs PROD:")
    if staging_buckets == prod_buckets == 6:
        print(f"  ✅ Both have 6 bucket procedures")
    else:
        print(f"  ⚠️  Staging: {staging_buckets}/6, Prod: {prod_buckets}/6")
        if staging_buckets != 6:
            print(f"     Fix staging: python sql/deploy/deploy.py --env staging")
        if prod_buckets != 6:
            print(f"     Fix prod: python sql/deploy/deploy.py --env prod")

    # Check main procedures
    cursor.execute(f"SHOW PROCEDURES IN {staging}.PUBLIC")
    staging_count = len(cursor.fetchall())

    cursor.execute(f"SHOW PROCEDURES IN {prod}.PUBLIC")
    prod_count = len(cursor.fetchall())

    diff = abs(staging_count - prod_count)
    if diff == 0:
        print(f"  ✅ Both have {staging_count} procedures")
    elif diff <= 5:
        print(f"  ⚠️  Minor difference: {staging_count} vs {prod_count} ({diff} diff)")
    else:
        print(f"  ❌ Major difference: {staging_count} vs {prod_count} ({diff} diff)")
        print(f"     Run: python sql/diagnostics/compare.py")


def main():
    parser = argparse.ArgumentParser(description='Quick health check')
    parser.add_argument('--env', choices=['staging', 'prod'], help='Check specific environment only')
    parser.add_argument('--fix', action='store_true', help='Show fix commands')
    parser.add_argument('--verbose', action='store_true', help='Detailed output')
    args = parser.parse_args()

    # Load config
    config_path = Path(__file__).parent.parent / 'deploy' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Connect
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)

    print("=" * 60)
    print("🏥 HEALTH CHECK")
    print("=" * 60)

    all_good = True
    all_issues = []

    # Check environments
    envs_to_check = ['staging', 'prod'] if not args.env else [args.env]

    for env in envs_to_check:
        print(f"\n{'🔵 STAGING' if env == 'staging' else '🔴 PROD'}:")
        print("-" * 60)

        checker = HealthCheck(conn, config['environments'][env], env)
        env_ok = checker.run_all()

        if not env_ok:
            all_good = False
            all_issues.extend([(env, issue) for issue in checker.issues])

    # Compare if checking both
    if not args.env:
        compare_environments(conn)

    conn.close()

    # Summary
    print(f"\n" + "=" * 60)
    if all_good:
        print("✅ ALL SYSTEMS OPERATIONAL")
    else:
        print(f"❌ {len(all_issues)} ISSUE(S) FOUND")

        if args.fix:
            print(f"\n🔧 FIX COMMANDS:")
            unique_fixes = {}
            for env, issue in all_issues:
                if issue['fix'] and issue['fix'] not in unique_fixes:
                    unique_fixes[issue['fix']] = []
                if issue['fix']:
                    unique_fixes[issue['fix']].append(f"{env}: {issue['name']}")

            for cmd, issues in unique_fixes.items():
                print(f"\n  {cmd}")
                for issue in issues[:3]:  # Show first 3
                    print(f"    → Fixes: {issue}")
                if len(issues) > 3:
                    print(f"    → ... and {len(issues) - 3} more")
        else:
            print(f"\n  Run with --fix to see remediation commands")

    print("=" * 60)

    return 0 if all_good else 1


if __name__ == '__main__':
    sys.exit(main())
