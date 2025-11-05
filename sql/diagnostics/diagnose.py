#!/usr/bin/env python3
"""
Consolidated diagnostic tool for viewership upload pipeline

Usage:
    # Check everything for a specific upload
    python sql/diagnostics/diagnose.py --env staging --platform Youtube --filename "file.csv"

    # Check specific component
    python sql/diagnostics/diagnose.py --env staging --check udfs
    python sql/diagnostics/diagnose.py --env staging --check schema
    python sql/diagnostics/diagnose.py --env staging --check asset-matching --platform Youtube --filename "file.csv"
    python sql/diagnostics/diagnose.py --env staging --check data-flow --platform Youtube --filename "file.csv"

    # Check all components
    python sql/diagnostics/diagnose.py --env staging --check all
"""

import sys
import os
import argparse
import yaml

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import snowflake.connector
from config import load_snowflake_config

# Import modular checks
from checks import check_udfs, check_schema, check_data_flow, check_asset_matching, check_deployment


def load_env_config(env):
    """Load environment configuration from config.yaml"""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'deploy',
        'config.yaml'
    )

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if env not in config['environments']:
        raise ValueError(f"Unknown environment: {env}")

    return config['environments'][env]


def run_diagnostics(env, check_type='all', platform=None, filename=None):
    """Run diagnostic checks"""

    # Load environment config
    env_config = load_env_config(env)

    # Connect to Snowflake
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    print()
    print("=" * 80)
    print(f"DIAGNOSTICS - Environment: {env.upper()}")
    print("=" * 80)
    print()

    if platform and filename:
        print(f"Platform: {platform}")
        print(f"Filename: {filename}")
        print()

    # Track overall success
    all_passed = True

    # Run checks based on type
    if check_type in ['all', 'deployment']:
        env_config['_env'] = env  # Pass environment name for error messages
        if not check_deployment(cursor, env_config):
            all_passed = False

    if check_type in ['all', 'udfs']:
        if not check_udfs(cursor, env_config):
            all_passed = False

    if check_type in ['all', 'schema']:
        if not check_schema(cursor, env_config):
            all_passed = False

    if check_type in ['all', 'data-flow']:
        if not check_data_flow(cursor, env_config, platform, filename):
            all_passed = False

    if check_type in ['all', 'asset-matching']:
        if not check_asset_matching(cursor, env_config, platform, filename):
            all_passed = False

    # Close connection
    conn.close()

    # Summary
    print()
    print("=" * 80)
    if all_passed:
        print("✅ All checks passed")
    else:
        print("❌ Some checks failed - see output above for details")
    print("=" * 80)
    print()

    return 0 if all_passed else 1


def main():
    parser = argparse.ArgumentParser(
        description='Diagnostic tool for viewership upload pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--env',
        required=True,
        choices=['staging', 'prod'],
        help='Environment to check'
    )

    parser.add_argument(
        '--check',
        choices=['all', 'deployment', 'udfs', 'schema', 'data-flow', 'asset-matching'],
        default='all',
        help='Type of check to run (default: all)'
    )

    parser.add_argument(
        '--platform',
        help='Platform name (e.g., Youtube, Roku) - required for data-flow and asset-matching checks'
    )

    parser.add_argument(
        '--filename',
        help='Filename to check - required for data-flow and asset-matching checks'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.check in ['data-flow', 'asset-matching']:
        if not args.platform or not args.filename:
            parser.error(f"--platform and --filename are required for --check {args.check}")

    # Run diagnostics
    return run_diagnostics(args.env, args.check, args.platform, args.filename)


if __name__ == '__main__':
    sys.exit(main())
