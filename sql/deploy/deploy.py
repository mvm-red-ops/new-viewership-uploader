#!/usr/bin/env python3
"""
Orchestrated Deployment Script for Viewership Upload Pipeline

Usage:
    python sql/deploy/deploy.py --env staging
    python sql/deploy/deploy.py --env prod
    python sql/deploy/deploy.py --env staging --only permissions
    python sql/deploy/deploy.py --env staging --skip-verify
"""

import sys
import os
import argparse
import yaml
from pathlib import Path

# Add parent directory to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import snowflake.connector
from config import load_snowflake_config


def load_deployment_config():
    """Load deployment configuration from YAML"""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def replace_placeholders(sql_content, env_config):
    """Replace {{PLACEHOLDERS}} with environment-specific values"""
    for key, value in env_config.items():
        sql_content = sql_content.replace(f"{{{{{key}}}}}", value)
    return sql_content


def execute_migration(conn, migration, env_config, migrations_dir):
    """Execute a single migration file"""
    migration_file = migrations_dir / migration['file']

    if not migration_file.exists():
        print(f"  ⚠️  File not found: {migration_file}")
        if migration.get('required', True):
            raise FileNotFoundError(f"Required migration file not found: {migration_file}")
        return False

    print(f"  📄 Reading: {migration_file.name}")

    with open(migration_file, 'r') as f:
        sql_template = f.read()

    # Replace placeholders
    sql = replace_placeholders(sql_template, env_config)

    # Remove comment-only lines for cleaner execution
    lines = sql.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--'):
            clean_lines.append(line)
    sql = '\n'.join(clean_lines).strip()

    try:
        # Execute all statements
        for _ in conn.execute_string(sql):
            pass
        print(f"  ✅ Executed successfully")
        return True
    except Exception as e:
        print(f"  ❌ Error: {e}")
        if migration.get('required', True):
            raise
        return False


def verify_deployment(conn, verifications, env_config):
    """Run verification queries"""
    print("\n" + "=" * 80)
    print("🔍 Verifying Deployment")
    print("=" * 80)

    cursor = conn.cursor()
    all_passed = True

    for verification in verifications:
        name = verification['name']
        query = replace_placeholders(verification['query'], env_config)
        expect = verification['expect']

        print(f"\n  Testing: {name}")
        print(f"  Query: {query[:80]}...")

        try:
            cursor.execute(query)
            results = cursor.fetchall()

            if expect == "at_least_one_row":
                if len(results) > 0:
                    print(f"  ✅ PASS: Found {len(results)} row(s)")
                else:
                    print(f"  ❌ FAIL: Expected at least one row, got {len(results)}")
                    all_passed = False

            elif expect == "contains":
                # Check if results contain expected values
                contains_list = verification.get('contains', [])
                result_str = str(results).upper()
                missing = []
                for item in contains_list:
                    if item.upper() not in result_str:
                        missing.append(item)

                if not missing:
                    print(f"  ✅ PASS: Found all required items")
                else:
                    print(f"  ❌ FAIL: Missing items: {', '.join(missing)}")
                    all_passed = False

        except Exception as e:
            print(f"  ❌ FAIL: {e}")
            all_passed = False

    return all_passed


def main():
    parser = argparse.ArgumentParser(description='Deploy viewership upload pipeline')
    parser.add_argument('--env', required=True, choices=['staging', 'prod'],
                        help='Environment to deploy to')
    parser.add_argument('--only', help='Only deploy specific migration (e.g., permissions)')
    parser.add_argument('--skip-verify', action='store_true',
                        help='Skip post-deployment verification')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be deployed without executing')

    args = parser.parse_args()

    print("=" * 80)
    print(f"🚀 Viewership Upload Pipeline Deployment")
    print(f"Environment: {args.env.upper()}")
    print("=" * 80)
    print()

    # Load configuration
    deployment_config = load_deployment_config()
    env_config = deployment_config['environments'][args.env]
    migrations = deployment_config['migrations']

    print("📋 Environment Configuration:")
    for key, value in env_config.items():
        print(f"  {key}: {value}")
    print()

    # Filter migrations if --only specified
    if args.only:
        migrations = [m for m in migrations if args.only.lower() in m['name'].lower()]
        if not migrations:
            print(f"❌ No migrations found matching: {args.only}")
            return 1

    # Dry run mode
    if args.dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print("\nMigrations that would be executed:")
        for i, migration in enumerate(migrations, 1):
            print(f"  {i}. {migration['name']}")
            print(f"     File: {migration['file']}")
            if migration.get('description'):
                print(f"     Note: {migration['description']}")
        return 0

    # Connect to Snowflake
    print("🔗 Connecting to Snowflake...")
    try:
        sf_config = load_snowflake_config()
        conn = snowflake.connector.connect(**sf_config)
        print("✅ Connected successfully")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 1

    print()

    # Execute migrations
    migrations_dir = Path(__file__).parent.parent / "migrations"
    failed_migrations = []

    for i, migration in enumerate(migrations, 1):
        print(f"[{i}/{len(migrations)}] {migration['name']}")
        if migration.get('description'):
            print(f"  Note: {migration['description']}")

        try:
            success = execute_migration(conn, migration, env_config, migrations_dir.parent)
            if not success and migration.get('required', True):
                failed_migrations.append(migration['name'])
        except Exception as e:
            print(f"  ❌ Migration failed: {e}")
            failed_migrations.append(migration['name'])
            if migration.get('required', True):
                print(f"\n❌ Deployment failed due to required migration failure")
                return 1

        print()

    # Verify deployment
    if not args.skip_verify:
        if 'verification' in deployment_config:
            verify_success = verify_deployment(conn, deployment_config['verification'], env_config)
        else:
            verify_success = True

        # Run comprehensive deployment verification
        print("\n" + "=" * 80)
        print("🔍 Running Comprehensive Deployment Verification")
        print("=" * 80)
        print()

        # Import deployment check
        import sys
        from pathlib import Path
        diagnostics_path = Path(__file__).parent.parent / 'diagnostics'
        sys.path.insert(0, str(diagnostics_path))
        from checks import check_deployment

        env_config['_env'] = args.env
        cursor = conn.cursor()
        deployment_check_passed = check_deployment(cursor, env_config)

        if not deployment_check_passed:
            print("\n⚠️  WARNING: Deployment verification found issues")
            print("   Review the output above and fix any missing components")
            verify_success = False
    else:
        verify_success = True

    conn.close()

    # Final summary
    print("\n" + "=" * 80)
    if failed_migrations:
        print("⚠️  Deployment completed with failures:")
        for name in failed_migrations:
            print(f"  - {name}")
        print("=" * 80)
        return 1
    elif verify_success:
        print("✅ Deployment completed successfully!")
        print("=" * 80)
        return 0
    else:
        print("⚠️  Deployment completed but verification failed")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
