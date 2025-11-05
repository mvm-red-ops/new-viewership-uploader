#!/usr/bin/env python3
"""
Compare deployment status between staging and prod

Usage:
    python sql/diagnostics/compare.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import snowflake.connector
from config import load_snowflake_config
import yaml
from pathlib import Path


def get_procedure_list(cursor, database):
    """Get list of all procedures in a database"""
    cursor.execute(f"""
        SHOW PROCEDURES IN {database}.PUBLIC
    """)
    procs = cursor.fetchall()
    return {proc[1] for proc in procs}  # procedure names


def get_udf_list(cursor, database):
    """Get list of all UDFs in a database"""
    cursor.execute(f"""
        SHOW USER FUNCTIONS IN {database}.PUBLIC
    """)
    funcs = cursor.fetchall()
    return {func[1] for func in funcs}  # function names


def compare_deployments():
    """Compare staging and prod deployments"""

    # Load configs
    config_path = Path(__file__).parent.parent / 'deploy' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    staging_config = config['environments']['staging']
    prod_config = config['environments']['prod']

    # Connect to Snowflake
    sf_config = load_snowflake_config()
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    print("=" * 80)
    print("STAGING vs PROD COMPARISON")
    print("=" * 80)
    print()

    # Compare procedures
    print("STORED PROCEDURES:")
    print("-" * 80)

    staging_procs = get_procedure_list(cursor, staging_config['UPLOAD_DB'])
    prod_procs = get_procedure_list(cursor, prod_config['UPLOAD_DB'])

    only_staging = staging_procs - prod_procs
    only_prod = prod_procs - staging_procs
    in_both = staging_procs & prod_procs

    print(f"  In both environments: {len(in_both)}")

    if only_staging:
        print(f"\n  ⚠️  Only in STAGING ({len(only_staging)}):")
        for proc in sorted(only_staging):
            print(f"     - {proc}")

    if only_prod:
        print(f"\n  ⚠️  Only in PROD ({len(only_prod)}):")
        for proc in sorted(only_prod):
            print(f"     - {proc}")

    if not only_staging and not only_prod:
        print(f"  ✅ All procedures match")

    # Compare UDFs
    print(f"\n\nUSER-DEFINED FUNCTIONS:")
    print("-" * 80)

    staging_udfs = get_udf_list(cursor, staging_config['UPLOAD_DB'])
    prod_udfs = get_udf_list(cursor, prod_config['UPLOAD_DB'])

    only_staging_udfs = staging_udfs - prod_udfs
    only_prod_udfs = prod_udfs - staging_udfs
    in_both_udfs = staging_udfs & prod_udfs

    print(f"  In both environments: {len(in_both_udfs)}")

    if only_staging_udfs:
        print(f"\n  ⚠️  Only in STAGING ({len(only_staging_udfs)}):")
        for udf in sorted(only_staging_udfs):
            print(f"     - {udf}")

    if only_prod_udfs:
        print(f"\n  ⚠️  Only in PROD ({len(only_prod_udfs)}):")
        for udf in sorted(only_prod_udfs):
            print(f"     - {udf}")

    if not only_staging_udfs and not only_prod_udfs:
        print(f"  ✅ All UDFs match")

    # Compare critical bucket procedures
    print(f"\n\nCRITICAL BUCKET PROCEDURES:")
    print("-" * 80)

    required_buckets = [
        'PROCESS_VIEWERSHIP_FULL_DATA_GENERIC',
        'PROCESS_VIEWERSHIP_REF_ID_ONLY_GENERIC',
        'PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC',
        'PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC',
        'PROCESS_VIEWERSHIP_SERIES_SEASON_EPISODE_GENERIC',
        'PROCESS_VIEWERSHIP_TITLE_ONLY_GENERIC'
    ]

    staging_missing = []
    prod_missing = []

    for bucket in required_buckets:
        in_staging = bucket in staging_procs
        in_prod = bucket in prod_procs

        status = ""
        if in_staging and in_prod:
            status = "✅"
        elif in_staging and not in_prod:
            status = "⚠️  STAGING ONLY"
            prod_missing.append(bucket)
        elif not in_staging and in_prod:
            status = "⚠️  PROD ONLY"
            staging_missing.append(bucket)
        else:
            status = "❌ MISSING FROM BOTH"
            staging_missing.append(bucket)
            prod_missing.append(bucket)

        print(f"  {status:20s} {bucket}")

    # Summary
    print(f"\n\n" + "=" * 80)
    if not only_staging and not only_prod and not only_staging_udfs and not only_prod_udfs and not staging_missing and not prod_missing:
        print("✅ STAGING AND PROD ARE IN SYNC")
        print("   All procedures and UDFs match")
    else:
        print("❌ STAGING AND PROD ARE OUT OF SYNC")
        if staging_missing:
            print(f"   STAGING missing {len(staging_missing)} critical bucket procedures")
        if prod_missing:
            print(f"   PROD missing {len(prod_missing)} critical bucket procedures")
        if only_staging or only_prod:
            print(f"   {len(only_staging) + len(only_prod)} procedure(s) don't match")
        if only_staging_udfs or only_prod_udfs:
            print(f"   {len(only_staging_udfs) + len(only_prod_udfs)} UDF(s) don't match")
    print("=" * 80)

    conn.close()


if __name__ == '__main__':
    compare_deployments()
