#!/usr/bin/env python3
"""
Check platform_viewership table schema in test_Staging and upload_db
"""
import snowflake.connector
import streamlit as st
from config import load_snowflake_config

def check_table_schema(database, schema_name, table_name):
    """Check if table exists and get its schema"""
    try:
        sf_config = load_snowflake_config()
        conn = snowflake.connector.connect(**sf_config)
        cursor = conn.cursor()

        # Switch to the specified database
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        print(f"\n{'='*80}")
        print(f"Checking {database}.{schema_name}.{table_name}")
        print(f"{'='*80}")

        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name.upper()}'
            AND TABLE_NAME = '{table_name.upper()}'
        """)
        exists = cursor.fetchone()[0] > 0

        if not exists:
            print(f"❌ Table {table_name} does NOT exist in {database}.{schema_name}")
            return

        print(f"✓ Table {table_name} EXISTS in {database}.{schema_name}")

        # Get column details
        cursor.execute(f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name.upper()}'
            AND TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
        """)

        columns = cursor.fetchall()
        print(f"\nColumns ({len(columns)} total):")
        print("-" * 80)

        # Check for specific columns
        revenue_col = None
        label_col = None

        for col in columns:
            col_name, data_type, nullable, default = col
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            print(f"  {col_name:30} {data_type:20} {nullable_str:10}{default_str}")

            if col_name.upper() == 'REVENUE':
                revenue_col = col_name
            if col_name.upper() == 'LABEL':
                label_col = col_name

        print("\n" + "=" * 80)
        print("Key Column Check:")
        print("=" * 80)
        if revenue_col:
            print(f"✓ REVENUE column EXISTS: {revenue_col}")
        else:
            print("❌ REVENUE column NOT FOUND")

        if label_col:
            print(f"✓ LABEL column EXISTS: {label_col}")
        else:
            print("❌ LABEL column NOT FOUND")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error checking {database}.{schema_name}.{table_name}: {str(e)}")

if __name__ == "__main__":
    # Check test_Staging
    check_table_schema("test_Staging", "public", "platform_viewership")

    # Check upload_db
    check_table_schema("upload_db", "public", "platform_viewership")
