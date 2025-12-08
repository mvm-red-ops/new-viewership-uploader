#!/usr/bin/env python3
"""
Check episode_details table and query actual data with a filename
"""
import snowflake.connector
import streamlit as st
from config import load_snowflake_config

def check_episode_details():
    """Check episode_details table schema"""
    try:
        sf_config = load_snowflake_config()
        conn = snowflake.connector.connect(**sf_config)
        cursor = conn.cursor()

        print(f"\n{'='*80}")
        print(f"Checking assets.public.episode_details table")
        print(f"{'='*80}")

        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*)
            FROM assets.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'PUBLIC'
            AND TABLE_NAME = 'EPISODE_DETAILS'
        """)
        exists = cursor.fetchone()[0] > 0

        if not exists:
            print(f"❌ Table episode_details does NOT exist in assets.public")
            return

        print(f"✓ Table episode_details EXISTS in assets.public")

        # Get column details
        cursor.execute("""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE
            FROM assets.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'PUBLIC'
            AND TABLE_NAME = 'EPISODE_DETAILS'
            ORDER BY ORDINAL_POSITION
        """)

        columns = cursor.fetchall()
        print(f"\nColumns ({len(columns)} total):")
        print("-" * 80)

        label_col = None
        for col in columns:
            col_name, data_type, nullable = col
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            print(f"  {col_name:30} {data_type:20} {nullable_str:10}")
            if col_name.upper() == 'LABEL':
                label_col = col_name

        print("\n" + "=" * 80)
        if label_col:
            print(f"✓ LABEL column EXISTS in episode_details: {label_col}")

            # Query some sample label values
            print(f"\nQuerying sample LABEL values from episode_details:")
            cursor.execute("""
                SELECT DISTINCT LABEL
                FROM assets.public.episode_details
                WHERE LABEL IS NOT NULL
                LIMIT 10
            """)
            labels = cursor.fetchall()
            for label in labels:
                print(f"  - {label[0]}")
        else:
            print("❌ LABEL column NOT FOUND in episode_details")

        # Get a sample filename from platform_viewership
        print(f"\n{'='*80}")
        print(f"Checking platform_viewership for sample data")
        print(f"{'='*80}")

        cursor.execute("""
            SELECT DISTINCT FILENAME
            FROM test_Staging.public.platform_viewership
            WHERE FILENAME IS NOT NULL
            LIMIT 5
        """)
        filenames = cursor.fetchall()

        if filenames:
            print(f"\nFound {len(filenames)} sample filenames:")
            for fname in filenames:
                print(f"  - {fname[0]}")

            # Pick first filename and query its data
            sample_filename = filenames[0][0]
            print(f"\n{'='*80}")
            print(f"Querying data for filename: {sample_filename}")
            print(f"{'='*80}")

            cursor.execute(f"""
                SELECT
                    PLATFORM, DATE, PLATFORM_CONTENT_NAME, REVENUE, TOT_HOV, TOT_MOV, FILENAME
                FROM test_Staging.public.platform_viewership
                WHERE FILENAME = '{sample_filename}'
                LIMIT 3
            """)
            rows = cursor.fetchall()

            if rows:
                print(f"\nSample data from platform_viewership ({len(rows)} rows):")
                print("-" * 80)
                for row in rows:
                    platform, date, content, revenue, hov, mov, fname = row
                    print(f"  Platform: {platform}")
                    print(f"  Date: {date}")
                    print(f"  Content: {content}")
                    print(f"  Revenue: {revenue}")
                    print(f"  TOT_HOV: {hov}")
                    print(f"  TOT_MOV: {mov}")
                    print(f"  Filename: {fname}")
                    print("-" * 80)

                # Check if this file has revenue or viewership data
                if rows[0][3] is not None and rows[0][3] != 0:
                    print(f"\n✓ This appears to be REVENUE data (has Revenue values)")
                elif (rows[0][4] is not None and rows[0][4] != 0) or (rows[0][5] is not None and rows[0][5] != 0):
                    print(f"\n✓ This appears to be VIEWERSHIP data (has TOT_HOV/TOT_MOV values)")
                else:
                    print(f"\n? Unable to determine data type from values")

                # Now check if this data made it to episode_details and what label it has
                print(f"\n{'='*80}")
                print(f"Checking if this data exists in episode_details with a LABEL")
                print(f"{'='*80}")

                if label_col:
                    # Try to find matching records in episode_details by date and content
                    cursor.execute(f"""
                        SELECT LABEL, DATE, SERIES, EPISODE_TITLE
                        FROM assets.public.episode_details
                        WHERE DATE = '{rows[0][1]}'
                        LIMIT 5
                    """)
                    ep_rows = cursor.fetchall()

                    if ep_rows:
                        print(f"\nFound {len(ep_rows)} matching records in episode_details for date {rows[0][1]}:")
                        for ep_row in ep_rows:
                            label, date, series, title = ep_row
                            print(f"  Label: {label}, Date: {date}, Series: {series}, Title: {title[:50] if title else 'N/A'}")
                    else:
                        print(f"❌ No matching records found in episode_details for this date")

        else:
            print("❌ No filenames found in platform_viewership")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_episode_details()
