#!/usr/bin/env python3
"""Check Philo records in nosey_prod platform_viewership with no content_provider/asset match."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
import toml

SECRETS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    '.streamlit', 'secrets.toml'
)
with open(SECRETS_PATH) as f:
    secrets = toml.load(f)

conn = snowflake.connector.connect(
    user=secrets['snowflake']['user'],
    password=secrets['snowflake']['password'],
    account=secrets['snowflake']['account'],
    warehouse=secrets['snowflake']['warehouse'],
    database='UPLOAD_DB_PROD',
    schema='PUBLIC',
)
cursor = conn.cursor()

PROD_TABLE = 'UPLOAD_DB_PROD.PUBLIC.PLATFORM_VIEWERSHIP'

print("=" * 80)
print("PHILO — Unmatched Records in nosey_prod (upload_db_prod)")
print("  'Unmatched' = ref_id IS NULL OR content_provider IS NULL")
print("=" * 80)
print()

# --- 1. Summary counts ---
cursor.execute(f"""
    SELECT
        COUNT(*)                                                    AS total_philo,
        COUNT(CASE WHEN ref_id IS NULL THEN 1 END)                 AS no_ref_id,
        COUNT(CASE WHEN content_provider IS NULL THEN 1 END)       AS no_content_provider,
        COUNT(CASE WHEN ref_id IS NULL
                    AND content_provider IS NULL THEN 1 END)       AS both_null
    FROM {PROD_TABLE}
    WHERE platform = 'Philo'
""")
row = cursor.fetchone()
total, no_ref, no_cp, both = row
print(f"Total Philo records:              {total:,}")
print(f"  Missing ref_id:                 {no_ref:,}")
print(f"  Missing content_provider:       {no_cp:,}")
print(f"  Missing BOTH (fully unmatched): {both:,}")
print()

if both == 0:
    print("✅ All Philo records have both ref_id and content_provider. Nothing to fix.")
    conn.close()
    sys.exit(0)

# --- 2. Breakdown by filename ---
print("Breakdown by filename (fully unmatched rows):")
cursor.execute(f"""
    SELECT
        filename,
        COUNT(*)                          AS unmatched_rows,
        MIN(date)                         AS earliest_date,
        MAX(date)                         AS latest_date
    FROM {PROD_TABLE}
    WHERE platform = 'Philo'
      AND ref_id IS NULL
      AND content_provider IS NULL
    GROUP BY filename
    ORDER BY unmatched_rows DESC
""")
rows = cursor.fetchall()
for filename, cnt, d_min, d_max in rows:
    print(f"  {filename or '(null)':60s}  {cnt:>6,} rows   [{d_min} → {d_max}]")
print()

# --- 3. Breakdown by platform_content_name (top offenders) ---
print("Top 20 platform_content_name values with no match:")
cursor.execute(f"""
    SELECT
        platform_content_name,
        internal_series,
        platform_content_id,
        COUNT(*) AS row_count
    FROM {PROD_TABLE}
    WHERE platform = 'Philo'
      AND ref_id IS NULL
      AND content_provider IS NULL
    GROUP BY platform_content_name, internal_series, platform_content_id
    ORDER BY row_count DESC
    LIMIT 20
""")
rows = cursor.fetchall()
for content_name, series, content_id, cnt in rows:
    print(f"  [{cnt:>5,}] {content_name or '(null)'}")
    print(f"         internal_series={series or '(null)'}  platform_content_id={content_id or '(null)'}")
print()

# --- 4. Phase distribution ---
print("Phase distribution of unmatched records:")
cursor.execute(f"""
    SELECT
        phase,
        COUNT(*) AS cnt
    FROM {PROD_TABLE}
    WHERE platform = 'Philo'
      AND ref_id IS NULL
      AND content_provider IS NULL
    GROUP BY phase
    ORDER BY phase
""")
for phase, cnt in cursor.fetchall():
    print(f"  Phase {phase or '(null)'}: {cnt:,}")
print()

cursor.close()
conn.close()
