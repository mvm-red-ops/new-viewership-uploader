import streamlit as st
import pandas as pd
from typing import Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.snowflake_utils import SnowflakeConnection
from config import get_environment_name, load_snowflake_config

st.set_page_config(page_title="Viewership Discrepancy Checker", page_icon="🔍", layout="wide")

st.title("🔍 Viewership Discrepancy Checker")
st.markdown("Compare **platform_viewership.tot_hov** (staging) vs **episode_details.hours** (final)")

# Environment indicator
env_name = get_environment_name()
env_color = "green" if env_name == "PRODUCTION" else "blue"

# Show config info for verification
sf_config = load_snowflake_config()
st.markdown(f"**Environment:** :{env_color}[{env_name}] | **Database:** {sf_config['database']} | **Role:** {sf_config['role']}")

st.divider()

# Initialize connection (uses same config as main app)
@st.cache_resource
def get_snowflake_connection():
    try:
        return SnowflakeConnection()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

conn = get_snowflake_connection()

if not conn:
    st.stop()

# Filters section
st.subheader("Filters")

col1, col2, col3, col4, col5 = st.columns(5)

# Determine table names based on environment
cursor = conn.cursor
if env_name == "PRODUCTION":
    staging_table = "NOSEY_PROD.PUBLIC.platform_viewership"
    final_table = "STAGING_ASSETS.PUBLIC.EPISODE_DETAILS"
else:
    staging_table = "TEST_STAGING.PUBLIC.platform_viewership"
    final_table = "STAGING_ASSETS.PUBLIC.EPISODE_DETAILS_TEST_STAGING"

with col1:
    # Get available platforms
    try:
        cursor.execute(f"SELECT DISTINCT platform FROM {staging_table} WHERE platform IS NOT NULL ORDER BY platform")
        platforms = [row[0] for row in cursor.fetchall()]
        platform = st.selectbox("Platform", options=["All"] + platforms)
    except Exception as e:
        st.error(f"Error loading platforms: {str(e)}")
        platform = "All"

with col2:
    # Get available partners
    try:
        cursor.execute(f"SELECT DISTINCT partner FROM {staging_table} WHERE partner IS NOT NULL ORDER BY partner")
        partners = [row[0] for row in cursor.fetchall()]
        partner = st.selectbox("Partner", options=["All"] + partners)
    except Exception as e:
        st.error(f"Error loading partners: {str(e)}")
        partner = "All"

with col3:
    # Get available channels
    try:
        cursor.execute(f"SELECT DISTINCT channel FROM {staging_table} WHERE channel IS NOT NULL ORDER BY channel")
        channels = [row[0] for row in cursor.fetchall()]
        channel = st.selectbox("Channel", options=["All"] + channels)
    except Exception as e:
        st.error(f"Error loading channels: {str(e)}")
        channel = "All"

with col4:
    # Get available years
    try:
        cursor.execute(f"SELECT DISTINCT year FROM {staging_table} WHERE year IS NOT NULL ORDER BY year DESC")
        years = [row[0] for row in cursor.fetchall()]
        year = st.selectbox("Year", options=["All"] + [str(y) for y in years])
    except Exception as e:
        st.error(f"Error loading years: {str(e)}")
        year = "All"

with col5:
    # Get available quarters
    try:
        cursor.execute(f"SELECT DISTINCT quarter FROM {staging_table} WHERE quarter IS NOT NULL ORDER BY quarter")
        quarters = [row[0] for row in cursor.fetchall()]
        quarter = st.selectbox("Quarter", options=["All"] + [str(q) for q in quarters])
    except Exception as e:
        st.error(f"Error loading quarters: {str(e)}")
        quarter = "All"

# Build WHERE clause
where_conditions = []
if platform != "All":
    where_conditions.append(f"platform = '{platform}'")
if partner != "All":
    where_conditions.append(f"partner = '{partner}'")
if channel != "All":
    where_conditions.append(f"channel = '{channel}'")
if year != "All":
    where_conditions.append(f"year = {year}")
if quarter != "All":
    where_conditions.append(f"quarter = '{quarter}'")

where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

st.divider()

# Run comparison button
if st.button("🔎 Check for Discrepancies", type="primary"):
    with st.spinner("Comparing viewership data..."):
        try:
            # Query platform_viewership (staging)
            staging_query = f"""
                SELECT
                    platform,
                    partner,
                    channel,
                    year,
                    quarter,
                    month,
                    COUNT(*) as record_count,
                    SUM(COALESCE(tot_hov, 0)) as staging_tot_hov
                FROM {staging_table}
                WHERE {where_clause}
                    AND processed = TRUE
                GROUP BY platform, partner, channel, year, quarter, month
                ORDER BY platform, partner, channel, year, quarter, month
            """

            cursor.execute(staging_query)
            staging_results = cursor.fetchall()
            staging_df = pd.DataFrame(
                staging_results,
                columns=['platform', 'partner', 'channel', 'year', 'quarter', 'month', 'record_count', 'staging_tot_hov']
            )

            # Query episode_details (final) - uses HOURS not TOT_HOV
            final_query = f"""
                SELECT
                    platform,
                    partner,
                    channel,
                    year,
                    quarter,
                    month,
                    COUNT(*) as record_count,
                    SUM(COALESCE(hours, 0)) as final_hours
                FROM {final_table}
                WHERE {where_clause}
                GROUP BY platform, partner, channel, year, quarter, month
                ORDER BY platform, partner, channel, year, quarter, month
            """

            cursor.execute(final_query)
            final_results = cursor.fetchall()
            final_df = pd.DataFrame(
                final_results,
                columns=['platform', 'partner', 'channel', 'year', 'quarter', 'month', 'record_count', 'final_hours']
            )

            # Merge the dataframes
            comparison_df = staging_df.merge(
                final_df,
                on=['platform', 'partner', 'channel', 'year', 'quarter', 'month'],
                how='outer',
                suffixes=('_staging', '_final')
            )

            # Calculate discrepancies
            comparison_df['record_count_staging'] = comparison_df['record_count_staging'].fillna(0).astype(int)
            comparison_df['record_count_final'] = comparison_df['record_count_final'].fillna(0).astype(int)
            comparison_df['record_count_diff'] = comparison_df['record_count_final'] - comparison_df['record_count_staging']

            # Compare staging tot_hov vs final hours
            comparison_df['staging_tot_hov'] = comparison_df['staging_tot_hov'].fillna(0)
            comparison_df['final_hours'] = comparison_df['final_hours'].fillna(0)
            comparison_df['hours_diff'] = comparison_df['final_hours'] - comparison_df['staging_tot_hov']
            comparison_df['hours_diff_pct'] = ((comparison_df['hours_diff'] / comparison_df['staging_tot_hov']) * 100).replace([float('inf'), -float('inf')], 0).fillna(0)

            # Display summary
            st.subheader("📊 Summary")

            col1, col2, col3 = st.columns(3)

            with col1:
                total_files = len(comparison_df)
                st.metric("Total File Groups", total_files)

            with col2:
                # Files with discrepancies (any difference > 0.01%)
                discrepancy_threshold = 0.01
                files_with_discrepancies = len(comparison_df[
                    (abs(comparison_df['hours_diff_pct']) > discrepancy_threshold) |
                    (comparison_df['record_count_diff'] != 0)
                ])
                st.metric("Groups with Discrepancies", files_with_discrepancies)

            with col3:
                match_rate = ((total_files - files_with_discrepancies) / total_files * 100) if total_files > 0 else 0
                st.metric("Match Rate", f"{match_rate:.1f}%")

            st.divider()

            # Display discrepancies
            if files_with_discrepancies > 0:
                st.subheader("⚠️ Discrepancies Found")

                discrepancy_df = comparison_df[
                    (abs(comparison_df['hours_diff_pct']) > discrepancy_threshold) |
                    (comparison_df['record_count_diff'] != 0)
                ].copy()

                # Format for display
                display_df = discrepancy_df[[
                    'platform', 'partner', 'channel', 'year', 'quarter', 'month',
                    'record_count_staging', 'record_count_final', 'record_count_diff',
                    'staging_tot_hov', 'final_hours', 'hours_diff', 'hours_diff_pct'
                ]]

                # Round numeric columns
                numeric_cols = ['staging_tot_hov', 'final_hours', 'hours_diff', 'hours_diff_pct']
                display_df[numeric_cols] = display_df[numeric_cols].round(2)

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400
                )

                # Export option
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Discrepancies as CSV",
                    data=csv,
                    file_name=f"viewership_discrepancies_{platform}_{partner}_{channel}_{year}_{quarter}.csv",
                    mime="text/csv"
                )
            else:
                st.success("✅ No discrepancies found! All staging data matches final table.")

            # Full comparison table (expandable)
            with st.expander("📋 View Full Comparison Table"):
                display_all_df = comparison_df[[
                    'platform', 'partner', 'channel', 'year', 'quarter', 'month',
                    'record_count_staging', 'record_count_final', 'record_count_diff',
                    'staging_tot_hov', 'final_hours', 'hours_diff', 'hours_diff_pct'
                ]]

                numeric_cols = ['staging_tot_hov', 'final_hours', 'hours_diff', 'hours_diff_pct']
                display_all_df[numeric_cols] = display_all_df[numeric_cols].round(2)

                st.dataframe(display_all_df, use_container_width=True, height=600)

        except Exception as e:
            st.error(f"Error during comparison: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

st.divider()

# Help section
with st.expander("ℹ️ How to use this tool"):
    st.markdown("""
    ### Purpose
    This tool helps you identify discrepancies between:
    - **Staging table** (`platform_viewership`): Where data lands after upload and normalization
    - **Final table** (`episode_details`): Where data is stored after asset matching and final processing

    ### What to check
    - **Record count differences**: Missing or extra records in final table
    - **Hours comparison**: Staging `tot_hov` vs Final `hours` - should match after processing

    ### When to use this
    - After data processing to verify data integrity
    - When investigating data loss or duplication issues
    - Before generating reports to ensure data accuracy

    ### Interpreting results
    - **0% difference**: Perfect match (expected)
    - **Small % difference (<1%)**: May be due to rounding or data transformations
    - **Large % difference (>5%)**: Investigate for data loss or processing errors
    - **Record count mismatch**: Check if records were filtered out during asset matching
    """)
