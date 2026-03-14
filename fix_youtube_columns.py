#!/usr/bin/env python3
"""
Fix YouTube CSV to match template column mappings
"""

import pandas as pd

print("Loading YouTube data...")
df = pd.read_csv('youtube_q4_2025_daily_all_metrics.csv')

print(f"Original rows: {len(df):,}")
print(f"Original columns: {list(df.columns)}")
print()

# Rename and add columns to match template
df_fixed = pd.DataFrame()

df_fixed['date'] = df['date']
df_fixed['video_id'] = df['Content']
df_fixed['title'] = df['Video title']
df_fixed['published_date'] = df['Video publish time']
df_fixed['duration_seconds'] = df['Duration']
df_fixed['views'] = df['Views']
df_fixed['hours_watched'] = df['Watch time (hours)']

# Calculate minutes_watched from hours_watched
df_fixed['minutes_watched'] = df_fixed['hours_watched'] * 60.0

# Calculate avg_view_duration_seconds
# avg_view_duration = (hours_watched * 3600) / views
# Handle division by zero
df_fixed['avg_view_duration_seconds'] = 0.0
mask = df_fixed['views'] > 0
df_fixed.loc[mask, 'avg_view_duration_seconds'] = (df_fixed.loc[mask, 'hours_watched'] * 3600) / df_fixed.loc[mask, 'views']

# Determine is_short (YouTube Shorts are <= 60 seconds)
df_fixed['is_short'] = df_fixed['duration_seconds'] <= 60

# Construct URL from video_id
df_fixed['url'] = 'https://www.youtube.com/watch?v=' + df_fixed['video_id']

# Reorder columns to match template
column_order = [
    'avg_view_duration_seconds',
    'date',
    'duration_seconds',
    'hours_watched',
    'is_short',
    'minutes_watched',
    'published_date',
    'title',
    'url',
    'video_id',
    'views'
]

df_final = df_fixed[column_order]

# Save
output_file = 'youtube_q4_2025_daily_all_metrics.csv'
df_final.to_csv(output_file, index=False)

print("Fixed columns:")
for i, col in enumerate(df_final.columns):
    print(f"  {i}:\"{col}\"")
print()

print(f"Saved to: {output_file}")
print(f"Total rows: {len(df_final):,}")
print()

# Show sample
print("Sample data:")
print(df_final.head(3).to_string())
print()

# Summary stats
print("Summary:")
print(f"  Date range: {df_final['date'].min()} to {df_final['date'].max()}")
print(f"  Unique videos: {df_final['video_id'].nunique():,}")
print(f"  Total views: {df_final['views'].sum():,.0f}")
print(f"  Total hours watched: {df_final['hours_watched'].sum():,.2f}")
print(f"  Shorts: {df_final['is_short'].sum():,} records")
print(f"  Regular videos: {(~df_final['is_short']).sum():,} records")
