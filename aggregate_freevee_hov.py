#!/usr/bin/env python3
"""
Aggregate Freevee minutes streamed to HOV by month and channel
"""

import pandas as pd
import glob
from datetime import datetime


def normalize_channel(channel_name):
    """Normalize channel names"""
    channel_lower = channel_name.lower()

    if 'presented by' in channel_lower:
        return 'Presented by Nosey'
    elif channel_name == 'Nosey':
        return 'Nosey'
    elif channel_name == 'Confess by Nosey':
        return 'Confess by Nosey'
    elif channel_name == 'Judge Nosey':
        return 'Judge Nosey'
    else:
        return channel_name


def main():
    print("=" * 80)
    print("FREEVEE HOV AGGREGATION")
    print("=" * 80)
    print()

    # Read all Freevee files
    files = glob.glob('sample_data/freevee/*.csv')
    print(f"Found {len(files)} Freevee files:")
    for f in files:
        print(f"  - {f}")
    print()

    all_data = []
    for file in files:
        df = pd.read_csv(file)
        all_data.append(df)

    # Combine all data
    combined = pd.concat(all_data, ignore_index=True)
    print(f"Total rows: {len(combined):,}")
    print()

    # Parse date and extract month
    combined['Date'] = pd.to_datetime(combined['Date'])
    combined['Month'] = combined['Date'].dt.to_period('M')

    # Normalize channel names
    combined['Channel_Normalized'] = combined['Channel Name'].apply(normalize_channel)

    print("Channel name mapping:")
    unique_channels = combined[['Channel Name', 'Channel_Normalized']].drop_duplicates()
    for _, row in unique_channels.iterrows():
        print(f"  {row['Channel Name']} → {row['Channel_Normalized']}")
    print()

    # Aggregate by Month and Channel
    aggregated = combined.groupby(['Month', 'Channel_Normalized']).agg({
        'Minutes Streamed': 'sum'
    }).reset_index()

    # Convert minutes to hours
    aggregated['HOV'] = aggregated['Minutes Streamed'] / 60.0

    # Rename columns
    aggregated = aggregated.rename(columns={
        'Channel_Normalized': 'Channel'
    })

    # Sort by Month and Channel
    aggregated = aggregated.sort_values(['Month', 'Channel'])

    # Format output
    print("=" * 80)
    print("RESULTS: HOV by Month and Channel")
    print("=" * 80)
    print()

    # Display results
    for month in aggregated['Month'].unique():
        print(f"{month}:")
        month_data = aggregated[aggregated['Month'] == month]
        for _, row in month_data.iterrows():
            print(f"  {row['Channel']}: {row['HOV']:,.2f} hours")
        print()

    # Save to CSV
    output_df = aggregated[['Month', 'Channel', 'Minutes Streamed', 'HOV']].copy()
    output_df['Month'] = output_df['Month'].astype(str)
    output_df.to_csv('freevee_hov_summary.csv', index=False)

    print("=" * 80)
    print(f"Summary saved to: freevee_hov_summary.csv")
    print("=" * 80)

    # Overall totals
    print()
    print("OVERALL TOTALS BY CHANNEL:")
    overall = aggregated.groupby('Channel')['HOV'].sum().sort_values(ascending=False)
    for channel, hov in overall.items():
        print(f"  {channel}: {hov:,.2f} hours")
    print()
    print(f"GRAND TOTAL: {overall.sum():,.2f} hours")
    print()


if __name__ == '__main__':
    main()
