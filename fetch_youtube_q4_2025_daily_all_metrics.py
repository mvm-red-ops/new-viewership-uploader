#!/usr/bin/env python3
"""
Fetch YouTube Q4 2025 data: ALL videos, ALL metrics, DAILY breakdown
"""

import os
import pickle
import csv
import time
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


START_DATE = '2025-10-01'
END_DATE = '2025-12-31'
CHANNEL_ID = 'UCFhu-4h07zEuIjm3ti69Dug'


def get_credentials():
    token_path = os.path.join('youtube_api', 'token.pickle')
    if not os.path.exists(token_path):
        print(f"❌ No credentials found")
        return None
    with open(token_path, 'rb') as token:
        return pickle.load(token)


def parse_duration(duration_str):
    """Parse ISO 8601 duration (PT1H30M15S) to seconds"""
    if not duration_str:
        return 0

    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)

    if not match:
        return 0

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    return hours * 3600 + minutes * 60 + seconds


def get_all_video_ids(youtube, channel_id):
    """Get ALL video IDs from the channel"""
    print("Fetching all videos from channel...")
    video_ids = []

    # Get uploads playlist ID
    channel_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()

    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # Get all videos
    next_page_token = None
    while True:
        playlist_response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in playlist_response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break

    print(f"  Found {len(video_ids)} total videos")
    return video_ids


def get_video_details(youtube, video_ids):
    """Get video metadata: title, publish date, duration"""
    print("Fetching video details...")
    video_details = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            response = youtube.videos().list(
                part='snippet,contentDetails',
                id=','.join(batch)
            ).execute()

            for item in response['items']:
                duration_seconds = parse_duration(item['contentDetails']['duration'])

                video_details[item['id']] = {
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt'][:10],  # YYYY-MM-DD
                    'duration': duration_seconds
                }

            if (i + 50) % 250 == 0:
                print(f"  Fetched {min(i+50, len(video_ids))}/{len(video_ids)} videos")

            time.sleep(0.2)
        except:
            pass

    print(f"  Got details for {len(video_details)} videos")
    return video_details


def get_video_daily_analytics(youtube_analytics, video_id, start_date, end_date):
    """Get daily analytics for a single video with ALL metrics"""
    try:
        response = youtube_analytics.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,subscribersGained,estimatedRevenue,cardImpressions',
            dimensions='day',
            filters=f'video=={video_id}'
        ).execute()

        daily_data = []
        if 'rows' in response:
            for row in response['rows']:
                daily_data.append({
                    'date': row[0],  # YYYY-MM-DD
                    'views': int(row[1]),
                    'watch_time_minutes': float(row[2]),
                    'watch_time_hours': float(row[2]) / 60.0,
                    'subscribers': int(row[3]),
                    'revenue': float(row[4]) if len(row) > 4 else 0.0,
                    'impressions': int(row[5]) if len(row) > 5 else 0
                })

        return daily_data

    except HttpError as e:
        if '429' in str(e):
            print(f"    ⚠️  Rate limit, waiting 60s...")
            time.sleep(60)
            return get_video_daily_analytics(youtube_analytics, video_id, start_date, end_date)
        elif '500' in str(e):
            # Backend error - skip this video
            return []
        return []


def main():
    print("=" * 80)
    print("YOUTUBE Q4 2025 - ALL VIDEOS, ALL METRICS, DAILY BREAKDOWN")
    print("=" * 80)
    print()
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Channel: {CHANNEL_ID}")
    print()
    print("This will take 15-30 minutes due to API rate limits...")
    print()

    # Load credentials
    creds = get_credentials()
    if not creds:
        return

    youtube_analytics = build('youtubeAnalytics', 'v2', credentials=creds)
    youtube = build('youtube', 'v3', credentials=creds)

    # Get all video IDs
    video_ids = get_all_video_ids(youtube, CHANNEL_ID)
    print()

    # Get video details
    video_details = get_video_details(youtube, video_ids)
    print()

    # Fetch daily analytics for each video
    print(f"Fetching daily analytics for {len(video_ids)} videos...")
    print("(Using rate limiting: ~2 videos per second)")
    print()

    csv_data = []
    start_time = time.time()

    for i, video_id in enumerate(video_ids):
        if (i + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(video_ids) - i - 1) / rate / 60
            print(f"  Progress: {i + 1}/{len(video_ids)} videos ({rate:.1f}/sec, ~{remaining:.1f} min remaining)")

        details = video_details.get(video_id, {})
        daily_data = get_video_daily_analytics(youtube_analytics, video_id, START_DATE, END_DATE)

        for day_record in daily_data:
            csv_data.append({
                'date': day_record['date'],
                'Content': video_id,
                'Video title': details.get('title', 'Unknown'),
                'Video publish time': details.get('published_at', ''),
                'Duration': details.get('duration', 0),
                'Views': day_record['views'],
                'Watch time (hours)': round(day_record['watch_time_hours'], 4),
                'Subscribers': day_record['subscribers'],
                'Estimated revenue (USD)': round(day_record['revenue'], 3),
                'Impressions': day_record['impressions']
            })

        time.sleep(0.5)  # Rate limiting

    print()
    print(f"✅ Completed!")
    print(f"   Daily records: {len(csv_data):,}")
    print()

    # Write CSV
    csv_filename = 'youtube_q4_2025_daily_all_metrics.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'date', 'Content', 'Video title', 'Video publish time', 'Duration',
            'Views', 'Watch time (hours)', 'Subscribers',
            'Estimated revenue (USD)', 'Impressions'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in csv_data:
            writer.writerow(row)

    print("=" * 80)
    print("SUCCESS!")
    print("=" * 80)
    print(f"CSV created: {csv_filename}")
    print()
    print(f"Format: Daily breakdown with ALL metrics")
    print(f"Columns: date, Content, Video title, Video publish time, Duration,")
    print(f"         Views, Watch time (hours), Subscribers, Revenue, Impressions")
    print("=" * 80)


if __name__ == '__main__':
    main()
