#!/usr/bin/env python3
"""
Fetch YouTube Q4 2025 viewership data for ALL videos with daily breakdown
Uses proper rate limiting to avoid quota issues
"""

import os
import pickle
import csv
import time
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

    # Get all videos from uploads playlist
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


def get_video_daily_data(youtube_analytics, video_id, start_date, end_date):
    """Get daily analytics for a specific video"""
    try:
        response = youtube_analytics.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='estimatedMinutesWatched',
            dimensions='day',
            filters=f'video=={video_id}'
        ).execute()

        if 'rows' in response:
            return response['rows']
        return []

    except HttpError as e:
        if '429' in str(e):  # Rate limit
            print(f"    ⚠️  Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            return get_video_daily_data(youtube_analytics, video_id, start_date, end_date)
        elif '403' in str(e):  # Forbidden
            return []  # Skip videos we don't have access to
        else:
            print(f"    ⚠️  Error for video {video_id}: {e}")
            return []


def get_video_titles(youtube, video_ids):
    """Get titles for video IDs with batching"""
    print("Fetching video titles...")
    video_titles = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            response = youtube.videos().list(
                part='snippet',
                id=','.join(batch)
            ).execute()

            for item in response['items']:
                video_titles[item['id']] = item['snippet']['title']

            if (i + 50) % 250 == 0:
                print(f"  Fetched titles for {min(i+50, len(video_ids))}/{len(video_ids)} videos")

            time.sleep(0.2)  # Rate limiting
        except:
            pass

    print(f"  Got titles for {len(video_titles)} videos")
    return video_titles


def main():
    print("=" * 80)
    print("YOUTUBE Q4 2025 - ALL VIDEOS WITH DAILY BREAKDOWN")
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

    # Get all video IDs from channel
    video_ids = get_all_video_ids(youtube, CHANNEL_ID)
    print()

    # Get video titles
    video_titles = get_video_titles(youtube, video_ids)
    print()

    # Fetch daily data for each video
    print(f"Fetching daily analytics for {len(video_ids)} videos...")
    print("(Using rate limiting: ~2 videos per second)")
    print()

    csv_data = []
    total_hours = 0.0
    videos_with_data = 0
    start_time = time.time()

    for i, video_id in enumerate(video_ids):
        # Progress indicator
        if (i + 1) % 50 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(video_ids) - i - 1) / rate / 60
            print(f"  Progress: {i + 1}/{len(video_ids)} videos ({rate:.1f}/sec, ~{remaining:.1f} min remaining)")

        daily_rows = get_video_daily_data(youtube_analytics, video_id, START_DATE, END_DATE)

        if daily_rows:
            videos_with_data += 1
            title = video_titles.get(video_id, f'Video {video_id}')

            for row in daily_rows:
                date = row[0]
                minutes_watched = float(row[1])
                hours_watched = minutes_watched / 60.0

                if hours_watched > 0:  # Only include days with watch time
                    total_hours += hours_watched

                    csv_data.append({
                        'date': date,
                        'title': title,
                        'hours_watched': round(hours_watched, 2)
                    })

        # Rate limiting: 2 requests per second
        time.sleep(0.5)

    print()
    print(f"✅ Completed!")
    print(f"   Daily records: {len(csv_data):,}")
    print(f"   Videos with watch time: {videos_with_data}/{len(video_ids)}")
    print(f"   Total watch time: {total_hours:,.2f} hours")
    print()

    # Write CSV
    csv_filename = 'youtube_q4_2025_all_videos_daily.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'title', 'hours_watched']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)

    print("=" * 80)
    print("SUCCESS!")
    print("=" * 80)
    print(f"CSV created: {csv_filename}")
    print()
    print("Format: date (actual viewing date), title, hours_watched per day")
    print("Ready to upload with YouTube template!")
    print("=" * 80)


if __name__ == '__main__':
    main()
