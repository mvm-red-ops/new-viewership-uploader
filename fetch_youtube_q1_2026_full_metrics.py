#!/usr/bin/env python3
"""
Fetch YouTube Q1 2026 data: ALL videos, ALL metrics, DAILY breakdown
Run one month at a time by setting START_DATE / END_DATE / OUTPUT_FILE below.
"""

import os
import pickle
import csv
import time
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ── Configure per run ──────────────────────────────────────────────────────────
START_DATE  = '2026-03-01'
END_DATE    = '2026-03-31'
OUTPUT_FILE = 'youtube_march_2026_daily.csv'
# ──────────────────────────────────────────────────────────────────────────────

CHANNEL_ID = 'UCFhu-4h07zEuIjm3ti69Dug'


def get_credentials():
    token_path = os.path.join('youtube_api', 'token.pickle')
    if not os.path.exists(token_path):
        print("❌ No credentials found")
        return None
    with open(token_path, 'rb') as token:
        return pickle.load(token)


def parse_duration(duration_str):
    if not duration_str:
        return 0
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return 0
    return int(match.group(1) or 0) * 3600 + int(match.group(2) or 0) * 60 + int(match.group(3) or 0)


def get_all_video_ids(youtube, channel_id):
    print("Fetching all videos from channel...")
    video_ids = []
    channel_response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        resp = youtube.playlistItems().list(
            part='contentDetails', playlistId=uploads_playlist_id,
            maxResults=50, pageToken=next_page_token
        ).execute()
        for item in resp['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = resp.get('nextPageToken')
        if not next_page_token:
            break

    print(f"  Found {len(video_ids)} total videos")
    return video_ids


def get_video_details(youtube, video_ids):
    print("Fetching video details...")
    video_details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = youtube.videos().list(part='snippet,contentDetails', id=','.join(batch)).execute()
            for item in resp['items']:
                video_details[item['id']] = {
                    'title': item['snippet']['title'],
                    'duration': parse_duration(item['contentDetails']['duration'])
                }
            if (i + 50) % 250 == 0:
                print(f"  Fetched {min(i+50, len(video_ids))}/{len(video_ids)} videos")
            time.sleep(0.2)
        except Exception:
            pass
    print(f"  Got details for {len(video_details)} videos")
    return video_details


def get_video_daily_analytics(youtube_analytics, video_id, start_date, end_date):
    try:
        response = youtube_analytics.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,subscribersGained,estimatedRevenue,cardImpressions',
            dimensions='day',
            filters=f'video=={video_id}'
        ).execute()

        rows = []
        for row in response.get('rows', []):
            rows.append({
                'date': row[0],
                'views': int(row[1]),
                'hours_watched': round(float(row[2]) / 60.0, 4),
                'subscribers': int(row[3]),
                'revenue': round(float(row[4]) if len(row) > 4 else 0.0, 3),
                'impressions': int(row[5]) if len(row) > 5 else 0
            })
        return rows

    except HttpError as e:
        if '429' in str(e):
            print(f"    ⚠️  Rate limit, waiting 60s...")
            time.sleep(60)
            return get_video_daily_analytics(youtube_analytics, video_id, start_date, end_date)
        return []


def main():
    print("=" * 80)
    print(f"YOUTUBE DAILY FETCH: {START_DATE} → {END_DATE}")
    print("=" * 80)
    print()

    creds = get_credentials()
    if not creds:
        return

    youtube_analytics = build('youtubeAnalytics', 'v2', credentials=creds)
    youtube = build('youtube', 'v3', credentials=creds)

    video_ids = get_all_video_ids(youtube, CHANNEL_ID)
    print()
    video_details = get_video_details(youtube, video_ids)
    print()

    print(f"Fetching daily analytics for {len(video_ids)} videos...")
    print()

    csv_data = []
    total_hours = 0.0
    start_time = time.time()

    for i, video_id in enumerate(video_ids):
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(video_ids) - i - 1) / rate / 60
            print(f"  Progress: {i + 1}/{len(video_ids)} (~{remaining:.1f} min remaining)")

        details = video_details.get(video_id, {})
        daily_rows = get_video_daily_analytics(youtube_analytics, video_id, START_DATE, END_DATE)

        for row in daily_rows:
            total_hours += row['hours_watched']
            csv_data.append({
                'video_id':    video_id,
                'title':       details.get('title', 'Unknown'),
                'date':        row['date'],
                'Duration':    details.get('duration', 0),
                'Views':       row['views'],
                'hours_watched': row['hours_watched'],
                'Subscribers': row['subscribers'],
                'Estimated revenue (USD)': row['revenue'],
                'Impressions': row['impressions']
            })

        time.sleep(0.5)

    print()
    print(f"✅ Completed!")
    print(f"   Daily rows: {len(csv_data):,}")
    print(f"   Total hours: {total_hours:,.0f}")
    print()

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['video_id', 'title', 'date', 'Duration', 'Views',
                      'hours_watched', 'Subscribers', 'Estimated revenue (USD)', 'Impressions']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_data:
            writer.writerow(row)

    print("=" * 80)
    print(f"CSV created: {OUTPUT_FILE}")
    print("=" * 80)


if __name__ == '__main__':
    main()
