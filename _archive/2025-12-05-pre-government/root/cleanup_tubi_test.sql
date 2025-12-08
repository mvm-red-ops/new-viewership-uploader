-- Clean up Tubi test data for retry
DELETE FROM upload_db.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM test_staging.public.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM staging_assets.public.episode_details_test_staging
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';

DELETE FROM metadata_master_cleaned_staging.public.record_reprocessing_batch_logs
WHERE filename = 'tubi_vod_july.csv';
