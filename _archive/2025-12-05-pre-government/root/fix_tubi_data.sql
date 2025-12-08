-- Fix existing Tubi data with missing partner/channel
UPDATE upload_db.public.platform_viewership
SET 
    partner = 'Tubi VOD',
    platform_partner_name = 'Tubi VOD',
    channel = 'VOD',
    platform_channel_name = 'VOD'
WHERE filename = 'tubi_vod_july.csv' AND platform = 'Tubi';

UPDATE test_staging.public.platform_viewership
SET 
    partner = 'Tubi VOD',
    platform_partner_name = 'Tubi VOD',
    channel = 'VOD',
    platform_channel_name = 'VOD'
WHERE filename = 'tubi_vod_july.csv' AND platform = 'Tubi';
