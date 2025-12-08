--Set the time_since_last_aired column 



--wurl 

CREATE OR REPLACE TEMPORARY TABLE TempWurlViewership AS
SELECT
    partner,
    channel,
    ref_id,
    asset_series,
    standardized_datetime,
    LAG(standardized_datetime) OVER (
        PARTITION BY partner, channel, ref_id, asset_series
        ORDER BY standardized_datetime
    ) AS previous_air_date
FROM wurl_viewership;



MERGE INTO wurl_viewership AS target
USING (
    SELECT
        partner,
        channel,
        ref_id,
        asset_series,
        standardized_datetime,
        MIN(previous_air_date) AS previous_air_date  -- Using MIN to ensure only one row is used for each group
    FROM TempWurlViewership
    GROUP BY partner, channel, ref_id, asset_series, standardized_datetime
) AS source
ON target.partner = source.partner
    AND target.channel = source.channel
    AND target.ref_id = source.ref_id
    AND target.asset_series = source.asset_series
    AND target.standardized_datetime = source.standardized_datetime
WHEN MATCHED AND source.previous_air_date IS NOT NULL THEN
    UPDATE SET target.time_since_last_aired = DATEDIFF(day, source.previous_air_date, target.standardized_datetime);

-- MOVE INSERT 
CREATE OR REPLACE TEMPORARY TABLE TopWurlViewership AS
SELECT
    deal_parent,
    partner,
    channel,
    ref_id,
    asset_series,
    standardized_datetime,
    platform,
    DATEDIFF('second', LAG(standardized_datetime) OVER (
        PARTITION BY deal_parent, partner, channel, ref_id, asset_series, platform
        ORDER BY standardized_datetime ASC
    ), standardized_datetime) / 86400.0 AS time_since_last_aired,  -- Calculate difference in days since last date
    ROW_NUMBER() OVER (
        PARTITION BY deal_parent, partner, channel, ref_id, asset_series, platform, standardized_datetime
        ORDER BY standardized_datetime DESC 
    ) as rn
FROM wurl_viewership
WHERE year IN (2024, 2024) 


INSERT INTO assets.public.rotation_details (
    deal_parent,
    partner,
    channel,
    ref_id,
    asset_series,
    standardized_datetime, 
    platform,
    time_since_last_aired
)
SELECT
    deal_parent,
    partner,
    channel,
    ref_id,
    asset_series,
    standardized_datetime, 
    platform,
    time_since_last_aired
FROM TopWurlViewership
WHERE rn = 1 AND time_since_last_aired IS NOT NULL;



--Amagi
CREATE TEMPORARY TABLE TempAirDates AS
SELECT
    ref_id,
    platform,
    deal_parent,
    channel,
    standardized_datetime,
    device_type, 
    city,
    LAG(standardized_datetime) OVER (
        PARTITION BY ref_id, platform, deal_parent, channel
        ORDER BY standardized_datetime
    ) AS last_aired_datetime
FROM amagi_viewership


UPDATE amagi_viewership
SET time_since_last_aired = DATEDIFF('day', t.last_aired_datetime, amagi_viewership.standardized_datetime)
FROM TempAirDates t
WHERE amagi_viewership.ref_id = t.ref_id
  AND amagi_viewership.platform = t.platform
  AND amagi_viewership.deal_parent = t.deal_parent
  AND amagi_viewership.channel = t.channel
  AND amagi_viewership.standardized_datetime = t.standardized_datetime;

    





