-- update  based on date_unformatted field 
CREATE OR REPLACE PROCEDURE upload_db.public.set_deal_parent_amagi()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `UPDATE test_staging.public.amagi_viewership r SET deal_parent = sub_r.q_deal_parent, partner = sub_r.partner_name
        FROM (select id, platform_channel_name, 
          CASE
              WHEN CONTAINS(LOWER(platform_channel_name), 'samsung')  THEN 23
              WHEN CONTAINS(LOWER(platform_channel_name), 'rlaxx')  THEN 22
              WHEN CONTAINS(LOWER(platform_channel_name), 'distro')  THEN 24
              WHEN CONTAINS(LOWER(platform_channel_name), 'freebie')  THEN 27
              WHEN CONTAINS(LOWER(platform_channel_name), 'klowd')  THEN 28
              WHEN CONTAINS(LOWER(platform_channel_name), 'lg')  THEN 30
              WHEN CONTAINS(LOWER(platform_channel_name), 'fetch')  THEN 31
              WHEN CONTAINS(LOWER(platform_channel_name), 'fubo')  THEN 34
              WHEN CONTAINS(LOWER(platform_channel_name), 'tubi')  THEN 39
              WHEN CONTAINS(LOWER(platform_channel_name), 'littlstar')  THEN 36
              WHEN CONTAINS(LOWER(platform_channel_name), 'freetv')  THEN 47
              WHEN CONTAINS(LOWER(platform_channel_name), 'stremium')  THEN 48
              WHEN CONTAINS(LOWER(platform_channel_name), 'netgem')  THEN 69
              WHEN CONTAINS(LOWER(platform_channel_name), 'tablo')  THEN 63
              WHEN CONTAINS(LOWER(platform_channel_name), 'ott studios')  THEN 59
              WHEN CONTAINS(LOWER(platform_channel_name), 'cineverse')  THEN 70
            ELSE  null
          END as q_deal_parent,
           CASE
              WHEN CONTAINS(LOWER(platform_channel_name), 'samsung')  THEN 'SamsungTV+'
              WHEN CONTAINS(LOWER(platform_channel_name), 'rlaxx')  THEN 'Rlaxx'
              WHEN CONTAINS(LOWER(platform_channel_name), 'distro')  THEN 'DistroTV'
              WHEN CONTAINS(LOWER(platform_channel_name), 'freebie')  THEN 'Freebie TV'
              WHEN CONTAINS(LOWER(platform_channel_name), 'klowd')  THEN 'KlowdTV'
              WHEN CONTAINS(LOWER(platform_channel_name), 'lg')  THEN 'LG'
              WHEN CONTAINS(LOWER(platform_channel_name), 'fetch')  THEN 'FetchTV'
              WHEN CONTAINS(LOWER(platform_channel_name), 'fubo')  THEN 'Fubo'
              WHEN CONTAINS(LOWER(platform_channel_name), 'tubi')  THEN 'Tubi'
              WHEN CONTAINS(LOWER(platform_channel_name), 'littlstar')  THEN 'Littlstar'
              WHEN CONTAINS(LOWER(platform_channel_name), 'freetv')  THEN 'FreeTV'
              WHEN CONTAINS(LOWER(platform_channel_name), 'stremium')  THEN 'Stremium'
              WHEN CONTAINS(LOWER(platform_channel_name), 'netgem')  THEN 'Netgem'
              WHEN CONTAINS(LOWER(platform_channel_name), 'tablo')  THEN 'Tablo'
              WHEN CONTAINS(LOWER(platform_channel_name), 'ott studios')  THEN 'Free Movies Plus'
              WHEN CONTAINS(LOWER(platform_channel_name), 'cineverse')  THEN 'Cineverse'
            ELSE  null
          END as partner_name
        from test_staging.public.amagi_viewership
        where deal_parent is null and processed is null
      ) as sub_r
      WHERE r.id = sub_r.id `;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$;