CREATE OR REPLACE PROCEDURE upload_db.public.set_deal_parent_wurl()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `UPDATE test_staging.public.wurl_viewership r SET deal_parent = sub_r.q_deal_parent, partner = sub_r.partner_name
        FROM (select id,  
          CASE
            WHEN CONTAINS(LOWER(platform_partner_name), 'plex')  THEN 21
            WHEN CONTAINS(LOWER(platform_partner_name), 'samsung')  THEN 18
            WHEN CONTAINS(LOWER(platform_partner_name),  'tcl')  THEN 49
            WHEN CONTAINS(LOWER(platform_partner_name), 'xumo')  THEN 44
            WHEN CONTAINS(LOWER(platform_partner_name), 'vidaa')  THEN 50
            WHEN CONTAINS(LOWER(platform_partner_name), 'xiaomi')  THEN 33
            WHEN CONTAINS(LOWER(platform_partner_name), 'zeasn')  THEN 51
            WHEN CONTAINS(LOWER(platform_partner_name), 'mybundletv')  THEN 58
            WHEN CONTAINS(LOWER(platform_partner_name), 'vizio')  THEN 20
            WHEN CONTAINS(LOWER(platform_partner_name), 'stirr')  THEN 41
            WHEN CONTAINS(LOWER(platform_partner_name), 'sinclair')  THEN 41
            WHEN CONTAINS(LOWER(platform_partner_name), 'imdbtv')  THEN 24
            WHEN CONTAINS(LOWER(platform_partner_name), 'freemoviesplus')  THEN 57
            WHEN CONTAINS(LOWER(platform_partner_name), 'sling')  THEN 55
            WHEN CONTAINS(LOWER(platform_partner_name), 'redbox')  THEN 54
            WHEN CONTAINS(LOWER(platform_partner_name), 'fubo')  THEN 34
            WHEN CONTAINS(LOWER(platform_partner_name), 'philo')  THEN 61
            ELSE  null
          END as q_deal_parent, 
         CASE
            WHEN CONTAINS(LOWER(platform_partner_name), 'plex')  THEN 'Plex'
            WHEN CONTAINS(LOWER(platform_partner_name), 'samsung')  THEN 'SamsungTV+'
            WHEN CONTAINS(LOWER(platform_partner_name), 'tcl')  THEN 'TCL'
            WHEN CONTAINS(LOWER(platform_partner_name), 'xumo')  THEN 'Xumo Linear'
            WHEN CONTAINS(LOWER(platform_partner_name), 'vidaa')  THEN 'Vidaa'
            WHEN CONTAINS(LOWER(platform_partner_name), 'xiaomi')  THEN 'Xiaomi'
            WHEN CONTAINS(LOWER(platform_partner_name), 'zeasn')  THEN 'Zeasn'
            WHEN CONTAINS(LOWER(platform_partner_name), 'mybundletv')  THEN 'MyBundleTV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'freetv')  THEN 'FreeTV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'freemoviesplus')  THEN 'Free Movies Plus'
            WHEN CONTAINS(LOWER(platform_partner_name), 'vizio')  THEN 'Vizio'
            WHEN CONTAINS(LOWER(platform_partner_name), 'imdbtv')  THEN 'IMDB TV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'sinclair')  THEN 'Sinclair/STIRRTV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'sling')  THEN 'Sling TV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'redbox')  THEN 'Redbox'
            WHEN CONTAINS(LOWER(platform_partner_name), 'fubo')  THEN 'Fubo TV'
            WHEN CONTAINS(LOWER(platform_partner_name), 'philo')  THEN 'Philo'
            ELSE  null
          END as partner_name
        from test_staging.public.wurl_viewership
        where processed is null 
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

