-- update territory_id field based on territory abbreviation
CREATE OR REPLACE PROCEDURE upload_db.public.set_territory_amagi()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `update test_staging.public.amagi_viewership r
        set r.territory = q.territory_name, r.territory_id = q.t_id 
        from (
            select r.id as r_id,
            CASE
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CA' THEN 'Canada'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*AU' THEN 'Australia'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*MX' THEN 'Mexico'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*GB.*' THEN 'United Kingdom'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*INTL*' THEN 'International'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*_INTL_1_KLOWDTV' THEN 'International'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*IN' THEN 'India'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*US' THEN 'United States'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NZ' THEN 'New Zealand'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*REALNOSEY_LG' THEN 'International'
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NOSEY_INTL_1_LG' THEN 'International'
              ELSE 'Unspecified'
            END as territory_name,
            CASE
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CA' THEN 4
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*AU' THEN 10
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*MX' THEN 8
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*INTL*' THEN 2
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*IN' THEN 6
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*GB.*' THEN 5
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*US' THEN 1
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NZ' THEN 13
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*REALNOSEY_LG' THEN 2
              WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NOSEY_INTL_1_LG' THEN 2
              ELSE 0
            END as t_id
            from test_staging.public.amagi_viewership r
            where territory_id IS NULL AND processed IS NULL
        ) q
        where r.id = q.r_id `;
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