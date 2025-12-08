CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_TERRITORY_AMAGI()
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS 
$$
    var sql_command = 
     `UPDATE test_staging.public.amagi_viewership r
        SET r.territory = q.territory_name, r.territory_id = q.t_id 
        FROM (
            SELECT r.id as r_id,
            CASE
                WHEN UPPER(PLATFORM_CHANNEL_NAME) = 'REALNOSEY_LG' THEN 'United Kingdom'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*TABLO' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*FREETV' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*LITTLSTAR' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*US' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NORTHAMERICA' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CINEVERSE' THEN 'United States'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CA' THEN 'Canada'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*AU' THEN 'Australia'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*MX' THEN 'Mexico'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*GB.*' THEN 'United Kingdom'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*INTL*' THEN 'International'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*IN' THEN 'India'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NZ' THEN 'New Zealand'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*_INTL_1_KLOWDTV' THEN 'International'
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NOSEY_INTL_1_LG' THEN 'International'
                ELSE 'Unspecified'
            END as territory_name,
            CASE
                WHEN UPPER(PLATFORM_CHANNEL_NAME) = 'REALNOSEY_LG' THEN 5
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*TABLO' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*FREETV' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*LITTLSTAR' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*US' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NORTHAMERICA' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CINEVERSE' THEN 1
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*CA' THEN 4
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*AU' THEN 10
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*MX' THEN 8
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*GB.*' THEN 5
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*INTL*' THEN 2
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*IN' THEN 6
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NZ' THEN 13
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*_INTL_1_KLOWDTV' THEN 13
                WHEN UPPER(PLATFORM_CHANNEL_NAME) REGEXP '\.*NOSEY_INTL_1_LG' THEN 2
                ELSE 0
            END as t_id
            FROM test_staging.public.amagi_viewership r
            WHERE territory_id IS NULL AND processed IS NULL
        ) q
        WHERE r.id = q.r_id `;
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