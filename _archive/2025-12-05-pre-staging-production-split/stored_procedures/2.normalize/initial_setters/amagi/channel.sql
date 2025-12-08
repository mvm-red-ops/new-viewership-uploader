--channel
CREATE OR REPLACE PROCEDURE upload_db.public.set_channel_dynamic_platform_amagi(platform VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
AS
$$
var platformName = PLATFORM; 
var sql_command = 
     `UPDATE test_staging.public.${platformName}_viewership w
        SET w.channel_id = q.cid, w.channel = q.cname
        FROM (
            SELECT v.id AS id, ad.internal_channel_id AS cid, ad.internal_channel AS cname
            FROM test_staging.public.${PLATFORM}_viewership v
            LEFT JOIN dictionary.pulic.active_deal ad ON ad.platform_channel_name = v.platform_channel_name
            WHERE v.channel IS NULL and processed is null
        ) q
        WHERE w.id = q.id`;

try {
    snowflake.execute(
        {sqlText: sql_command}
    );
    return "Succeeded.";  // Return a success/error indicator.
}
catch (err) {
    return "Failed: " + err;   // Return a success/error indicator.
}
$$;
