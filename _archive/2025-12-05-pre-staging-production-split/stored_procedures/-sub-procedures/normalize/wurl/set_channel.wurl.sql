CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_CHANNEL_WURL()
RETURNS STRING 
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
var sql_command = 
     `UPDATE test_staging.public.wurl_viewership w
        SET w.channel_id = q.cid, w.channel = q.cname
        FROM (
            SELECT wv.id AS id, c.channel_id AS cid, c.name AS cname
            FROM test_staging.public.wurl_viewership wv
            LEFT JOIN dictionary.public.channel_map c ON c.entry = wv.platform_channel_name
            WHERE wv.channel IS NULL and processed is null
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