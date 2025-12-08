-- channel and deal parent
CREATE OR REPLACE PROCEDURE upload_db.public.set_channel_deal_parent_pluto()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
AS
$$
var sql_command = 
    `UPDATE test_staging.public.pluto_viewership p
    SET channel = 'Nosey', channel_id = 8, deal_parent = 29
    WHERE channel is null and processed is null`;

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