CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_PLATFORM_WURL()
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    var sql_command = 
     `  UPDATE test_staging.public.wurl_viewership
        SET platform = 'Wurl'
        WHERE platform is null and processed is null `;
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