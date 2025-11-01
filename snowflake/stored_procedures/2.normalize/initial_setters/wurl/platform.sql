CREATE OR REPLACE PROCEDURE upload_db.public.set_platform_wurl()
    returns string
    language javascript
    strict
    execute as owner
    as
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
