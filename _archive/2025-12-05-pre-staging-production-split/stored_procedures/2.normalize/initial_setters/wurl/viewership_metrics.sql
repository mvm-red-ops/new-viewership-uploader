
CREATE OR REPLACE PROCEDURE upload_db.public.set_minutes_wurl()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `update test_staging.public.wurl_viewership w
        set w.tot_mov =  w.tot_hov * 60
        where w.tot_mov is null and w.processed is null`;
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


    