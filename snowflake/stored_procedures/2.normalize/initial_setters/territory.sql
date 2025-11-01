CREATE OR REPLACE PROCEDURE upload_db.public.set_territory()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     ` MAKE DYNAMIC`;
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