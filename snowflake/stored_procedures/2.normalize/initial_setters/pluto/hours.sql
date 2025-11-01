-- IF TERRITORY IS US, then SET TOT HOV
CREATE OR REPLACE PROCEDURE upload_db.public.set_hours_pluto()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `UPDATE test_staging.public.pluto_viewership p
        SET p.tot_hov =  p.tot_mov/60
        WHERE p.tot_hov is null and p.territory = 'United States' and p.processed is null and p.tot_mov is not null`;
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