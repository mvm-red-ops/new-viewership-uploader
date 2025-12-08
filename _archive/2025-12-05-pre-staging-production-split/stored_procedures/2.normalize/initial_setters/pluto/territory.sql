-- SET TERRITORY
CREATE OR REPLACE PROCEDURE upload_db.public.set_territory_pluto()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `UPDATE test_staging.public.pluto_viewership p
        SET w.territory_id = q.tid 
        FROM (
            SELECT 
                w.id AS id, 
                w.territory,
            FROM test_staging.public.pluto_viewership p 
            JOIN dictionary.public.territories t 
            ON (t.name = p.territory)
            WHERE w.territory_id IS NULL 
              AND w.processed IS NULL     
        ) q
        WHERE w.id = q.id;`;
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