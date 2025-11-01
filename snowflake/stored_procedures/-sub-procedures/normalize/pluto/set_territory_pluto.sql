CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_TERRITORY_PLUTO()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    var sql_command = 
     `UPDATE test_staging.public.pluto_viewership p
        SET p.territory_id = q.tid 
        FROM (
            SELECT 
                pv.id AS id, 
                pv.territory,
                t.id as tid
            FROM test_staging.public.pluto_viewership pv
            JOIN dictionary.public.territories t 
            ON (t.name = pv.territory)
            WHERE pv.territory_id IS NULL 
              AND pv.processed IS NULL     
        ) q
        WHERE p.id = q.id;`;
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