CREATE OR REPLACE PROCEDURE upload_db.public.validate_nulls(platform VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
AS
$$
var sql_command = 
    `UPDATE test_staging.public.${PLATFORM}_viewership
        SET 
            territory = COALESCE(territory, 'Unspecified'),
            channel = COALESCE(channel, 'Unspecified'),
            territory_id = COALESCE(territory_id, 0),
            channel_id = COALESCE(channel_id, 0)
        WHERE 
            (territory IS NULL OR 
            channel IS NULL OR 
            territory_id IS NULL OR 
            channel_id IS NULL) AND 
            processed is null;`;
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
