-- Generic version of set_channel_deal_parent_pluto
-- Works with generic platform_viewership table
-- Filters by platform and filename

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_CHANNEL_DEAL_PARENT_PLUTO(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    const lowerFilename = FILENAME.toLowerCase();

    var sql_command = `
        UPDATE test_staging.public.platform_viewership p
        SET channel = 'Nosey',
            channel_id = 8,
            deal_parent = 29
        WHERE UPPER(p.platform) = 'PLUTO'
          AND LOWER(p.filename) = '${lowerFilename}'
          AND p.channel IS NULL
          AND p.processed IS NULL
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err.message;
    }
$$;

GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_CHANNEL_DEAL_PARENT_PLUTO(STRING) TO ROLE web_app;
