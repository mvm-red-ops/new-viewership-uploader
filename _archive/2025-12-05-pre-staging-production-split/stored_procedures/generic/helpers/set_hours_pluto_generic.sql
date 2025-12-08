-- Generic version of set_hours_pluto
-- Works with generic platform_viewership table
-- Converts minutes to hours for US territory (Pluto-specific logic)

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_HOURS_PLUTO(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    const lowerFilename = FILENAME.toLowerCase();

    var sql_command = `
        UPDATE test_staging.public.platform_viewership p
        SET p.tot_hov = p.tot_mov / 60
        WHERE UPPER(p.platform) = 'PLUTO'
          AND LOWER(p.filename) = '${lowerFilename}'
          AND p.tot_hov IS NULL
          AND UPPER(p.platform_territory) = 'UNITED STATES'
          AND p.processed IS NULL
          AND p.tot_mov IS NOT NULL
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err.message;
    }
$$;

GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_HOURS_PLUTO(STRING) TO ROLE web_app;
