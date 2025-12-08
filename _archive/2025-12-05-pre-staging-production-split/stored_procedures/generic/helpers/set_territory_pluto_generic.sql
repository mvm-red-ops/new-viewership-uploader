-- Generic version of set_territory_pluto
-- Works with generic platform_viewership table
-- Filters by platform and filename

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_TERRITORY_PLUTO(filename STRING)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    const lowerFilename = FILENAME.toLowerCase();

    var sql_command = `
        UPDATE test_staging.public.platform_viewership p
        SET p.territory_id = q.tid
        FROM (
            SELECT
                pv.id AS id,
                pv.platform_territory as territory,
                t.id as tid
            FROM test_staging.public.platform_viewership pv
            JOIN dictionary.public.territories t
            ON (UPPER(t.name) = UPPER(pv.platform_territory))
            WHERE UPPER(pv.platform) = 'PLUTO'
              AND LOWER(pv.filename) = '${lowerFilename}'
              AND pv.territory_id IS NULL
              AND pv.processed IS NULL
        ) q
        WHERE p.id = q.id;
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err.message;
    }
$$;

GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_TERRITORY_PLUTO(STRING) TO ROLE web_app;
