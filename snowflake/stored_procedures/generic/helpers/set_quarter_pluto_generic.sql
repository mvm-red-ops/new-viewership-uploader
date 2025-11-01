-- Generic version of set_quarter_pluto
-- Works with generic platform_viewership table
-- Derives quarter from month column

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_QUARTER_PLUTO(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    const lowerFilename = FILENAME.toLowerCase();

    var sql_command = `
        UPDATE test_staging.public.platform_viewership
        SET quarter =
            CASE
                WHEN TO_NUMBER(month) BETWEEN 1 AND 3 THEN 'Q1'
                WHEN TO_NUMBER(month) BETWEEN 4 AND 6 THEN 'Q2'
                WHEN TO_NUMBER(month) BETWEEN 7 AND 9 THEN 'Q3'
                ELSE 'Q4'
            END
        WHERE UPPER(platform) = 'PLUTO'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL
          AND quarter IS NULL
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err.message;
    }
$$;

GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_QUARTER_PLUTO(STRING) TO ROLE web_app;
