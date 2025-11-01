-- Generic version of set_ymd_pluto
-- Works with generic platform_viewership table
-- Creates year_month_day field from year and month columns

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_YMD_PLUTO(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
    const lowerFilename = FILENAME.toLowerCase();

    var sql_command = `
        UPDATE test_staging.public.platform_viewership
        SET year_month_day = LPAD(year::string, 4, '0') || LPAD(month::string, 2, '0') || '01'
        WHERE UPPER(platform) = 'PLUTO'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL
          AND year_month_day IS NULL
    `;

    try {
        snowflake.execute({sqlText: sql_command});
        return "Succeeded.";
    } catch (err) {
        return "Failed: " + err.message;
    }
$$;

GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_YMD_PLUTO(STRING) TO ROLE web_app;
