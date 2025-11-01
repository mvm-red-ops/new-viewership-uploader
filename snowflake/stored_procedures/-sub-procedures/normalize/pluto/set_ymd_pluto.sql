CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_YMD_PLUTO()
RETURNS STRING
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS
$$
var sql_command = 
    `UPDATE test_staging.public.pluto_viewership
    SET year_month_day = LPAD(year::string, 4, '0') || LPAD(month::string, 2, '0') || '01'
    WHERE processed is null and year_month_day is null`;
try {
    snowflake.execute(
        {sqlText: sql_command}
    );
    return "Succeeded.";  // Return a success/error indicator.
}
catch (err) {
    return "Failed: " + err;   // Return a success/error indicator.
}
$$;