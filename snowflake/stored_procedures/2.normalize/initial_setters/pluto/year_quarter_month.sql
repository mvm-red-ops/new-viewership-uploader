--upload_db.public.set_dates_pluto()
--create stored procedure to set the following columns: 
--year
--quarter
--year_month_day (this is MMDDYYYY, where DD is always 01. When month is April and year = 2024, year_month_day will be 04012024)

-- Year, Month, quarter based on ymd (CREATED)
CREATE OR REPLACE PROCEDURE upload_db.public.set_year_quarter_month_pluto()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
AS
$$
var sql_command = 
    `UPDATE test_staging.public.pluto_viewership p
SET 
    year_column = TO_NUMBER(SUBSTR(year_month_day, 1, 4)),
    month_column = TO_NUMBER(SUBSTR(year_month_day, 5, 2)),
    quarter_column = 
        CASE 
            WHEN TO_NUMBER(SUBSTR(year_month_day, 5, 2)) BETWEEN 1 AND 3 THEN 'q1'
            WHEN TO_NUMBER(SUBSTR(year_month_day, 5, 2)) BETWEEN 4 AND 6 THEN 'q2'
            WHEN TO_NUMBER(SUBSTR(year_month_day, 5, 2)) BETWEEN 7 AND 9 THEN 'q3'
            ELSE 'q4'
        END
WHERE year_month_day IS NOT NULL and processed is null`;
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

