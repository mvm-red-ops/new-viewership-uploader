CREATE OR REPLACE PROCEDURE TEST_STAGING.PUBLIC.SET_QUARTER_DYNAMIC("PLATFORM" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    const sql_command = 
     `UPDATE test_staging.public.${PLATFORM}_viewership
        SET quarter = 
        CASE 
            WHEN TO_NUMBER(month) BETWEEN 1 AND 3 THEN ''q1''
            WHEN TO_NUMBER(month) BETWEEN 4 AND 6 THEN ''q2''
            WHEN TO_NUMBER(month) BETWEEN 7 AND 9 THEN ''q3''
            ELSE ''q4''
        END
        WHERE processed is null and quarter is null`;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
';


GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_QUARTER_DYNAMIC(varchar) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE upload_db_prod.PUBLIC.SET_QUARTER_DYNAMIC("PLATFORM" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    const sql_command = 
     `UPDATE nosey_prod.public.${PLATFORM}_viewership
        SET quarter = 
        CASE 
            WHEN TO_NUMBER(month) BETWEEN 1 AND 3 THEN ''q1''
            WHEN TO_NUMBER(month) BETWEEN 4 AND 6 THEN ''q2''
            WHEN TO_NUMBER(month) BETWEEN 7 AND 9 THEN ''q3''
            ELSE ''q4''
        END
        WHERE processed is null and quarter is null`;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
';
