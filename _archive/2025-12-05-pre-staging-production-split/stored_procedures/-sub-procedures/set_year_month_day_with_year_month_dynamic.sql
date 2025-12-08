GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_YEAR_MONTH_DAY_WITH_YEAR_MONTH_DYNAMIC(VARCHAR, VARCHAR) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_YEAR_MONTH_DAY_WITH_YEAR_MONTH_DYNAMIC("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Prepare the dynamic SQL query
    const lowerFilename = FILENAME.toLowerCase();  // Ensure filename is in lowercase for comparison
    const updateMutation = `
        UPDATE test_staging.public.${PLATFORM}_viewership
        SET 
            year_month_day = year || LPAD(month::STRING, 2, ''0'') || ''01''
        WHERE processed IS NULL AND lower(filename) = ''${lowerFilename}'';
    `;

    // Execute the query
    snowflake.execute({sqlText: updateMutation});

    return "Initial sanitization completed successfully.";
} catch (err) {
    // Error handling
    return "Error during initial sanitization: " + err.message;
}
';