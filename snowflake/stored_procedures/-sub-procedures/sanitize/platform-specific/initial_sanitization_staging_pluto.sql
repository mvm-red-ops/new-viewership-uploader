GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_PLUTO(STRING) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_PLUTO("FILENAME" STRING)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Update all columns at once
    const lowerFilename = FILENAME.toLowerCase();
    var updateQuery = `
    UPDATE upload_db.public.pluto_viewership 
    SET   
        tot_mov = CASE 
            WHEN TRIM(REGEXP_REPLACE(tot_mov, '[^0-9]', '')) = '' THEN 0 
            ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(tot_mov, '[^0-9.]', '')), 25, 5)
        END,
        tot_sessions = CASE 
            WHEN TRIM(REGEXP_REPLACE(tot_sessions, '[^0-9]', '')) = '' THEN 0 
            ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(tot_sessions, '[^0-9.]', '')), 10, 0)
        END,
        episode_number = REGEXP_REPLACE(episode_number, ',', ''),
        season_number = REGEXP_REPLACE(season_number, ',', ''),
        revenue = CASE 
            WHEN TRIM(REGEXP_REPLACE(revenue, '[^0-9]', '')) = '' THEN 0 
            ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(revenue, '[^0-9.]', '')), 38, 8)
        END,
        month = TO_NUMBER(month)
        WHERE lower(filename) = '${lowerFilename}';`
    snowflake.execute({sqlText: updateQuery});

    return "Initial sanitization completed successfully.";
} catch (err) {
    // Error handling
    return "Error during initial sanitization: " + err.message;
}
$$;