-- Move Streamlit Data to Staging Procedure
-- For Streamlit uploads where data is already transformed/sanitized
-- Simply copies data from upload_db.public.platform_viewership to test_staging.public.platform_viewership

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_STREAMLIT_DATA_TO_STAGING(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
let sql_command = "";
try {
    const upperPlatform = PLATFORM.toUpperCase();
    const lowerFilename = FILENAME.toLowerCase();

    // Step 1: Get all column names from the upload table dynamically
    sql_command = `
        SELECT COLUMN_NAME
        FROM UPLOAD_DB.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'PUBLIC'
          AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
          AND TABLE_CATALOG = 'UPLOAD_DB'
        ORDER BY ORDINAL_POSITION;
    `;

    var stmt = snowflake.createStatement({sqlText: sql_command});
    var colsResult = stmt.execute();
    var columns = [];

    while (colsResult.next()) {
        columns.push(colsResult.getColumnValue(1));
    }

    if (columns.length === 0) {
        throw new Error("No columns found in upload_db.public.platform_viewership table");
    }

    // Remove LOAD_TIMESTAMP if it exists (will be auto-generated in target table)
    columns = columns.filter(col => col.toUpperCase() !== 'LOAD_TIMESTAMP');

    console.log(`Found ${columns.length} columns to copy`);

    // Step 2: Construct and execute the INSERT INTO SELECT query
    // For Streamlit, data has no phase yet and processed is NULL/FALSE
    sql_command = `
        INSERT INTO test_staging.public.platform_viewership (${columns.join(", ")})
        SELECT ${columns.join(", ")}
        FROM upload_db.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND (processed IS NULL OR processed = FALSE)
          AND (phase IS NULL OR phase = '');
    `;

    console.log("Streamlit - Move data SQL:", sql_command);

    var insertStmt = snowflake.createStatement({sqlText: sql_command});
    var insertResult = insertStmt.execute();
    var rowsInserted = insertStmt.getNumRowsAffected();

    console.log(`✓ Data copied to staging: ${rowsInserted} rows`);

    // Step 3: Set phase to 0 (initial load complete)
    sql_command = `
        UPDATE test_staging.public.platform_viewership
        SET phase = '0'
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND (phase IS NULL OR phase = '');
    `;

    console.log("Setting phase to 0:", sql_command);

    var updateStmt = snowflake.createStatement({sqlText: sql_command});
    updateStmt.execute();
    var rowsUpdated = updateStmt.getNumRowsAffected();

    console.log(`✓ Phase set to 0: ${rowsUpdated} rows updated`);

    return `Data copied successfully. ${rowsInserted} rows moved from upload_db to test_staging for ${PLATFORM} - ${FILENAME}. Phase set to 0 for ${rowsUpdated} rows.`;

} catch (err) {
    const errorMessage = "Error in move_streamlit_data_to_staging: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform, query_text) VALUES (?, ?, ?, ?)",
        binds: [errorMessage, 'move_streamlit_data_to_staging', PLATFORM, sql_command]
    });

    throw new Error(errorMessage);
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_STREAMLIT_DATA_TO_STAGING(STRING, STRING) TO ROLE web_app;
