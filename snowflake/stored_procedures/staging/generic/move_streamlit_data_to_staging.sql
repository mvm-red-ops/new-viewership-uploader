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

    // Step 1: Get column names from TARGET table that also exist in source
    // Use TARGET table's column order to ensure correct positional alignment
    sql_command = `
        SELECT COLUMN_NAME
        FROM TEST_STAGING.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'PUBLIC'
          AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
          AND COLUMN_NAME IN (
              SELECT COLUMN_NAME
              FROM UPLOAD_DB.INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = 'PUBLIC'
                AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
                AND TABLE_CATALOG = 'UPLOAD_DB'
          )
        ORDER BY ORDINAL_POSITION;
    `;

    var stmt = snowflake.createStatement({sqlText: sql_command});
    var colsResult = stmt.execute();
    var columns = [];

    while (colsResult.next()) {
        columns.push(colsResult.getColumnValue(1));
    }

    if (columns.length === 0) {
        throw new Error("No matching columns found between upload_db and test_staging");
    }

    // Remove LOAD_TIMESTAMP and ID if they exist (will be auto-generated in target table)
    columns = columns.filter(col => col.toUpperCase() !== 'LOAD_TIMESTAMP' && col.toUpperCase() !== 'ID');

    console.log(`Found ${columns.length} columns to copy`);

    // Step 2: Delete any existing records for this filename in staging (prevents duplicates on retry)
    sql_command = `
        DELETE FROM TEST_STAGING.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}';
    `;

    console.log("Deleting existing records:", sql_command);

    var deleteStmt = snowflake.createStatement({sqlText: sql_command});
    deleteStmt.execute();
    var rowsDeleted = deleteStmt.getNumRowsAffected();

    console.log(`✓ Deleted ${rowsDeleted} existing rows from staging`);

    // Step 3: Construct and execute the INSERT INTO SELECT query
    // For Streamlit, data has no phase yet and processed is NULL/FALSE
    // Build column list with LOWER() transform for quarter column
    const selectColumns = columns.map(col => {
        if (col.toUpperCase() === 'QUARTER') {
            return `LOWER(${col})`;
        }
        return col;
    });

    sql_command = `
        INSERT INTO TEST_STAGING.public.platform_viewership (${columns.join(", ")})
        SELECT ${selectColumns.join(", ")}
        FROM UPLOAD_DB.public.platform_viewership
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
        UPDATE TEST_STAGING.public.platform_viewership
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
        sqlText: "INSERT INTO UPLOAD_DB.public.error_log_table (log_message, procedure_name, platform, query_text) VALUES (?, ?, ?, ?)",
        binds: [errorMessage, 'move_streamlit_data_to_staging', PLATFORM, sql_command]
    });

    throw new Error(errorMessage);
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_STREAMLIT_DATA_TO_STAGING(STRING, STRING) TO ROLE web_app;
