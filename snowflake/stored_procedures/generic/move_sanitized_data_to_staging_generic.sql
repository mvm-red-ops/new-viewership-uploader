-- Generic Move Sanitized Data to Staging Procedure
-- Replaces platform-specific table copying logic
-- Copies data from upload_db.public.platform_viewership to test_staging.public.platform_viewership

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_SANITIZED_DATA_TO_STAGING_GENERIC(
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

    // Step 1: Get all column names from the upload table
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

    // Step 2: Construct and execute the INSERT INTO SELECT query
    sql_command = `
        INSERT INTO test_staging.public.platform_viewership (${columns.join(", ")})
        SELECT ${columns.join(", ")}
        FROM upload_db.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL;
    `;

    var insertStmt = snowflake.createStatement({sqlText: sql_command});
    var insertResult = insertStmt.execute();

    // Get the number of rows inserted
    var rowCountQuery = `SELECT COUNT(*) as cnt FROM test_staging.public.platform_viewership
                         WHERE UPPER(platform) = '${upperPlatform}'
                           AND LOWER(filename) = '${lowerFilename}'
                           AND processed IS NULL`;
    var rowCountStmt = snowflake.createStatement({sqlText: rowCountQuery});
    var rowCountResult = rowCountStmt.execute();
    rowCountResult.next();
    var rowCount = rowCountResult.getColumnValue('CNT');

    return `Data copied successfully. ${rowCount} rows moved from upload_db to test_staging for ${PLATFORM} - ${FILENAME}`;

} catch (err) {
    const errorMessage = "Error in move_sanitized_data_to_staging_generic: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform, query_text) VALUES (?, ?, ?, ?)",
        binds: [errorMessage, 'move_sanitized_data_to_staging_generic', PLATFORM, sql_command]
    });

    return errorMessage;
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_SANITIZED_DATA_TO_STAGING_GENERIC(STRING, STRING) TO ROLE web_app;
