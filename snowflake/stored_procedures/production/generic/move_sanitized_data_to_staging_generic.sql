-- Generic Move Sanitized Data to Staging Procedure
-- Replaces platform-specific table copying logic
-- Copies data from UPLOAD_DB_PROD.public.platform_viewership to NOSEY_PROD.public.platform_viewership

CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC.MOVE_SANITIZED_DATA_TO_STAGING_GENERIC(
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
        throw new Error("No columns found in UPLOAD_DB_PROD.public.platform_viewership table");
    }

    // Remove LOAD_TIMESTAMP if it exists (will be auto-generated in target table)
    columns = columns.filter(col => col.toUpperCase() !== 'LOAD_TIMESTAMP');

    // Step 2: Construct and execute the INSERT INTO SELECT query
    sql_command = `
        INSERT INTO NOSEY_PROD.public.platform_viewership (${columns.join(", ")})
        SELECT ${columns.join(", ")}
        FROM UPLOAD_DB_PROD.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL;
    `;

    var insertStmt = snowflake.createStatement({sqlText: sql_command});
    var insertResult = insertStmt.execute();

    // Get the number of rows inserted
    var rowCountQuery = `SELECT COUNT(*) as cnt FROM NOSEY_PROD.public.platform_viewership
                         WHERE UPPER(platform) = '${upperPlatform}'
                           AND LOWER(filename) = '${lowerFilename}'
                           AND processed IS NULL`;
    var rowCountStmt = snowflake.createStatement({sqlText: rowCountQuery});
    var rowCountResult = rowCountStmt.execute();
    rowCountResult.next();
    var rowCount = rowCountResult.getColumnValue('CNT');

    return `Data copied successfully. ${rowCount} rows moved from upload_db to NOSEY_PROD for ${PLATFORM} - ${FILENAME}`;

} catch (err) {
    const errorMessage = "Error in move_sanitized_data_to_staging_generic: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_message, procedure_name, platform, query_text) VALUES (?, ?, ?, ?)",
        binds: [errorMessage, 'move_sanitized_data_to_staging_generic', PLATFORM, sql_command]
    });

    return errorMessage;
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.MOVE_SANITIZED_DATA_TO_STAGING_GENERIC(STRING, STRING) TO ROLE web_app;
