CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_SANITIZED_DATA_TO_STAGING(platform STRING, filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
let sql_command = "";
try {
    var upperPlatform = PLATFORM.toUpperCase();
    const lowerFilename = FILENAME.toLowerCase();
    sql_command = `
        SELECT COLUMN_NAME
        FROM UPLOAD_DB.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'PUBLIC' AND 
            TABLE_NAME = '${upperPlatform}_VIEWERSHIP' 
            TABLE_CATALOG = 'UPLOAD_DB' 
        ;
    `;

    var stmt = snowflake.createStatement({sqlText: sql_command});
    var colsResult = stmt.execute();
    var columns = [];
    while (colsResult.next()) {
        columns.push(colsResult.getColumnValue(1));
    }
    
    if (columns.length === 0) {
        throw "No matching columns found between the two tables.";
    }

    // Step 2: Construct and execute the INSERT INTO SELECT query
    sql_command = `
        INSERT INTO test_staging.public.${PLATFORM}_viewership (${columns.join(", ")})
        SELECT ${columns.join(", ")}
        FROM upload_db.public.${upperPlatform}_viewership
        WHERE processed is null and lower(filename) = '${lowerFilename}';
    `;

    var insertStmt = snowflake.createStatement({sqlText: sql_command});
    insertStmt.execute();
    
    return "Data copied successfully.";
}
catch (err) {
    // Error handling
    var errorMessage = "Error: " + err;
    
    // Log the error message to an error log table
    snowflake.execute({sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform, query_text) VALUES (?, ?, ?, ?)", binds: [errorMessage, 'move_sanitized_data_to_staging', PLATFORM, sql_command]});
    
    // Return the error message
    return errorMessage;
}
$$;