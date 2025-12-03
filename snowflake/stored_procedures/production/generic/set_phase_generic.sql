-- Generic Set Phase Procedure
-- Replaces set_phase_dynamic to work with generic platform_viewership table
-- Updates the processing phase for records

CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_PHASE_GENERIC(
    platform VARCHAR,
    phase_number FLOAT,
    filename VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const upperPlatform = PLATFORM.toUpperCase();
    const jsPhaseNumber = PHASE_NUMBER;
    const lowerFilename = FILENAME.toLowerCase();

    // Update phase for all unprocessed records matching platform and filename
    var sql_command = `
        UPDATE NOSEY_PROD.public.platform_viewership
        SET phase = ?
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL
    `;

    var statement = snowflake.createStatement({
        sqlText: sql_command,
        binds: [jsPhaseNumber]
    });

    var result = statement.execute();

    // Get the number of rows updated
    var rowCountQuery = `
        SELECT COUNT(*) as cnt
        FROM NOSEY_PROD.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND phase = ${jsPhaseNumber}
          AND processed IS NULL
    `;

    var rowCountStmt = snowflake.createStatement({sqlText: rowCountQuery});
    var rowCountResult = rowCountStmt.execute();
    rowCountResult.next();
    var rowCount = rowCountResult.getColumnValue('CNT');

    return `Phase set to ${jsPhaseNumber} for ${rowCount} records (${PLATFORM} - ${FILENAME})`;

} catch (err) {
    const errorMessage = "Error in set_phase_generic: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)",
        binds: [errorMessage, 'set_phase_generic', PLATFORM]
    });

    return errorMessage;
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_PHASE_GENERIC(VARCHAR, FLOAT, VARCHAR) TO ROLE web_app;
