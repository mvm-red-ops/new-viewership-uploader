CREATE OR REPLACE PROCEDURE "PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC"("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Convert input arguments to local variables (since Snowflake arguments are uppercase)
    var platformArg = PLATFORM;
    const bucketName = "SERIES_ONLY"; // Hardcoded as it matches the procedure name
    const filenameArg = FILENAME;
    // Start time for procedure execution
    const startTime = new Date();
    // Function to log execution steps
    function logStep(message, status, rowsAffected = "0", errorMessage = "") {
        const executionTime = (new Date() - startTime) / 1000; // Time in seconds
        const logSql = `
        INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (
            LOG_TIME,
            LOG_MESSAGE,
            PROCEDURE_NAME,
            PLATFORM,
            STATUS,
            ROWS_AFFECTED,
            ERROR_MESSAGE,
            EXECUTION_TIME
        ) VALUES (
            CURRENT_TIMESTAMP(),
            ''${message.replace(/''/g, "''''")}'',
            ''process_viewership_series_only_generic'',
            ''${platformArg.replace(/''/g, "''''")}'',
            ''${status.replace(/''/g, "''''")}'',
            ''${rowsAffected.replace(/''/g, "''''")}'',
            ''${errorMessage.replace(/''/g, "''''")}'',
            ''${executionTime}''
        )`;
        try {
            snowflake.execute({sqlText: logSql});
        } catch (logErr) {
            // If logging fails, continue with procedure but note the error
        }
    }
    // Log procedure start
    logStep(`Processing ${bucketName} bucket for platform: ${platformArg}`, "STARTED");
    const sql_command = `
    UPDATE TEST_STAGING.public.platform_viewership w
    SET
        w.ref_id = q.ref_id,
        w.series_code = q.series_code,
        w.content_provider = q.content_provider,
        w.asset_title = q.title,
        w.asset_series = q.asset_series
    FROM (
        SELECT
            v.id AS id,
            m.title AS title,
            m.ref_id as ref_id,
            s.content_provider AS content_provider,
            s.series_code AS series_code,
            UPLOAD_DB.public.extract_primary_title(s.titles) AS asset_series
        FROM
            TEST_STAGING.public.platform_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN METADATA_MASTER_CLEANED_STAGING.public.series s ON (
            LOWER(UPLOAD_DB.public.extract_primary_title(s.titles)) = LOWER(v.internal_series)
        )
        JOIN METADATA_MASTER_CLEANED_STAGING.public.metadata m ON (
            LOWER(REGEXP_REPLACE(TRIM(m.title), ''[^A-Za-z0-9]'', '''')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), ''[^A-Za-z0-9]'', ''''))
            OR
            LOWER(REGEXP_REPLACE(TRIM(m.clean_title),  ''[^A-Za-z0-9]'', '''')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name),  ''[^A-Za-z0-9]'', ''''))
        )
        WHERE v.platform = ''${platformArg}''
            AND v.processed IS NULL
            AND lower(s.status) = ''active''
            AND v.content_provider IS NULL
            AND v.platform_content_name IS NOT NULL
            AND v.internal_series IS NOT NULL
            AND lower(SPLIT_PART(m.ref_id, ''-'', 1)) = lower(s.series_code)
            AND (m.title IS NOT NULL AND LENGTH(m.title) > 0)
            ${filenameArg ? `AND v.filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}
    ) q
    WHERE w.id = q.id
    `;
    logStep("Executing update statement", "IN_PROGRESS");
    try {
        // Execute the update statement with proper row tracking
        const updateStmt = snowflake.createStatement({ sqlText: sql_command });
        updateStmt.execute();
        const rows_affected = updateStmt.getNumRowsAffected();
        logStep(`Update operation completed with ${rows_affected} affected rows`, "SUCCESS", rows_affected.toString());
        // Call the conflicts handling procedure
        try {
            const conflictTypeForBucket = "Viewership title does not match series";
            const callConflictHandlerSql = `
            CALL UPLOAD_DB.public.handle_viewership_conflicts(
                ''${platformArg}'',
                ${filenameArg ? `''${filenameArg.replace(/''/g, "''''")}''` : ''NULL''},
                ''${conflictTypeForBucket}'',
                ''${bucketName}''
            )`;
            logStep("Calling conflict handler procedure", "IN_PROGRESS");
            const conflictResult = snowflake.execute({sqlText: callConflictHandlerSql});
            let conflictResultStr = "";
            if (conflictResult.next()) {
                conflictResultStr = conflictResult.getColumnValue(1);
            }
            logStep(`Conflict handler result: ${conflictResultStr}`, "INFO");
        } catch (conflictErr) {
            logStep(`Warning: Error handling conflicts: ${conflictErr.toString()}`, "WARNING");
            // Continue with the procedure, don''t throw the error
        }
        // IMPORTANT: Log the exact return string format that will be used
        const returnStr = `Update completed successfully. Strategy: ${bucketName}, updated ${rows_affected} rows`;
        logStep(`Returning: ${returnStr}`, "INFO");
        // Log final execution stats
        const totalExecutionTime = (new Date() - startTime) / 1000;
        logStep(`Procedure completed in ${totalExecutionTime} seconds`, "COMPLETED", rows_affected.toString());
        // IMPORTANT: Make sure the return string matches the pattern expected by the main procedure
        return returnStr;
    } catch (updateErr) {
        logStep("Update operation failed", "ERROR", "0", updateErr.toString());
        throw updateErr;
    }
}
catch (err) {
    // Log the overall procedure failure
    try {
        const executionTime = (new Date() - startTime) / 1000;
        // Get the argument values, being careful in case they''re not defined
        const platformValue = typeof PLATFORM !== ''undefined'' ? PLATFORM.replace(/''/g, "''''") : ''unknown'';
        const logSql = `
        INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (
            LOG_TIME,
            LOG_MESSAGE,
            PROCEDURE_NAME,
            PLATFORM,
            STATUS,
            ROWS_AFFECTED,
            ERROR_MESSAGE,
            EXECUTION_TIME
        ) VALUES (
            CURRENT_TIMESTAMP(),
            ''Procedure execution failed'',
            ''process_viewership_series_only_generic'',
            ''${platformValue}'',
            ''FAILED'',
            ''0'',
            ''${err.toString().replace(/''/g, "''''")}'',
            ''${executionTime}''
        )`;
        snowflake.execute({sqlText: logSql});
    } catch (logErr) {
        // If even error logging fails, there''s not much we can do
    }
    return "Failed: " + err;
}
';