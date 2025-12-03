CREATE OR REPLACE PROCEDURE "PROCESS_VIEWERSHIP_FULL_DATA_GENERIC"("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Convert input arguments to local variables (since Snowflake arguments are uppercase)
    var platformArg = PLATFORM;
    const bucketName = "FULL_DATA"; // Hardcoded as it matches the procedure name
    const filenameArg = FILENAME;
    // Start time for procedure execution
    const startTime = new Date();
    // Function to log execution steps
    function logStep(message, status, rowsAffected = "0", errorMessage = "") {
        const executionTime = (new Date() - startTime) / 1000; // Time in seconds
        const logSql = `
        INSERT INTO UPLOAD_DB_PROD.PUBLIC.ERROR_LOG_TABLE (
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
            ''process_viewership_full_data_generic'',
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
    UPDATE NOSEY_PROD.public.platform_viewership w
    SET
        w.series_code = q.series_code,
        w.content_provider = q.content_provider,
        w.asset_title = q.title,
        w.asset_series = q.asset_series
    FROM (
        SELECT
            v.id AS id,
            m.title AS title,
            s.content_provider AS content_provider,
            s.series_code AS series_code,
            UPLOAD_DB_PROD.public.extract_primary_title(s.titles) as asset_series
        FROM
            NOSEY_PROD.public.platform_viewership v
        JOIN UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN METADATA_MASTER.public.episode e ON (
            v.ref_id = e.ref_id
            AND CAST(e.episode AS VARCHAR) = v.episode_number
            AND CAST(e.season AS VARCHAR) = v.season_number
        )
        JOIN METADATA_MASTER.public.series s ON (s.id = e.series_id)
        JOIN METADATA_MASTER.public.metadata m ON (
            LOWER(REGEXP_REPLACE(TRIM(m.title), ''[^A-Za-z0-9]'', '''')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), ''[^A-Za-z0-9]'', ''''))
            OR
            LOWER(REGEXP_REPLACE(TRIM(m.clean_title),  ''[^A-Za-z0-9]'', '''')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name),  ''[^A-Za-z0-9]'', ''''))
        )
        WHERE v.platform = ''${platformArg}''
            AND v.processed IS NULL
            AND v.content_provider IS NULL
            AND v.platform_content_name IS NOT NULL
            AND v.ref_id IS NOT NULL
            AND v.internal_series IS NOT NULL
            AND lower(s.status) = ''active''
            AND (m.title IS NOT NULL AND LENGTH(TRIM(m.title)) > 0)
            ${filenameArg ? `AND v.filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}
    ) q
    WHERE w.id = q.id
    `;
    logStep("Executing update statement", "IN_PROGRESS");
    try {
        // First create a temporary table to mark records before update
        const createMarkerSql = `
        CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${platformArg.toUpperCase()}_UPDATE_MARKER AS
        SELECT
            id,
            content_provider IS NOT NULL AS had_content_provider,
            series_code IS NOT NULL AS had_series_code,
            asset_title IS NOT NULL AS had_asset_title,
            asset_series IS NOT NULL AS had_asset_series
        FROM NOSEY_PROD.public.platform_viewership
        WHERE platform = ''${platformArg}''
          AND id IN (
            SELECT id FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET
        )
        `;
        snowflake.execute({sqlText: createMarkerSql});
        // Execute the update statement with proper row tracking
        const updateStmt = snowflake.createStatement({ sqlText: sql_command });
        updateStmt.execute();
        const rows_affected = updateStmt.getNumRowsAffected();
        if (rows_affected > 0) {
            logStep(`FULL_DATA: Successfully updated ${rows_affected} records with content_provider, series_code, asset_title, asset_series`, "SUCCESS", rows_affected.toString());
        } else {
            logStep(`FULL_DATA: No records matched strict criteria (exact ref_id + episode/season + title match)`, "INFO", "0");
        }
        // Let''s see how many records potentially match our criteria for unmatched
        const countPotentialUnmatched = `
        SELECT COUNT(*) AS POTENTIAL_UNMATCHED
        FROM NOSEY_PROD.public.platform_viewership v
        JOIN UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON v.id = b.id
        WHERE v.platform = ''${platformArg}''
          AND v.content_provider IS NULL
          AND v.processed IS NULL;
        `;
        const potentialResult = snowflake.execute({sqlText: countPotentialUnmatched});
        let potentialCount = 0;
        if (potentialResult.next()) {
            potentialCount = potentialResult.getColumnValue(''POTENTIAL_UNMATCHED'');
        }
        logStep(`Found ${potentialCount} potential unmatched records in viewership table`, "INFO");
        // Call the conflicts handling procedure
        try {
            const conflictType = "No match in FULL_DATA bucket (requires exact ref_id + episode/season + title match)";
            const callConflictHandlerSql = `
                CALL UPLOAD_DB_PROD.public.handle_viewership_conflicts(
                    ''${platformArg.replace(/''/g, "''''")}'',
                    ''${filenameArg ? filenameArg.replace(/''/g, "''''") : ''NULL''}'',
                    ''${conflictType}'',
                    ''${bucketName}''
                )
            `;
            const conflictResult = snowflake.execute({sqlText: callConflictHandlerSql});
            if (conflictResult.next()) {
                const resultMessage = conflictResult.getColumnValue(1);
                logStep(`Conflict handling: ${resultMessage}`, "INFO");
            }
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
        INSERT INTO UPLOAD_DB_PROD.PUBLIC.ERROR_LOG_TABLE (
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
            ''process_viewership_full_data_generic'',
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