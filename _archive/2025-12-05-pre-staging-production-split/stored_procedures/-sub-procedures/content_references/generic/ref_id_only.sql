CREATE OR REPLACE PROCEDURE upload_db.public.process_viewership_ref_id_only_generic(
    platform VARCHAR,
    filename VARCHAR
)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
    // Convert input arguments to local variables (since Snowflake arguments are uppercase)
    var platformArg = PLATFORM;
    const bucketName = "REF_ID_ONLY"; // Hardcoded as it matches the procedure name
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
            '${message.replace(/'/g, "''")}',
            'process_viewership_ref_id_only_generic',
            '${platformArg.replace(/'/g, "''")}',
            '${status.replace(/'/g, "''")}',
            '${rowsAffected.replace(/'/g, "''")}',
            '${errorMessage.replace(/'/g, "''")}',
            '${executionTime}'
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
    UPDATE test_staging.public.platform_viewership w
    SET w.series_code = q.series_code,
        w.content_provider = q.content_provider,
        w.asset_title = q.title,
        w.asset_series = q.asset_series
    FROM (
        -- First attempt: Match with title check
        SELECT
            v.id AS id,
            m.title AS title,
            s.content_provider AS content_provider,
            s.series_code AS series_code,
            upload_db.public.extract_primary_title(s.titles) AS asset_series
        FROM
            test_staging.public.platform_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN metadata_master_cleaned_staging.public.episode e ON (v.ref_id = e.ref_id)
        JOIN metadata_master_cleaned_staging.public.series s ON (s.id = e.series_id)
        JOIN metadata_master_cleaned_staging.public.metadata m ON (e.ref_id = m.ref_id)
        JOIN metadata_master_cleaned_staging.public.metadata m_title_check ON (
            e.ref_id = m_title_check.ref_id
            AND (
                LOWER(REGEXP_REPLACE(TRIM(m_title_check.title), '[^A-Za-z0-9]', '')) =
                LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                OR
                LOWER(REGEXP_REPLACE(TRIM(m_title_check.clean_title), '[^A-Za-z0-9]', '')) =
                LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
            )
        )
        WHERE v.platform = '${platformArg}'
            AND v.processed IS NULL
            AND v.content_provider IS NULL
            AND v.ref_id IS NOT NULL
            AND lower(s.status) = 'active'
            ${filenameArg ? `AND v.filename = '${filenameArg.replace(/'/g, "''")}'` : ''}

        UNION

        -- Fallback: Match on ref_id only, no title check
        SELECT
            v.id AS id,
            m.title AS title,
            s.content_provider AS content_provider,
            s.series_code AS series_code,
            upload_db.public.extract_primary_title(s.titles) AS asset_series
        FROM
            test_staging.public.platform_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN metadata_master_cleaned_staging.public.episode e ON (v.ref_id = e.ref_id)
        JOIN metadata_master_cleaned_staging.public.series s ON (s.id = e.series_id)
        JOIN metadata_master_cleaned_staging.public.metadata m ON (e.ref_id = m.ref_id)
        WHERE v.platform = '${platformArg}'
            AND v.processed IS NULL
            AND v.content_provider IS NULL
            AND v.ref_id IS NOT NULL
            AND lower(s.status) = 'active'
            ${filenameArg ? `AND v.filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
            -- Exclude records already matched in first attempt
            AND NOT EXISTS (
                SELECT 1
                FROM metadata_master_cleaned_staging.public.metadata m_check
                WHERE e.ref_id = m_check.ref_id
                AND (
                    LOWER(REGEXP_REPLACE(TRIM(m_check.title), '[^A-Za-z0-9]', '')) =
                    LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                    OR
                    LOWER(REGEXP_REPLACE(TRIM(m_check.clean_title), '[^A-Za-z0-9]', '')) =
                    LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
                )
            )
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
            const conflictTypeForBucket = "Viewership title does match ref_id";
            const callConflictHandlerSql = `
            CALL upload_db.public.handle_viewership_conflicts(
                '${platformArg}',
                ${filenameArg ? `'${filenameArg.replace(/'/g, "''")}'` : 'NULL'},
                '${conflictTypeForBucket}',
                '${bucketName}'
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
            // Continue with the procedure, don't throw the error
        }

        // IMPORTANT: Log the exact return string format that will be used
        const returnStr = `Update completed successfully. Strategy: ${bucketName}, updated ${rows_affected} rows`;
        logStep(`Returning: ${returnStr}`, "INFO");

        // Log final execution stats
        const totalExecutionTime = (new Date() - startTime) / 1000;
        logStep(`Procedure completed in ${totalExecutionTime} seconds`, "COMPLETED", rows_affected.toString());

        // IMPORTANT: Make sure the return string exactly matches the pattern expected by the main procedure
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

        // Get the argument values, being careful in case they're not defined
        const platformValue = typeof PLATFORM !== 'undefined' ? PLATFORM.replace(/'/g, "''") : 'unknown';

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
            'Procedure execution failed',
            'process_viewership_ref_id_only_generic',
            '${platformValue}',
            'FAILED',
            '0',
            '${err.toString().replace(/'/g, "''")}',
            '${executionTime}'
        )`;

        snowflake.execute({sqlText: logSql});
    } catch (logErr) {
        // If even error logging fails, there's not much we can do
    }

    return "Failed: " + err;
}
$$;

GRANT USAGE ON PROCEDURE upload_db.public.process_viewership_ref_id_only_generic(VARCHAR, VARCHAR) TO ROLE web_app;
