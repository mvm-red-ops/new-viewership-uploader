CREATE OR REPLACE PROCEDURE upload_db.public.process_viewership_series_only(
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
            '${message.replace(/'/g, "''")}',
            'process_viewership_series_only',
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
    UPDATE test_staging.public.${platformArg}_viewership w 
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
            upload_db.public.extract_primary_title(s.titles) AS asset_series
        FROM 
            test_staging.public.${platformArg}_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN metadata_master_cleaned_staging.public.series s ON (
            LOWER(upload_db.public.extract_primary_title(s.titles)) = LOWER(v.internal_series)
        )
        JOIN metadata_master_cleaned_staging.public.metadata m ON (
            LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9]', '')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9]', ''))
            OR
            LOWER(REGEXP_REPLACE(TRIM(m.clean_title),  '[^A-Za-z0-9]', '')) =
            LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name),  '[^A-Za-z0-9]', ''))
        )
        WHERE 
            v.processed IS NULL 
            AND lower(s.status) = 'active'
            AND v.content_provider IS NULL 
            AND v.platform_content_name IS NOT NULL
            AND v.internal_series IS NOT NULL
            AND lower(SPLIT_PART(m.ref_id, '-', 1)) = lower(s.series_code)
            AND (m.title IS NOT NULL AND LENGTH(m.title) > 0)
            ${filenameArg ? `AND v.filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
    ) q 
    WHERE w.id = q.id
    `;
    
    logStep("Executing update statement", "IN_PROGRESS");
    
    try {
        // REMOVED: Code that managed the unmatched table
        
        // Debug - Check for specific title to trace why the match isn't working
        if (platformArg.toLowerCase() === 'wurl') {
            const debugSql = `
            SELECT
                v.id,
                v.platform_content_name,
                TRIM(REGEXP_REPLACE(v.platform_content_name, '[^A-Za-z0-9 ]', '')) AS normalized_platform_name,
                v.internal_series,
                m.title AS metadata_title,
                TRIM(REGEXP_REPLACE(m.title, '[^A-Za-z0-9 ]', '')) AS normalized_metadata_title
            FROM
                test_staging.public.wurl_viewership v
            LEFT JOIN
                metadata_master_cleaned_staging.public.metadata m ON
                m.title LIKE '%Romeos%'
            JOIN
                UPLOAD_DB.PUBLIC.TEMP_WURL_${bucketName}_BUCKET b ON v.id = b.id
            WHERE
                v.platform_content_name LIKE '%Romeos%'
                OR v.platform_content_name LIKE '%romeos%'
                AND v.processed IS NULL
                AND v.content_provider IS NULL
                ${filenameArg ? `AND v.filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
            LIMIT 10`;

            const debugResult = snowflake.execute({sqlText: debugSql});

            let debugRows = [];
            while (debugResult.next()) {
                debugRows.push({
                    id: debugResult.getColumnValue('ID'),
                    platform_content_name: debugResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                    normalized_platform_name: debugResult.getColumnValue('NORMALIZED_PLATFORM_NAME'),
                    internal_series: debugResult.getColumnValue('INTERNAL_SERIES'),
                    metadata_title: debugResult.getColumnValue('METADATA_TITLE'),
                    normalized_metadata_title: debugResult.getColumnValue('NORMALIZED_METADATA_TITLE')
                });
            }

            if (debugRows.length > 0) {
                logStep(`Debug - Romeos records found: ${JSON.stringify(debugRows)}`, "INFO");
            } else {
                logStep(`Debug - No Romeos records found in the current bucket`, "INFO");
            }
        }
        
        // Debug logging only for this record - no special case handling
        if (platformArg.toLowerCase() === 'wurl' && filenameArg && filenameArg.includes('February_NEW')) {
            const debugCountSql = `
            SELECT COUNT(*) AS MATCH_COUNT
            FROM
                test_staging.public.wurl_viewership v
            JOIN metadata_master_cleaned_staging.public.metadata m ON (
                LOWER(REGEXP_REPLACE(TRIM(m.title), '[^A-Za-z0-9 ]+', ' ')) =
                LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name), '[^A-Za-z0-9 ]+', ' '))
            )
            WHERE
                v.processed IS NULL
                AND v.content_provider IS NULL
                AND v.internal_series = 'Séptimo Día'
                AND m.title LIKE '%Romeos%'
                AND v.platform_content_name LIKE '%Romeos%'
                AND v.filename = '${filenameArg.replace(/'/g, "''")}'
            `;

            try {
                const countResult = snowflake.execute({sqlText: debugCountSql});
                let matchCount = 0;
                if (countResult.next()) {
                    matchCount = countResult.getColumnValue('MATCH_COUNT');
                }

                logStep(`Debug only - Found ${matchCount} potential Romeos matches using regular title comparison`, "INFO");
            } catch (err) {
                logStep(`Debug count for Romeos failed: ${err.toString()}`, "INFO");
            }
        }

        // Execute the update statement with proper row tracking
        const updateStmt = snowflake.createStatement({ sqlText: sql_command });
        updateStmt.execute();
        const rows_affected = updateStmt.getNumRowsAffected();
        
        logStep(`Update operation completed with ${rows_affected} affected rows`, "SUCCESS", rows_affected.toString());
        
        // Call the conflicts handling procedure
        try {
            const conflictTypeForBucket = "Viewership title does match series";
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
            'process_viewership_series_only',
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