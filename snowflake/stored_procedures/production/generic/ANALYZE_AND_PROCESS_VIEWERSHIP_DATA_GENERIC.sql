CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC."ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC"("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const platformArg = PLATFORM;
const filenameArg = FILENAME;
// Use generic platform_viewership table
const viewershipTable = `NOSEY_PROD.public.platform_viewership`;
// Build base conditions with platform filter
// NOTE: We don''t require platform_content_name here because SERIES_SEASON_EPISODE bucket
// can match on series+episode+season without needing a title
const baseConditions = `platform = ''${platformArg}''
AND processed IS NULL
AND content_provider IS NULL
${filenameArg ? `AND filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}`;
try {
    // Start time for procedure execution
    const startTime = new Date();
    // Function to log execution steps
    function logStep(message, status, rowsAffected = "0", errorMessage = "") {
        const executionTime = (new Date() - startTime) / 1000;
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
            ''analyze_and_process_viewership_data_generic'',
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
    logStep(`Starting procedure for platform: ${platformArg}, filename: ${filenameArg || ''ALL''}`, "STARTED");
    // Check if there are any records in the viewership table matching our criteria
    const recordCheckSql = `
        SELECT COUNT(*) AS RECORD_COUNT
        FROM ${viewershipTable}
        WHERE ${baseConditions}`;
    try {
        const recordCheckResult = snowflake.execute({sqlText: recordCheckSql});
        let recordCount = 0;
        if (recordCheckResult.next()) {
            recordCount = recordCheckResult.getColumnValue(''RECORD_COUNT'');
        }
        logStep(`Found ${recordCount} records to process in ${viewershipTable}`, "INFO");
        if (recordCount === 0) {
            const msg = `No records to process for platform ${platformArg}${filenameArg ? `, filename ${filenameArg}` : ''''}`;
            logStep(msg, "COMPLETED", "0");
            return msg;
        }
    } catch (err) {
        logStep(`Error checking for records: ${err.toString()}`, "ERROR");
        throw new Error(`Failed to access ${viewershipTable}: ${err.toString()}`);
    }
    // Define bucket categories based on what fields exist in viewership data
    const bucketCategories = [
        ["FULL_DATA", true, true, true, true],
        ["REF_ID_SERIES", true, true, null, null],
        ["REF_ID_ONLY", true, false, null, null],
        ["SERIES_SEASON_EPISODE", false, true, true, true],
        ["SERIES_ONLY", false, true, null, null],
        ["TITLE_ONLY", null, null, null, null]
    ];
    const bucketOrder = ["FULL_DATA", "REF_ID_SERIES", "REF_ID_ONLY", "SERIES_SEASON_EPISODE", "SERIES_ONLY", "TITLE_ONLY"];
    // Drop unmatched records table if it exists from previous runs
    try {
        snowflake.execute({
            sqlText: `DROP TABLE IF EXISTS UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`
        });
        logStep("Dropped previous unmatched records table if it existed", "INFO");
    } catch (err) {
        logStep(`Warning: Error dropping unmatched records table: ${err.toString()}`, "WARNING");
    }
    // Delete old conflict records from previous runs of this same filename
    // This ensures Lambda gets accurate unmatched count for the current batch only
    if (filenameArg) {
        try {
            const deleteResult = snowflake.execute({
                sqlText: `DELETE FROM METADATA_MASTER.public.record_reprocessing_batch_logs WHERE filename = ''${filenameArg.replace(/''/g, "''''")}''`
            });
            const deletedCount = deleteResult.getNumRowsAffected();
            logStep(`Deleted ${deletedCount} old conflict records for filename: ${filenameArg}`, "INFO");
        } catch (err) {
            logStep(`Warning: Error deleting old conflict records: ${err.toString()}`, "WARNING");
        }
    }
    // Track successful buckets and their record counts
    const successfulBuckets = [];
    // Create temporary tables for each bucket
    logStep("Analyzing data distribution across bucket types", "IN_PROGRESS");
    for (const [bucketName, needsRefId, needsInternalSeries, needsEpisodeNum, needsSeasonNumber] of bucketCategories) {
        let conditions = [baseConditions];
        // Add specific conditions based on bucket type
        if (bucketName === "FULL_DATA") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''''");
            conditions.push("REGEXP_LIKE(episode_number, ''^[0-9]+$'')");
            conditions.push("REGEXP_LIKE(season_number, ''^[0-9]+$'')");
        }
        else if (bucketName === "REF_ID_SERIES") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''''");
        }
        else if (bucketName === "REF_ID_ONLY") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''''");
            conditions.push("(internal_series IS NULL OR TRIM(internal_series) = '''')");
        }
        else if (bucketName === "SERIES_SEASON_EPISODE") {
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''''");
            conditions.push("REGEXP_LIKE(episode_number, ''^[0-9]+$'')");
            conditions.push("REGEXP_LIKE(season_number, ''^[0-9]+$'')");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '''')");
        }
        else if (bucketName === "SERIES_ONLY") {
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''''");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '''')");
            conditions.push("((episode_number IS NULL OR TRIM(episode_number) = '''' OR NOT REGEXP_LIKE(episode_number, ''^[0-9]+$'')) OR (season_number IS NULL OR TRIM(season_number) = '''' OR NOT REGEXP_LIKE(season_number, ''^[0-9]+$'')))");
        }
        else if (bucketName === "TITLE_ONLY") {
            conditions.push("platform_content_name IS NOT NULL AND TRIM(platform_content_name) != ''''");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '''') AND (internal_series IS NULL OR TRIM(internal_series) = '''')");
        }
        // Check for any records matching this bucket before creating the temp table
        const checkSql = `
            SELECT COUNT(*) AS MATCH_COUNT
            FROM ${viewershipTable}
            WHERE ${conditions.join(" AND ")}`;
        let matchCount = 0;
        try {
            const checkResult = snowflake.execute({sqlText: checkSql});
            if (checkResult.next()) {
                matchCount = checkResult.getColumnValue(''MATCH_COUNT'');
            }
            logStep(`Found ${matchCount} records matching criteria for bucket ${bucketName}`, "INFO");
            if (matchCount === 0) {
                logStep(`Skipping bucket ${bucketName} - no matching records`, "INFO");
                continue;
            }
            // Add to successful buckets if it contains records
            successfulBuckets.push({
                name: bucketName,
                rowCount: matchCount
            });
        } catch (err) {
            logStep(`Error checking for matches in bucket ${bucketName}: ${err.toString()}`, "ERROR");
            continue;
        }
    }
    // If no successful buckets were created, return early
    if (successfulBuckets.length === 0) {
        logStep("No buckets with records were created, nothing to process", "COMPLETED", "0");
        return "No buckets with records were created, nothing to process";
    }
    // Now process each successful bucket with the appropriate matching strategy
    let totalProcessed = 0;
    const bucketResults = [];
    // First, manually create the unmatched records table with all records
    const createUnmatchedSql = `
    CREATE OR REPLACE TABLE UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED AS
    SELECT DISTINCT id
    FROM NOSEY_PROD.public.platform_viewership
    WHERE ${baseConditions}
    `;
    snowflake.execute({sqlText: createUnmatchedSql});
    // Check the initial count of records in the unmatched table
    const initialCountSql = `
    SELECT COUNT(*) AS INITIAL_COUNT
    FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
    const initialCountResult = snowflake.execute({sqlText: initialCountSql});
    let initialCount = 0;
    if (initialCountResult.next()) {
        initialCount = initialCountResult.getColumnValue(''INITIAL_COUNT'');
    }
    logStep(`Initial count of records to process: ${initialCount}`, "INFO");
    // Process each bucket type in order
    for (const bucketType of bucketOrder) {
        // Check how many unmatched records we have left
        const checkUnmatchedSql = `
        SELECT COUNT(*) AS UNMATCHED_COUNT
        FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
        const unmatchedResult = snowflake.execute({sqlText: checkUnmatchedSql});
        let unmatchedCount = 0;
        if (unmatchedResult.next()) {
            unmatchedCount = unmatchedResult.getColumnValue(''UNMATCHED_COUNT'');
        }
        logStep(`Processing ${bucketType} - Found ${unmatchedCount} unmatched records`, "INFO");
        if (unmatchedCount === 0) {
            logStep(`No unmatched records left to process for ${bucketType}`, "INFO");
            continue; // Skip to the next bucket type
        }
        // Create temporary bucket with unmatched records
        const bucketTableName = `TEMP_${platformArg.toUpperCase()}_${bucketType}_BUCKET`;
        // Create the bucket - with modified filtering to prevent full data records from flowing to less stringent procedures
        let createBucketSql;
        if (bucketType === "FULL_DATA") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''''
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''''
            AND REGEXP_LIKE(v.episode_number, ''^[0-9]+$'')
            AND REGEXP_LIKE(v.season_number, ''^[0-9]+$'')
            `;
        } else if (bucketType === "REF_ID_SERIES") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''''
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''''
            `;
        } else if (bucketType === "REF_ID_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''''
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''''
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '''')
            `;
        } else if (bucketType === "SERIES_SEASON_EPISODE") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''''
            AND REGEXP_LIKE(v.episode_number, ''^[0-9]+$'')
            AND REGEXP_LIKE(v.season_number, ''^[0-9]+$'')
            `;
        } else if (bucketType === "SERIES_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''''
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '''')
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''''
            AND ((v.episode_number IS NULL OR TRIM(v.episode_number) = '''' OR NOT REGEXP_LIKE(v.episode_number, ''^[0-9]+$''))
                 OR (v.season_number IS NULL OR TRIM(v.season_number) = '''' OR NOT REGEXP_LIKE(v.season_number, ''^[0-9]+$'')))
            `;
        }
        else if (bucketType === "TITLE_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB_PROD.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN NOSEY_PROD.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = ''${platformArg}''
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''''
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '''')
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '''')
            `;
        }
        snowflake.execute({sqlText: createBucketSql});
        // Count the actual records in the bucket
        const countActualSql = `SELECT COUNT(*) AS ACTUAL_COUNT FROM UPLOAD_DB_PROD.PUBLIC.${bucketTableName}`;
        const actualCountResult = snowflake.execute({sqlText: countActualSql});
        let actualCount = 0;
        if (actualCountResult.next()) {
            actualCount = actualCountResult.getColumnValue(''ACTUAL_COUNT'');
        }
        logStep(`${bucketType}: Starting to process ${actualCount} records`, "INFO");
        // If actual data count in bucket is zero, we don''t need to run bucket procs
        if (!actualCount) {
            logStep(`${bucketType}: Skipping - no records need processing`, "INFO");
            continue;
        }
        // Process this bucket with filename parameter - call the GENERIC sub-procedure
        const processSql = `
        CALL UPLOAD_DB_PROD.public.process_viewership_${bucketType.toLowerCase()}_generic(
            ''${platformArg}''
            ${filenameArg ? `, ''${filenameArg.replace(/''/g, "''''")}''` : '', NULL''}
        )`;
        try {
            const bucketResult = snowflake.execute({sqlText: processSql});
            let bucketResultStr = "";
            if (bucketResult.next()) {
                bucketResultStr = bucketResult.getColumnValue(1);
            }
            // Extract rows affected from the result
            const bucketRowsMatch = bucketResultStr.match(/updated (\\d+) rows/);
            const bucketRowsAffected = bucketRowsMatch ? bucketRowsMatch[1] : "0";
            const bucketRowsCount = parseInt(bucketRowsAffected, 10);
            logStep(`${bucketType}: Successfully updated ${bucketRowsAffected} records`, "SUCCESS", bucketRowsAffected);
            // IMPORTANT: Always count this bucket''s results, even if zero rows were affected
            bucketResults.push(`${bucketType}: ${bucketRowsAffected} records`);
            totalProcessed += bucketRowsCount;
            // Update the unmatched records table to remove any we just matched
            const updateUnmatchedSql = `
            DELETE FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            WHERE EXISTS (
                SELECT 1
                FROM NOSEY_PROD.public.platform_viewership v
                WHERE v.id = u.id
                AND v.platform = ''${platformArg}''
                AND v.content_provider IS NOT NULL
            )`;
            snowflake.execute({sqlText: updateUnmatchedSql});
            // Count how many records remain in the unmatched table
            const remainingUnmatchedSql = `
            SELECT COUNT(*) AS REMAINING_UNMATCHED
            FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
            const remainingResult = snowflake.execute({sqlText: remainingUnmatchedSql});
            let remainingCount = 0;
            if (remainingResult.next()) {
                remainingCount = remainingResult.getColumnValue(''REMAINING_UNMATCHED'');
            }
            logStep(`After ${bucketType} processing, ${remainingCount} records still need processing by other strategies`, "INFO");
        } catch (err) {
            logStep(`Error processing ${bucketType} bucket: ${err.toString()}`, "ERROR");
        }
        // Clean up this bucket
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB_PROD.PUBLIC.${bucketTableName}`});
    }
    // Clean up all temporary bucket tables
    for (const bucketType of bucketOrder) {
        try {
            snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketType}_BUCKET`});
        } catch (err) {
            logStep(`Warning: Failed to drop temporary table for ${bucketType}`, "WARNING", "0", err.toString());
        }
    }
    // Calculate the total number of unmatched records
    const unmatchedCount = initialCount - totalProcessed;
    logStep(`Total records: ${initialCount}, Processed: ${totalProcessed}, Unmatched: ${unmatchedCount}`, "INFO");
    // Check for any records that remained unmatched
    try {
        const finalUnmatchedSql = `
        SELECT COUNT(*) AS FINAL_UNMATCHED
        FROM UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
        const finalResult = snowflake.execute({sqlText: finalUnmatchedSql});
        let finalUnmatchedCount = 0;
        if (finalResult.next()) {
            finalUnmatchedCount = finalResult.getColumnValue(''FINAL_UNMATCHED'');
        }
        if (finalUnmatchedCount > 0) {
            logStep(`FINAL RESULT: ${finalUnmatchedCount} records could not be processed by any strategy`, "WARNING");
            // Log these final unmatched records to record_reprocessing_batch_logs
            // This ensures Lambda verification accounts for ALL records
            const logFinalUnmatchedSql = `
                INSERT INTO METADATA_MASTER.public.record_reprocessing_batch_logs (
                    title,
                    viewership_id,
                    filename,
                    notes,
                    platform
                )
                SELECT
                    v.platform_content_name,
                    v.id,
                    v.filename,
                    ''Final unmatched: No bucket could process this record'',
                    ''${platformArg}''
                FROM NOSEY_PROD.public.platform_viewership v
                JOIN UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u ON v.id = u.id
                WHERE v.platform = ''${platformArg}''
                  ${filenameArg ? `AND v.filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}
                  AND v.content_provider IS NULL
                  AND v.processed IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM METADATA_MASTER.public.record_reprocessing_batch_logs l
                      WHERE l.viewership_id = v.id
                  )
            `;
            const logResult = snowflake.execute({sqlText: logFinalUnmatchedSql});
            const rowsLogged = logResult.getNumRowsAffected();
            logStep(`Logged ${rowsLogged} final unmatched records to record_reprocessing_batch_logs`, "INFO");
        }
        // Clean up the unmatched records table
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB_PROD.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`});
        logStep("Dropped unmatched records table", "INFO");
    } catch (err) {
        logStep(`Warning: Failed to finalize unmatched records: ${err.toString()}`, "WARNING", "0", err.toString());
    }
    const totalExecutionTime = (new Date() - startTime) / 1000;
    const completionMessage = filenameArg
        ? `Processing completed for platform ${platformArg}, filename ${filenameArg} in ${totalExecutionTime} seconds. Total records processed: ${totalProcessed}`
        : `Processing completed for platform ${platformArg} in ${totalExecutionTime} seconds. Total records processed: ${totalProcessed}`;
    logStep(completionMessage, "COMPLETED", totalProcessed.toString());
    // IMPORTANT: Always return a success message if totalProcessed > 0
    if (totalProcessed > 0) {
        return `FINAL SUMMARY: Successfully updated ${totalProcessed} total records. Breakdown: ${bucketResults.join(", ")}`;
    } else {
        return `FINAL SUMMARY: No records were updated by any strategy.`;
    }
}
catch (err) {
    return `Error in analyze_and_process_viewership_data_generic: ${err.message}`;
}
';

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(VARCHAR, VARCHAR) TO ROLE web_app;
