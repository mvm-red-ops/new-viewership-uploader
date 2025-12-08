CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC("PLATFORM" VARCHAR, "FILENAME" VARCHAR, "TITLES_ARRAY" ARRAY DEFAULT null)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Convert input arguments to local variables (since Snowflake arguments are uppercase)
    var platformArg = PLATFORM;
    var filenameArg = FILENAME;
    var titlesArrayArg = TITLES_ARRAY;

    // Define table name - using generic platform_viewership table
    const viewershipTable = `{{STAGING_DB}}.public.platform_viewership`;
    
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
            'analyze_and_process_viewership_data_generic',
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
            snowflake.execute({sqlText: `
                INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (
                    LOG_TIME, 
                    LOG_MESSAGE, 
                    PROCEDURE_NAME, 
                    PLATFORM, 
                    STATUS
                ) VALUES (
                    CURRENT_TIMESTAMP(),
                    'Failed to log step. Original message: ${message.replace(/'/g, "''")}',
                    'analyze_and_process_viewership_data',
                    '${platformArg.replace(/'/g, "''")}',
                    'LOGGING_ERROR'
                )`
            });
        }
    }
    
    // Build titles condition if array is provided
    var titlesCondition = "";
    if (titlesArrayArg && titlesArrayArg.length > 0) {
        var quotedTitles = [];
        for (var i = 0; i < titlesArrayArg.length; i++) {
            var escapedTitle = titlesArrayArg[i].replace(/'/g, "''");
            quotedTitles.push(`'${escapedTitle}'`);
        }
        titlesCondition = ` AND platform_content_name IN (${quotedTitles.join(",")})`;
    }
    
    // Log procedure start
    logStep(`Starting procedure for platform: ${platformArg}, filename: ${filenameArg || 'ALL'}, titles: ${titlesArrayArg ? titlesArrayArg.length + ' specific titles' : 'ALL'}`, "STARTED");

    // EARLY DEBUG: Check what fields exist in viewership table for these titles
    if (titlesArrayArg && titlesArrayArg.length > 0) {
        const earlyDebugSql = `
        SELECT TOP 2 v.id, v.platform_content_name, v.ref_id, v.internal_series,
               v.episode_number, v.season_number, v.asset_series, v.asset_title,
               v.content_provider, v.processed
        FROM ${viewershipTable} v
        WHERE v.platform_content_name IN (${quotedTitles.join(',')})
        ORDER BY v.id
        `;

        logStep(`EARLY DEBUG: Checking available fields in viewership table`, "DEBUG");
        try {
            const earlyResult = snowflake.execute({sqlText: earlyDebugSql});
            let earlyData = [];
            while (earlyResult.next()) {
                earlyData.push({
                    id: earlyResult.getColumnValue('ID'),
                    platform_content_name: earlyResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                    ref_id: earlyResult.getColumnValue('REF_ID'),
                    internal_series: earlyResult.getColumnValue('INTERNAL_SERIES'),
                    episode_number: earlyResult.getColumnValue('EPISODE_NUMBER'),
                    season_number: earlyResult.getColumnValue('SEASON_NUMBER'),
                    asset_series: earlyResult.getColumnValue('ASSET_SERIES'),
                    asset_title: earlyResult.getColumnValue('ASSET_TITLE'),
                    content_provider: earlyResult.getColumnValue('CONTENT_PROVIDER'),
                    processed: earlyResult.getColumnValue('PROCESSED')
                });
            }
            logStep(`EARLY DEBUG SAMPLE: ${JSON.stringify(earlyData)}`, "DEBUG");
        } catch (earlyErr) {
            logStep(`EARLY DEBUG FAILED: ${earlyErr.toString()}`, "ERROR");
        }
    }
    
    // Check if there are any records in the viewership table matching our criteria
    const recordCheckSql = `
        SELECT COUNT(*) AS RECORD_COUNT
        FROM ${viewershipTable}
        WHERE platform = '${platformArg}'
        AND processed IS NULL
        AND content_provider IS NULL
        AND platform_content_name IS NOT NULL
        ${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
        ${titlesCondition}`;
    
    try {
        const recordCheckResult = snowflake.execute({sqlText: recordCheckSql});
        let recordCount = 0;
        if (recordCheckResult.next()) {
            recordCount = recordCheckResult.getColumnValue('RECORD_COUNT');
        }
        
        logStep(`Found ${recordCount} records to process in ${viewershipTable}`, "INFO");
        
        if (recordCount === 0) {
            const msg = `No records to process for platform ${platformArg}${filenameArg ? `, filename ${filenameArg}` : ''}${titlesArrayArg ? `, titles ${titlesArrayArg.join(', ')}` : ''}`;
            logStep(msg, "COMPLETED", "0");
            return msg;
        }
    } catch (err) {
        logStep(`Error checking for records: ${err.toString()}`, "ERROR");
        throw new Error(`Failed to access ${viewershipTable}: ${err.toString()}`);
    }
    
    // Base conditions for all queries - including platform, filename filter and titles filter if provided
    const baseConditions = `platform = '${platformArg}'
    AND processed IS NULL
    AND content_provider IS NULL
    AND platform_content_name IS NOT NULL
    ${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
    ${titlesCondition}`;
    

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
            sqlText: `DROP TABLE IF EXISTS UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`
        });
        logStep("Dropped previous unmatched records table if it existed", "INFO");
    } catch (err) {
        logStep(`Warning: Error dropping unmatched records table: ${err.toString()}`, "WARNING");
    }
    
    // Track successful buckets and their record counts
    const successfulBuckets = [];
    
    // Create temporary tables for each bucket
    logStep("Analyzing data distribution across bucket types", "IN_PROGRESS");
    for (const [bucketName, needsRefId, needsInternalSeries, needsEpisodeNum, needsSeasonNumber] of bucketCategories) {
        let conditions = [baseConditions];
        
        // Add specific conditions based on bucket type
        if (bucketName === "FULL_DATA") {
            // FULL_DATA: requires all fields with proper format
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''");
            conditions.push("REGEXP_LIKE(episode_number, '^[0-9]+$')"); //
            conditions.push("REGEXP_LIKE(season_number, '^[0-9]+$')");  
        } 
        else if (bucketName === "REF_ID_SERIES") {
            // REF_ID_SERIES: requires ref_id and internal_series
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            // Don't care about episode_number or season_number
        }
        else if (bucketName === "REF_ID_ONLY") {
            // REF_ID_ONLY: requires ref_id, no internal_series
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("(internal_series IS NULL OR TRIM(internal_series) = '')");
            // Don't care about episode_number or season_number
        }
        else if (bucketName === "SERIES_SEASON_EPISODE") {
            // SERIES_SEASON_EPISODE: requires internal_series, episode_number, season_number, but no ref_id
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''");
            conditions.push("REGEXP_LIKE(episode_number, '^[0-9]+$')");
            conditions.push("REGEXP_LIKE(season_number, '^[0-9]+$')");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '')");
        }
        else if (bucketName === "SERIES_ONLY") {
            // SERIES_ONLY: requires internal_series, no ref_id, and missing/invalid episode OR season numbers
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '')");
            // Include records that are missing either episode OR season number (they can't go to SERIES_SEASON_EPISODE)
            conditions.push("((episode_number IS NULL OR TRIM(episode_number) = '' OR NOT REGEXP_LIKE(episode_number, '^[0-9]+$')) OR (season_number IS NULL OR TRIM(season_number) = '' OR NOT REGEXP_LIKE(season_number, '^[0-9]+$')))");
        }

        else if (bucketName === "TITLE_ONLY") {
                    // TITLE_ONLY: only requires a platform_content_name, no specific requirements for other fields
                    conditions.push("platform_content_name IS NOT NULL AND TRIM(platform_content_name) != ''");
                    // Exclude records that would match more specific bucket types
                    conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '') AND (internal_series IS NULL OR TRIM(internal_series) = '')");
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
                matchCount = checkResult.getColumnValue('MATCH_COUNT');
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
            // Log the exact query that failed
            logStep(`Error checking for matches in bucket ${bucketName}: ${err.toString()}\nSQL: ${checkSql}`, "ERROR");
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
    CREATE OR REPLACE TABLE UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED AS
    SELECT DISTINCT id
    FROM {{STAGING_DB}}.public.platform_viewership
    WHERE platform = '${platformArg}'
    AND processed IS NULL
    AND content_provider IS NULL
    AND platform_content_name IS NOT NULL
    ${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
    ${titlesCondition}
    `;

    snowflake.execute({sqlText: createUnmatchedSql});

    // Check the initial count of records in the unmatched table
    const initialCountSql = `
    SELECT COUNT(*) AS INITIAL_COUNT
    FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;

    const initialCountResult = snowflake.execute({sqlText: initialCountSql});
    let initialCount = 0;
    if (initialCountResult.next()) {
        initialCount = initialCountResult.getColumnValue('INITIAL_COUNT');
    }

    logStep(`Initial count of records to process: ${initialCount}`, "INFO");

    // Process each bucket type in order
    for (const bucketType of bucketOrder) {
        // Check how many unmatched records we have left
        const checkUnmatchedSql = `
        SELECT COUNT(*) AS UNMATCHED_COUNT
        FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
        
        const unmatchedResult = snowflake.execute({sqlText: checkUnmatchedSql});
        let unmatchedCount = 0;
        if (unmatchedResult.next()) {
            unmatchedCount = unmatchedResult.getColumnValue('UNMATCHED_COUNT');
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
            // DEBUG: First show sample of unmatched records to see what data we're working with
            const sampleDataSql = `
            SELECT TOP 3 v.ref_id, v.internal_series, v.episode_number, v.season_number,
                   v.platform_content_name, LENGTH(TRIM(v.ref_id)) as ref_id_len,
                   LENGTH(TRIM(v.internal_series)) as series_len,
                   REGEXP_LIKE(v.episode_number, '^[0-9]+\$') as ep_regex,
                   REGEXP_LIKE(v.season_number, '^[0-9]+\$') as season_regex
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            ORDER BY u.id
            `;

            logStep(`DEBUG: Sample unmatched records for FULL_DATA analysis`, "DEBUG");
            const sampleResult = snowflake.execute({sqlText: sampleDataSql});
            let sampleData = [];
            while (sampleResult.next()) {
                sampleData.push({
                    ref_id: sampleResult.getColumnValue('REF_ID'),
                    internal_series: sampleResult.getColumnValue('INTERNAL_SERIES'),
                    episode_number: sampleResult.getColumnValue('EPISODE_NUMBER'),
                    season_number: sampleResult.getColumnValue('SEASON_NUMBER'),
                    platform_content_name: sampleResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                    ref_id_len: sampleResult.getColumnValue('REF_ID_LEN'),
                    series_len: sampleResult.getColumnValue('SERIES_LEN'),
                    ep_regex: sampleResult.getColumnValue('EP_REGEX'),
                    season_regex: sampleResult.getColumnValue('SEASON_REGEX')
                });
            }
            logStep(`FULL_DATA SAMPLE: ${JSON.stringify(sampleData)}`, "DEBUG");

            // DEBUG: Check how many records match FULL_DATA criteria before bucket creation
            const debugFullDataSql = `
            SELECT COUNT(*) as FULL_DATA_COUNT,
                   COUNT(CASE WHEN v.ref_id IS NOT NULL AND TRIM(v.ref_id) != '' THEN 1 END) as HAS_REF_ID,
                   COUNT(CASE WHEN v.internal_series IS NOT NULL AND TRIM(v.internal_series) != '' THEN 1 END) as HAS_INTERNAL_SERIES,
                   COUNT(CASE WHEN v.episode_number IS NOT NULL AND TRIM(v.episode_number) != '' THEN 1 END) as HAS_EPISODE_NUM,
                   COUNT(CASE WHEN v.season_number IS NOT NULL AND TRIM(v.season_number) != '' THEN 1 END) as HAS_SEASON_NUM,
                   COUNT(CASE WHEN REGEXP_LIKE(v.episode_number, '^[0-9]+\$') THEN 1 END) as EPISODE_REGEX_MATCH,
                   COUNT(CASE WHEN REGEXP_LIKE(v.season_number, '^[0-9]+\$') THEN 1 END) as SEASON_REGEX_MATCH
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            `;

            logStep(`DEBUG: Checking FULL_DATA criteria match counts`, "DEBUG");
            const debugResult = snowflake.execute({sqlText: debugFullDataSql});
            if (debugResult.next()) {
                const counts = {
                    total: debugResult.getColumnValue('FULL_DATA_COUNT'),
                    hasRefId: debugResult.getColumnValue('HAS_REF_ID'),
                    hasInternalSeries: debugResult.getColumnValue('HAS_INTERNAL_SERIES'),
                    hasEpisodeNum: debugResult.getColumnValue('HAS_EPISODE_NUM'),
                    hasSeasonNum: debugResult.getColumnValue('HAS_SEASON_NUM'),
                    episodeRegex: debugResult.getColumnValue('EPISODE_REGEX_MATCH'),
                    seasonRegex: debugResult.getColumnValue('SEASON_REGEX_MATCH')
                };
                logStep(`FULL_DATA DEBUG: ${JSON.stringify(counts)}`, "DEBUG");
            }

            // For FULL_DATA, select records with full data
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''
            AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
            AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
            `;
        } else if (bucketType === "REF_ID_SERIES") {
            // For REF_ID_SERIES, ONLY include records that had full data but failed to match in FULL_DATA
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            `;
        } else if (bucketType === "REF_ID_ONLY") {
            // For REF_ID_ONLY, only include records with ref_id but no internal_series
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        } else if (bucketType === "SERIES_SEASON_EPISODE") {
            // For SERIES_SEASON_EPISODE, include records with internal_series, episode_number, season_number but no ref_id
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''
            AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
            AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
            `;
        } else if (bucketType === "SERIES_ONLY") {
            // For SERIES_ONLY, only include records with internal_series but no ref_id and missing/invalid episode OR season numbers
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND ((v.episode_number IS NULL OR TRIM(v.episode_number) = '' OR NOT REGEXP_LIKE(v.episode_number, '^[0-9]+$'))
                 OR (v.season_number IS NULL OR TRIM(v.season_number) = '' OR NOT REGEXP_LIKE(v.season_number, '^[0-9]+$')))
            `;
        }
        else if (bucketType === "TITLE_ONLY") {
            // For TITLE_ONLY, include remaining unmatched records that have a platform_content_name
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        }
        snowflake.execute({sqlText: createBucketSql});
        
        // Count the actual records in the bucket
        const countActualSql = `SELECT COUNT(*) AS ACTUAL_COUNT FROM UPLOAD_DB.PUBLIC.${bucketTableName}`;
        const actualCountResult = snowflake.execute({sqlText: countActualSql});
        let actualCount = 0;
        if (actualCountResult.next()) {
            actualCount = actualCountResult.getColumnValue('ACTUAL_COUNT');
        }
        logStep(`${bucketType}: Starting to process ${actualCount} records`, "INFO");
        // If actual data count in bucket is zero, we don't need to run bucket procs
        // Running procs with zero data count does this:
        //  It runs the handle conflict proc and this proc update unnecessary notes
        if (!actualCount) {
            logStep(`${bucketType}: Skipping - no records need processing`, "INFO");
            continue;
        };

        // Process this bucket with filename parameter
        const processSql = `
        CALL UPLOAD_DB.public.process_viewership_${bucketType.toLowerCase()}_generic(
            '${platformArg}'
            ${filenameArg ? `, '${filenameArg.replace(/'/g, "''")}'` : ', NULL'}
        )`;
        
        try {
            const bucketResult = snowflake.execute({sqlText: processSql});
            let bucketResultStr = "";
            if (bucketResult.next()) {
                bucketResultStr = bucketResult.getColumnValue(1);
            }
            
            // Extract rows affected from the result
            const bucketRowsMatch = bucketResultStr.match(/updated (\d+) rows/);
            const bucketRowsAffected = bucketRowsMatch ? bucketRowsMatch[1] : "0";
            const bucketRowsCount = parseInt(bucketRowsAffected, 10);
            
            logStep(`${bucketType}: Successfully updated ${bucketRowsAffected} records`, "SUCCESS", bucketRowsAffected);
            
            // IMPORTANT: Always count this bucket's results, even if zero rows were affected
            bucketResults.push(`${bucketType}: ${bucketRowsAffected} records`);
            totalProcessed += bucketRowsCount;
            
            // Update the unmatched records table to remove any we just matched
            const updateUnmatchedSql = `
            DELETE FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            WHERE EXISTS (
                SELECT 1
                FROM {{STAGING_DB}}.public.platform_viewership v
                WHERE v.id = u.id
                AND v.content_provider IS NOT NULL
            )`;
            
            snowflake.execute({sqlText: updateUnmatchedSql});
            
            // Count how many records remain in the unmatched table
            const remainingUnmatchedSql = `
            SELECT COUNT(*) AS REMAINING_UNMATCHED
            FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
            
            const remainingResult = snowflake.execute({sqlText: remainingUnmatchedSql});
            let remainingCount = 0;
            if (remainingResult.next()) {
                remainingCount = remainingResult.getColumnValue('REMAINING_UNMATCHED');
            }
            
            logStep(`After ${bucketType} processing, ${remainingCount} records still need processing by other strategies`, "INFO");
            
        } catch (err) {
            logStep(`Error processing ${bucketType} bucket: ${err.toString()}`, "ERROR");
        }
        
        // Clean up this bucket
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB.PUBLIC.${bucketTableName}`});
    }

    // Clean up all temporary bucket tables
    for (const bucketType of bucketOrder) {
        try {
            snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketType}_BUCKET`});
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
        FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
        
        const finalResult = snowflake.execute({sqlText: finalUnmatchedSql});
        let finalUnmatchedCount = 0;
        if (finalResult.next()) {
            finalUnmatchedCount = finalResult.getColumnValue('FINAL_UNMATCHED');
        }
        
        if (finalUnmatchedCount > 0) {
            logStep(`FINAL RESULT: ${finalUnmatchedCount} records could not be processed by any strategy`, "WARNING");
            
            // Sample some unmatched records for inspection - use EXISTS to avoid duplicates
            const sampleUnmatchedSql = `
            WITH sample_ids AS (
                SELECT DISTINCT id
                FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED
                LIMIT 5
            )
            SELECT v.id, v.platform_content_name, v.ref_id, v.internal_series, v.episode_number, v.season_number
            FROM {{STAGING_DB}}.public.platform_viewership v
            WHERE v.platform = '${platformArg}'
            AND EXISTS (SELECT 1 FROM sample_ids s WHERE s.id = v.id)
            `;
            
            const sampleResult = snowflake.execute({sqlText: sampleUnmatchedSql});
            let sampleRecords = [];
            while (sampleResult.next()) {
                sampleRecords.push({
                    id: sampleResult.getColumnValue('ID'),
                    title: sampleResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                    ref_id: sampleResult.getColumnValue('REF_ID'),
                    series: sampleResult.getColumnValue('INTERNAL_SERIES'),
                    episode: sampleResult.getColumnValue('EPISODE_NUMBER'),
                    season: sampleResult.getColumnValue('SEASON_NUMBER')
                });
            }
            
            if (sampleRecords.length > 0) {
                logStep(`Sample of unmatched records: ${JSON.stringify(sampleRecords)}`, "INFO");
            }
        }

        // Insert unmatched records into permanent record_reprocessing_batch_logs table
        try {
            const insertUnmatchedSql = `
                INSERT INTO METADATA_DB.public.record_reprocessing_batch_logs (
                    platform, filename, platform_content_name, platform_series, internal_series,
                    ref_id, season_number, episode_number, content_provider, asset_series,
                    asset_title, match_status, notes, created_at
                )
                SELECT
                    '${platformArg}' AS platform,
                    '${filenameArg ? filenameArg.replace(/'/g, "''") : ''}' AS filename,
                    platform_content_name,
                    platform_series,
                    internal_series,
                    ref_id,
                    season_number,
                    episode_number,
                    content_provider,
                    asset_series,
                    asset_title,
                    'UNMATCHED' AS match_status,
                    'No matching content found' AS notes,
                    CURRENT_TIMESTAMP() AS created_at
                FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED
            `;

            const insertResult = snowflake.execute({sqlText: insertUnmatchedSql});
            const insertedCount = insertResult.getNumRowsAffected();
            logStep(`Inserted ${insertedCount} unmatched records into record_reprocessing_batch_logs`, "INFO");
        } catch (insertErr) {
            logStep(`Warning: Failed to insert unmatched records to batch logs: ${insertErr.toString()}`, "WARNING");
        }

        // Clean up the unmatched records table
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`});
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
                'analyze_and_process_viewership_data',
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
;


GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC(VARCHAR, VARCHAR, ARRAY) TO ROLE web_app;

