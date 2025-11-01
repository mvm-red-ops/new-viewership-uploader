-- ========================================================================
-- CREATE GENERIC PHASE 2 STORED PROCEDURES
-- These procedures work with the platform_viewership table (generic architecture)
-- Run this file to create both procedures at once
-- ========================================================================

-- ========================================================================
-- 1. SET_INTERNAL_SERIES_GENERIC
-- ========================================================================

CREATE OR REPLACE PROCEDURE upload_db.public.set_internal_series_generic("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var platformArg = PLATFORM;
    var filenameArg = FILENAME;

    // Function to update internal_series based on dictionary.public.series
    function setInternalSeries() {
        try {
            // Perform the update
                var sqlText = `
                    UPDATE test_staging.public.platform_viewership
                    SET internal_series = x.validated_series
                    FROM (
                        SELECT
                            v.platform_series,
                            s.series AS validated_series
                        FROM test_staging.public.platform_viewership v
                        JOIN dictionary.public.series s
                        ON (LOWER(REGEXP_REPLACE(s.entry, '[^A-Za-z0-9 ]', '')) = LOWER(REGEXP_REPLACE(v.platform_series, '[^A-Za-z0-9 ]', '')))
                        WHERE v.platform = '${platformArg}'
                        AND v.processed IS NULL
                        AND v.filename = '${filenameArg}'
                        AND v.platform_series IS NOT NULL
                        GROUP BY all
                    ) x
                    WHERE test_staging.public.platform_viewership.platform_series = x.platform_series
                    AND test_staging.public.platform_viewership.platform = '${platformArg}'
                `;

            snowflake.execute({sqlText: sqlText});
            return "Succeeded";
        } catch (err) {
            return "Failed: " + err;
        }
    }

    // Function to check for NULL platform_series in test_staging
    function checkForNullPlatformSeries() {
        try {
            // Direct query to count NULLs in test_staging database
            var nullCountQuery = snowflake.createStatement({
                sqlText: `
                    SELECT COUNT(*) AS null_count
                    FROM test_staging.public.platform_viewership
                    WHERE platform = '${platformArg}'
                    AND processed IS NULL
                    AND filename = '${filenameArg}'
                    AND platform_series IS NULL
                `
            }).execute();

            var nullCount = 0;
            if (nullCountQuery.next()) {
                nullCount = nullCountQuery.getColumnValue('NULL_COUNT');
            }

            if (nullCount === 0) {
                return {
                    hasNulls: false,
                    nullCount: 0,
                    nullRecords: []
                };
            }

            // Get sample NULL records for the email - only get filename as it's guaranteed to exist
            var nullRecordsQuery = snowflake.createStatement({
                sqlText: `
                    SELECT
                        '${platformArg}' AS platform,
                        filename
                    FROM test_staging.public.platform_viewership
                    WHERE platform = '${platformArg}'
                    AND processed IS NULL
                    AND filename = '${filenameArg}'
                    AND platform_series IS NULL
                    LIMIT 100
                `
            }).execute();

            var nullRecords = [];
            while (nullRecordsQuery.next()) {
                nullRecords.push({
                    platform: nullRecordsQuery.getColumnValue('PLATFORM'),
                    filename: nullRecordsQuery.getColumnValue('FILENAME')
                });
            }

            return {
                hasNulls: true,
                nullCount: nullCount,
                nullRecords: nullRecords
            };
        } catch (err) {
            throw new Error("Error checking for NULL platform_series: " + err);
        }
    }

    // Function to get match statistics for non-NULL records in test_staging
    function getMatchStatistics() {
        try {
            var statsQuery = snowflake.createStatement({
                sqlText: `
                    SELECT
                        COUNT(*) AS total_records,
                        SUM(CASE WHEN internal_series IS NOT NULL THEN 1 ELSE 0 END) AS matched_records,
                        SUM(CASE WHEN internal_series IS NULL THEN 1 ELSE 0 END) AS unmatched_records
                    FROM test_staging.public.platform_viewership
                    WHERE platform = '${platformArg}'
                    AND processed IS NULL
                    AND filename = '${filenameArg}'
                    AND platform_series IS NOT NULL
                `
            }).execute();

            var stats = {
                total: 0,
                matched: 0,
                unmatched: 0,
                unmatchedList: []
            };

            if (statsQuery.next()) {
                stats.total = statsQuery.getColumnValue('TOTAL_RECORDS');
                stats.matched = statsQuery.getColumnValue('MATCHED_RECORDS');
                stats.unmatched = statsQuery.getColumnValue('UNMATCHED_RECORDS');
            }

            // Get unmatched series details
            var unmatchedList = [];
            if (stats.unmatched > 0) {
                var unmatchedQuery = snowflake.createStatement({
                    sqlText: `
                        SELECT DISTINCT
                            '${platformArg}' AS platform,
                            platform_series,
                            REGEXP_REPLACE(platform_series, '[^A-Za-z0-9 ]', '') AS cleaned_platform_series
                        FROM test_staging.public.platform_viewership
                        WHERE platform = '${platformArg}'
                        AND processed IS NULL
                        AND filename = '${filenameArg}'
                        AND internal_series IS NULL
                        AND platform_series IS NOT NULL
                        ORDER BY platform_series
                    `
                }).execute();

                while (unmatchedQuery.next()) {
                    unmatchedList.push({
                        platform: unmatchedQuery.getColumnValue('PLATFORM'),
                        series: unmatchedQuery.getColumnValue('PLATFORM_SERIES'),
                        cleaned_series: unmatchedQuery.getColumnValue('CLEANED_PLATFORM_SERIES')
                    });
                }
            }

            stats.unmatchedList = unmatchedList;
            return stats;
        } catch (err) {
            throw new Error("Error getting match statistics: " + err);
        }
    }

    // Function to send email notification about NULL platform_series
    function sendNullSeriesEmail(nullData) {
        try {
            var html = '<html><body style="font-family: Arial, sans-serif;">';
            html += '<h2 style="color: #FF0000;">⚠️ NULL Platform Series Detected</h2>';
            html += '<p><strong>Platform:</strong> ' + platformArg + '</p>';
            html += '<p><strong>Filename:</strong> ' + filenameArg + '</p>';
            html += '<p><strong>Records with NULL platform_series:</strong> ' + nullData.nullCount + '</p>';
            html += '<p>These records cannot be matched to internal series and will be skipped.</p>';

            // Show sample records
            html += '<h3>Sample Records (first 100)</h3>';
            html += '<table style="border-collapse: collapse; width: 100%;">';
            html += '<tr style="background-color: #f2f2f2;">';
            html += '<th style="border: 1px solid #ddd; padding: 8px;">Platform</th>';
            html += '<th style="border: 1px solid #ddd; padding: 8px;">Filename</th>';
            html += '</tr>';

            for (var i = 0; i < Math.min(100, nullData.nullRecords.length); i++) {
                var record = nullData.nullRecords[i];
                html += '<tr>';
                html += '<td style="border: 1px solid #ddd; padding: 8px;">' + record.platform + '</td>';
                html += '<td style="border: 1px solid #ddd; padding: 8px;">' + record.filename + '</td>';
                html += '</tr>';
            }

            html += '</table>';
            html += '</body></html>';

            snowflake.execute({
                sqlText: `
                CALL SYSTEM$SEND_EMAIL(
                    'SNOWFLAKE_EMAIL_SENDER',
                    'tayloryoung@mvmediasales.com, data@nosey.com',
                    ?,
                    ?,
                    'text/html'
                )`,
                binds: [
                    "⚠️ NULL Platform Series - " + platformArg + " - " + filenameArg,
                    html
                ]
            });

            return "NULL series email sent";
        } catch (err) {
            return "Failed to send NULL series email: " + err;
        }
    }

    // Function to send email notification about match statistics
    function sendMatchStatisticsEmail(stats) {
        try {
            var matchRate = stats.total > 0 ? ((stats.matched / stats.total) * 100).toFixed(2) : 0;

            var html = '<html><body style="font-family: Arial, sans-serif;">';
            html += '<h2>📊 Series Matching Statistics</h2>';
            html += '<p><strong>Platform:</strong> ' + platformArg + '</p>';
            html += '<p><strong>Filename:</strong> ' + filenameArg + '</p>';
            html += '<hr>';
            html += '<h3>Summary</h3>';
            html += '<p><strong>Total Records Processed:</strong> ' + stats.total + '</p>';
            html += '<p style="color: green;"><strong>✓ Matched:</strong> ' + stats.matched + '</p>';
            html += '<p style="color: red;"><strong>✗ Unmatched:</strong> ' + stats.unmatched + '</p>';
            html += '<p><strong>Match Rate:</strong> ' + matchRate + '%</p>';

            if (stats.unmatched > 0) {
                html += '<hr>';
                html += '<h3>Unmatched Series</h3>';
                html += '<p>The following series could not be matched to the dictionary:</p>';
                html += '<table style="border-collapse: collapse; width: 100%;">';
                html += '<tr style="background-color: #f2f2f2;">';
                html += '<th style="border: 1px solid #ddd; padding: 8px;">Platform</th>';
                html += '<th style="border: 1px solid #ddd; padding: 8px;">Platform Series</th>';
                html += '<th style="border: 1px solid #ddd; padding: 8px;">Cleaned Series</th>';
                html += '</tr>';

                for (var i = 0; i < stats.unmatchedList.length; i++) {
                    var series = stats.unmatchedList[i];
                    html += '<tr>';
                    html += '<td style="border: 1px solid #ddd; padding: 8px;">' + series.platform + '</td>';
                    html += '<td style="border: 1px solid #ddd; padding: 8px;">' + series.series + '</td>';
                    html += '<td style="border: 1px solid #ddd; padding: 8px;">' + series.cleaned_series + '</td>';
                    html += '</tr>';
                }

                html += '</table>';
            }

            html += '</body></html>';

            snowflake.execute({
                sqlText: `
                CALL SYSTEM$SEND_EMAIL(
                    'SNOWFLAKE_EMAIL_SENDER',
                    'tayloryoung@mvmediasales.com, data@nosey.com',
                    ?,
                    ?,
                    'text/html'
                )`,
                binds: [
                    "📊 Series Matching Results - " + platformArg + " - " + filenameArg,
                    html
                ]
            });

            return "Statistics email sent";
        } catch (err) {
            return "Failed to send statistics email: " + err;
        }
    }

    try {
        // Check for NULL platform_series
        var nullCheck = checkForNullPlatformSeries();
        if (nullCheck.hasNulls) {
            sendNullSeriesEmail(nullCheck);
        }

        // Set internal series for non-NULL platform_series
        var setResult = setInternalSeries();

        // Get match statistics
        var stats = getMatchStatistics();

        // Send statistics email
        sendMatchStatisticsEmail(stats);

        return "Set internal series completed: " + stats.matched + " matched, " + stats.unmatched + " unmatched";
    } catch (err) {
        // Catch and return any errors
        return err.toString();
    }
$$;

-- Grant usage permission
GRANT USAGE ON PROCEDURE upload_db.public.set_internal_series_generic(VARCHAR, VARCHAR) TO ROLE web_app;


-- ========================================================================
-- 2. ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC
-- ========================================================================

CREATE OR REPLACE PROCEDURE upload_db.public.analyze_and_process_viewership_data_generic("PLATFORM" VARCHAR, "FILENAME" VARCHAR, "TITLES_ARRAY" ARRAY DEFAULT null)
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
    const viewershipTable = `test_staging.public.platform_viewership`;

    // Start time for procedure execution
    const startTime = new Date();

    // Helper function to log steps with timestamps
    function logStep(message, status = "INFO", rowsAffected = "", errorMessage = "") {
        const logSql = `
        INSERT INTO upload_db.public.ERROR_LOG_TABLE (
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
            DATEDIFF(second, '${startTime.toISOString()}', CURRENT_TIMESTAMP())
        )
        `;

        try {
            snowflake.execute({sqlText: logSql});
        } catch (logErr) {
            // If logging fails, just continue - don't break execution
            console.log("Logging failed: " + logErr.toString());
        }
    }

    logStep("Starting analyze_and_process_viewership_data for platform: " + platformArg, "START");

    // Build titles condition if titles array is provided
    let titlesCondition = "";
    if (titlesArrayArg && titlesArrayArg.length > 0) {
        const titlesListSql = titlesArrayArg.map(t => `'${t.replace(/'/g, "''")}'`).join(',');
        titlesCondition = `AND platform_content_name IN (${titlesListSql})`;
        logStep(`Filtering by ${titlesArrayArg.length} specific titles`, "INFO");
    }

    // Early debug logging
    try {
        const debugSql = `
        SELECT
            COUNT(*) as total_count,
            COUNT(CASE WHEN content_provider IS NULL THEN 1 END) as null_provider_count,
            COUNT(CASE WHEN platform_content_name IS NOT NULL THEN 1 END) as has_content_name_count
        FROM ${viewershipTable}
        WHERE platform = '${platformArg}'
        AND processed IS NULL
        ${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
        `;

        const debugResult = snowflake.execute({sqlText: debugSql});
        if (debugResult.next()) {
            const debugInfo = {
                total: debugResult.getColumnValue('TOTAL_COUNT'),
                nullProvider: debugResult.getColumnValue('NULL_PROVIDER_COUNT'),
                hasContentName: debugResult.getColumnValue('HAS_CONTENT_NAME_COUNT')
            };
            logStep(`EARLY DEBUG: total=${debugInfo.total}, null_provider=${debugInfo.nullProvider}, has_content_name=${debugInfo.hasContentName}`, "DEBUG");
        }
    } catch (earlyErr) {
        logStep(`EARLY DEBUG FAILED: ${earlyErr.toString()}`, "ERROR");
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

        logStep(`Found ${recordCount} records to process`, "INFO");

        if (recordCount === 0) {
            logStep("No records found matching criteria - exiting early", "WARNING");
            return "No records found matching criteria";
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
        ["REF_ID_SERIES", true, true, false, false],
        ["REF_ID_ONLY", true, false, false, false],
        ["SERIES_SEASON_EPISODE", false, true, true, true],
        ["SERIES_ONLY", false, true, false, false],
        ["TITLE_ONLY", false, false, false, false]
    ];

    // Mapping of bucket types to their corresponding stored procedures
    const bucketProcedures = {
        "FULL_DATA": "upload_db.public.match_ref_series_episode_season",
        "REF_ID_SERIES": "upload_db.public.match_ref_series",
        "REF_ID_ONLY": "upload_db.public.match_ref",
        "SERIES_SEASON_EPISODE": "upload_db.public.match_series_episode_season",
        "SERIES_ONLY": "upload_db.public.match_series",
        "TITLE_ONLY": "upload_db.public.match_title"
    };

    let totalProcessed = 0;
    const bucketResults = [];


    // First, manually create the unmatched records table with all records
    const createUnmatchedSql = `
    CREATE OR REPLACE TABLE upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED AS
    SELECT DISTINCT id
    FROM test_staging.public.platform_viewership
    WHERE platform = '${platformArg}'
    AND processed IS NULL
    AND content_provider IS NULL
    AND platform_content_name IS NOT NULL
    ${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}
    ${titlesCondition}
    `;

    snowflake.execute({sqlText: createUnmatchedSql});

    // Check the initial count of records in the unmatched table
    const initialUnmatchedCountSql = `SELECT COUNT(*) AS CNT FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
    const initialCountResult = snowflake.execute({sqlText: initialUnmatchedCountSql});
    let initialUnmatchedCount = 0;
    if (initialCountResult.next()) {
        initialUnmatchedCount = initialCountResult.getColumnValue('CNT');
    }
    logStep(`Initial unmatched records count: ${initialUnmatchedCount}`, "INFO");

    // Process each bucket category
    for (let i = 0; i < bucketCategories.length; i++) {
        const [bucketType, needsRefId, needsSeries, needsEpisode, needsSeason] = bucketCategories[i];
        const bucketTableName = `TEMP_${platformArg.toUpperCase()}_${bucketType}`;

        logStep(`Processing bucket: ${bucketType}`, "INFO");

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
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            ORDER BY u.id
            `;

            logStep(`DEBUG: Sample unmatched records for FULL_DATA analysis`, "DEBUG");
            const sampleResult = snowflake.execute({sqlText: sampleDataSql});
            let sampleData = [];
            while (sampleResult.next()) {
                sampleData.push({
                    ref_id: sampleResult.getColumnValue('REF_ID'),
                    series: sampleResult.getColumnValue('INTERNAL_SERIES'),
                    ep: sampleResult.getColumnValue('EPISODE_NUMBER'),
                    season: sampleResult.getColumnValue('SEASON_NUMBER'),
                    content: sampleResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                    ref_len: sampleResult.getColumnValue('REF_ID_LEN'),
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
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            `;

            logStep(`DEBUG: Checking FULL_DATA criteria match counts`, "DEBUG");
            const debugResult = snowflake.execute({sqlText: debugFullDataSql});
            if (debugResult.next()) {
                const counts = {
                    total: debugResult.getColumnValue('FULL_DATA_COUNT'),
                    hasRefId: debugResult.getColumnValue('HAS_REF_ID'),
                    hasSeries: debugResult.getColumnValue('HAS_INTERNAL_SERIES'),
                    hasEp: debugResult.getColumnValue('HAS_EPISODE_NUM'),
                    hasSeason: debugResult.getColumnValue('HAS_SEASON_NUM'),
                    epRegex: debugResult.getColumnValue('EPISODE_REGEX_MATCH'),
                    seasonRegex: debugResult.getColumnValue('SEASON_REGEX_MATCH')
                };
                logStep(`FULL_DATA DEBUG: ${JSON.stringify(counts)}`, "DEBUG");
            }

            // For FULL_DATA, select records with full data
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
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
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            `;
        } else if (bucketType === "REF_ID_ONLY") {
            // For REF_ID_ONLY, only include records with ref_id but no internal_series
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        } else if (bucketType === "SERIES_SEASON_EPISODE") {
            // For SERIES_SEASON_EPISODE, include records with internal_series, episode_number, season_number but no ref_id
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
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
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
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
            CREATE OR REPLACE TEMPORARY TABLE upload_db.public.${bucketTableName} AS
            SELECT u.id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN test_staging.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        }
        snowflake.execute({sqlText: createBucketSql});

        // Count the actual records in the bucket
        const countBucketSql = `SELECT COUNT(*) AS BUCKET_COUNT FROM upload_db.public.${bucketTableName}`;
        const bucketCountResult = snowflake.execute({sqlText: countBucketSql});
        let bucketCount = 0;
        if (bucketCountResult.next()) {
            bucketCount = bucketCountResult.getColumnValue('BUCKET_COUNT');
        }

        logStep(`Bucket ${bucketType}: ${bucketCount} records`, "INFO");

        if (bucketCount === 0) {
            logStep(`Skipping ${bucketType} - no records in bucket`, "INFO");
            continue;
        }

        // Call the appropriate matching procedure
        const procedureName = bucketProcedures[bucketType];
        const callProcSql = `CALL ${procedureName}('${bucketTableName}')`;

        try {
            logStep(`Calling ${procedureName} for ${bucketType}`, "INFO");
            snowflake.execute({sqlText: callProcSql});

            // Check how many were actually matched
            const matchedCountSql = `
            SELECT COUNT(*) AS MATCHED_COUNT
            FROM upload_db.public.${bucketTableName} b
            JOIN ${viewershipTable} v ON b.id = v.id
            WHERE v.platform = '${platformArg}' AND v.content_provider IS NOT NULL
            `;
            const matchedResult = snowflake.execute({sqlText: matchedCountSql});
            let matchedCount = 0;
            if (matchedResult.next()) {
                matchedCount = matchedResult.getColumnValue('MATCHED_COUNT');
            }

            logStep(`Bucket ${bucketType}: ${matchedCount} records matched`, "INFO");

            // Remove matched records from unmatched table
            const removeMatchedSql = `
            DELETE FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED
            WHERE id IN (
                SELECT b.id
                FROM upload_db.public.${bucketTableName} b
                JOIN ${viewershipTable} v ON b.id = v.id
                WHERE v.platform = '${platformArg}' AND v.content_provider IS NOT NULL
            )
            `;
            snowflake.execute({sqlText: removeMatchedSql});

            totalProcessed += matchedCount;

            bucketResults.push({
                bucket: bucketType,
                totalInBucket: bucketCount,
                matched: matchedCount,
                procedure: procedureName
            });

        } catch (procErr) {
            logStep(`Error in ${bucketType}: ${procErr.toString()}`, "ERROR");
        }
    }

    // Check final unmatched count
    const finalUnmatchedCountSql = `SELECT COUNT(*) AS CNT FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;
    const finalCountResult = snowflake.execute({sqlText: finalUnmatchedCountSql});
    let finalUnmatchedCount = 0;
    if (finalCountResult.next()) {
        finalUnmatchedCount = finalCountResult.getColumnValue('CNT');
    }

    logStep(`Processing complete: ${totalProcessed} matched, ${finalUnmatchedCount} unmatched`, "SUCCESS");

    if (finalUnmatchedCount > 0) {
        logStep(`FINAL RESULT: ${finalUnmatchedCount} records could not be processed by any strategy`, "WARNING");

        // Sample some unmatched records for inspection - use EXISTS to avoid duplicates
        const sampleUnmatchedSql = `
        WITH sample_ids AS (
            SELECT DISTINCT id
            FROM upload_db.public.TEMP_${platformArg.toUpperCase()}_UNMATCHED
            LIMIT 5
        )
        SELECT v.id, v.platform_content_name, v.ref_id, v.internal_series, v.episode_number, v.season_number
        FROM test_staging.public.platform_viewership v
        WHERE v.platform = '${platformArg}'
        AND EXISTS (SELECT 1 FROM sample_ids s WHERE s.id = v.id)
        `;

        const sampleResult = snowflake.execute({sqlText: sampleUnmatchedSql});
        let sampleRecords = [];
        while (sampleResult.next()) {
            sampleRecords.push({
                id: sampleResult.getColumnValue('ID'),
                content_name: sampleResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                ref_id: sampleResult.getColumnValue('REF_ID'),
                series: sampleResult.getColumnValue('INTERNAL_SERIES'),
                episode: sampleResult.getColumnValue('EPISODE_NUMBER'),
                season: sampleResult.getColumnValue('SEASON_NUMBER')
            });
        }

        logStep(`Unmatched sample: ${JSON.stringify(sampleRecords)}`, "WARNING");
    }

    // Return summary
    const summary = {
        platform: platformArg,
        filename: filenameArg,
        totalProcessed: totalProcessed,
        finalUnmatched: finalUnmatchedCount,
        bucketResults: bucketResults
    };

    return JSON.stringify(summary);

} catch (err) {
    const errorMsg = "Error in analyze_and_process_viewership_data: " + err.toString();

    // Log error
    try {
        snowflake.execute({
            sqlText: `
            INSERT INTO upload_db.public.ERROR_LOG_TABLE (
                LOG_TIME, LOG_MESSAGE, PROCEDURE_NAME, PLATFORM, STATUS
            ) VALUES (
                CURRENT_TIMESTAMP(),
                ?,
                'analyze_and_process_viewership_data_generic',
                ?,
                'ERROR'
            )`,
            binds: [errorMsg, PLATFORM]
        });
    } catch (logErr) {
        // Ignore logging errors
    }

    return errorMsg;
    }
    $$;
;


GRANT USAGE ON PROCEDURE upload_db.public.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR, ARRAY) TO ROLE web_app;
