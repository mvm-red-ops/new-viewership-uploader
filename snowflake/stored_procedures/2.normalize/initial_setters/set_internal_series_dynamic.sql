GRANT USAGE ON PROCEDURE upload_db.public.set_internal_series_generic(VARCHAR, VARCHAR) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_INTERNAL_SERIES_GENERIC("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
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
            throw "Function setInternalSeries failed: " + err;
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
            
            // If no NULL platform_series, don't need further processing
            if (nullCount === 0) {
                return {
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
                nullCount: nullCount,
                nullRecords: nullRecords
            };
        } catch (err) {
            throw "Function checkForNullPlatformSeries failed: " + err;
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
                unmatched: 0
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
                        platform_series: unmatchedQuery.getColumnValue('PLATFORM_SERIES'),
                        cleaned_series: unmatchedQuery.getColumnValue('CLEANED_PLATFORM_SERIES')
                    });
                }
            }
            
            return {
                stats: stats,
                unmatchedList: unmatchedList
            };
        } catch (err) {
            throw "Function getMatchStatistics failed: " + err;
        }
    }
    
    // Function to send email notification
    function sendEmail(matchResults, nullResults) {
        try {
            var hasIssues = (nullResults.nullCount > 0) || 
                           (matchResults.unmatchedList.length > 0);
            
            // Only send email if there are issues
            if (!hasIssues) {
                return "No email sent - all series matched and no NULL platform_series found";
            }
            
            // Build HTML email
            var html_message = '<html><body style="font-family: Arial, sans-serif;">';
            
            // Header
            html_message += '<h2 style="color: #FF0000;">🚨 Platform Series Issues Detected</h2>';
            html_message += '<p>Date: ' + new Date().toISOString().split('T')[0] + '</p>';
            html_message += '<p>Platform: ' + platformArg + '</p>';
            html_message += '<p>Filename: ' + filenameArg + '</p>';
            
            // Statistics
            html_message += '<h3>Match Statistics</h3>';
            html_message += '<ul>';
            html_message += '<li>Non-NULL records: ' + matchResults.stats.total + '</li>';
            html_message += '<li>Matched records: ' + matchResults.stats.matched + '</li>';
            html_message += '<li>Unmatched records: ' + matchResults.stats.unmatched + '</li>';
            html_message += '<li style="color: ' + (nullResults.nullCount > 0 ? '#FF0000' : '#000000') + ';">Records with NULL platform_series: ' + nullResults.nullCount + '</li>';
            html_message += '</ul>';
            
            // First show NULL platform_series if any exist
            if (nullResults.nullCount > 0) {
                html_message += '<h3 style="color: #FF0000;">Records with NULL platform_series</h3>';
                html_message += '<p>There are ' + nullResults.nullCount + ' records with NULL platform_series, which is considered an error.</p>';
                
                // Only show table if we have records with details
                if (nullResults.nullRecords.length > 0) {
                    html_message += '<table style="border-collapse: collapse; width: 100%;">';
                    html_message += '<thead>';
                    html_message += '<tr style="background-color: #f2f2f2;">';
                    html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Platform</th>';
                    html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Filename</th>';
                    html_message += '</tr>';
                    html_message += '</thead>';
                    html_message += '<tbody>';
                    
                    // Add rows for each NULL platform_series record
                    for (var i = 0; i < nullResults.nullRecords.length; i++) {
                        var nullItem = nullResults.nullRecords[i];
                        
                        html_message += '<tr>';
                        html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + nullItem.platform + '</td>';
                        html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + nullItem.filename + '</td>';
                        html_message += '</tr>';
                    }
                    
                    html_message += '</tbody></table>';
                    
                    // If we limited the results, note that
                    if (nullResults.nullCount > 100) {
                        html_message += '<p><em>Note: Showing first 100 records out of ' + nullResults.nullCount + ' total NULL platform_series records.</em></p>';
                    }
                }
            }
            
            // Then show unmatched series if any exist
            if (matchResults.unmatchedList.length > 0) {
                html_message += '<h3>Unmatched Platform Series</h3>';
                html_message += '<p>The following platform series do not have matching entries in dictionary.public.series:</p>';
                
                html_message += '<table style="border-collapse: collapse; width: 100%;">';
                html_message += '<thead>';
                html_message += '<tr style="background-color: #f2f2f2;">';
                html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Platform</th>';
                html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Platform Series</th>';
                html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Cleaned Platform Series</th>';
                html_message += '</tr>';
                html_message += '</thead>';
                html_message += '<tbody>';
                
                // Add rows for each unmatched series
                for (var i = 0; i < matchResults.unmatchedList.length; i++) {
                    var unmatchedItem = matchResults.unmatchedList[i];
                    
                    html_message += '<tr>';
                    html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + unmatchedItem.platform + '</td>';
                    html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + unmatchedItem.platform_series + '</td>';
                    html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + unmatchedItem.cleaned_series + '</td>';
                    html_message += '</tr>';
                }
                
                html_message += '</tbody></table>';
            }
            
            html_message += '</body></html>';
            
            // Send email
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
                    '🚨 URGENT: Null Values Detected for Series in ' + platformArg + ' - ' + filenameArg,
                    html_message
                ]    
            });
            
            // Prepare return message
            var returnMsg = "Email notification sent for ";
            if (matchResults.unmatchedList.length > 0) {
                returnMsg += matchResults.unmatchedList.length + " unmatched series";
            }
            if (nullResults.nullCount > 0) {
                if (matchResults.unmatchedList.length > 0) {
                    returnMsg += " and ";
                }
                returnMsg += nullResults.nullCount + " NULL platform_series records";
                if (nullResults.nullCount > 100) {
                    returnMsg += " (showing first 100)";
                }
            }
            
            return returnMsg;
        } catch (err) {
            throw "Function sendEmail failed: " + err;
        }
    }

    try {
        // Step 1: Call the setInternalSeries function
        var seriesResult = setInternalSeries();
        if (seriesResult !== "Succeeded") {
            return seriesResult;
        }
        
        // Step 2: Check for NULL platform_series - do this first and separately
        var nullResults = checkForNullPlatformSeries();
        
        // Step 3: Get match statistics for non-NULL records
        var matchResults = getMatchStatistics();
        
        // Step 4: Send email if there are issues
        var emailResult = sendEmail(matchResults, nullResults);
        
        // Step 5: Return success message with statistics
        var totalRecords = matchResults.stats.total + nullResults.nullCount;
        
        return "Internal series updated successfully. " +
               "Total: " + totalRecords + ", " +
               "Matched: " + matchResults.stats.matched + ", " +
               "Unmatched: " + matchResults.stats.unmatched + ", " +
               "NULL platform_series: " + nullResults.nullCount + ". " +
               emailResult;
    } catch (err) {
        // Catch and return any errors
        return err.toString();
    }
$$;

-- Grant usage permission
GRANT USAGE ON PROCEDURE upload_db.public.set_internal_series_generic(VARCHAR, VARCHAR) TO ROLE web_app;