-- STAGING: Match series from title using CONTAINS approach
-- This procedure:
-- 1. Searches for dictionary.public.series entries WITHIN PLATFORM_CONTENT_NAME
-- 2. Sets INTERNAL_SERIES to the matched series name
-- 3. Sends email notifications for unmatched titles
--
-- This is safer than extraction because it relies on known series names from the dictionary

-- Replace upload_db.public with actual schema (e.g., upload_db.public, test_staging.public)
-- Replace {{TABLE_NAME}} with actual table (e.g., platform_viewership)

CREATE OR REPLACE PROCEDURE upload_db.public.SET_INTERNAL_SERIES_WITH_EXTRACTION(
    "TABLE_NAME" VARCHAR,
    "FILTER_COLUMN" VARCHAR,  -- e.g., 'filename' or 'platform'
    "FILTER_VALUE" VARCHAR     -- e.g., 'file123.csv' or 'Karamo'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var tableName = TABLE_NAME;
    var filterColumn = FILTER_COLUMN;
    var filterValue = FILTER_VALUE;

    // Function to match series using CONTAINS approach
    // Searches for dictionary series names within the PLATFORM_CONTENT_NAME
    function matchSeriesInTitle() {
        try {
            // Update records where dictionary series is found in the title
            var sqlText = `
                UPDATE ${tableName}
                SET internal_series = x.matched_series
                FROM (
                    SELECT
                        v.id,
                        s.series AS matched_series,
                        ROW_NUMBER() OVER (
                            PARTITION BY v.id
                            ORDER BY LENGTH(s.entry) DESC
                        ) AS match_rank
                    FROM ${tableName} v
                    JOIN dictionary.public.series s
                    ON (
                        -- Case-insensitive CONTAINS check
                        LOWER(v.platform_content_name) LIKE LOWER('%' || s.entry || '%')
                        OR LOWER(v.platform_content_name) LIKE LOWER('%' || s.series || '%')
                    )
                    WHERE v.${filterColumn} = '${filterValue}'
                    AND v.processed IS NULL
                    AND v.platform_content_name IS NOT NULL
                    AND (v.internal_series IS NULL OR v.internal_series = '')
                ) x
                WHERE ${tableName}.id = x.id
                AND x.match_rank = 1  -- Take longest matching series name to avoid false positives
            `;

            snowflake.execute({sqlText: sqlText});

            // Get count of matched rows
            var countQuery = snowflake.createStatement({
                sqlText: `
                    SELECT COUNT(*) AS matched_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND internal_series IS NOT NULL
                `
            }).execute();

            var matchedCount = 0;
            if (countQuery.next()) {
                matchedCount = countQuery.getColumnValue('MATCHED_COUNT');
            }

            return {
                success: true,
                matchedCount: matchedCount
            };
        } catch (err) {
            throw "Function matchSeriesInTitle failed: " + err;
        }
    }

    // Function to check for unmatched titles (after matching attempt)
    function checkForUnmatchedTitles() {
        try {
            var unmatchedCountQuery = snowflake.createStatement({
                sqlText: `
                    SELECT COUNT(*) AS unmatched_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND platform_content_name IS NOT NULL
                    AND (internal_series IS NULL OR internal_series = '')
                `
            }).execute();

            var unmatchedCount = 0;
            if (unmatchedCountQuery.next()) {
                unmatchedCount = unmatchedCountQuery.getColumnValue('UNMATCHED_COUNT');
            }

            if (unmatchedCount === 0) {
                return {
                    unmatchedCount: 0,
                    unmatchedRecords: []
                };
            }

            // Get sample unmatched records
            var unmatchedRecordsQuery = snowflake.createStatement({
                sqlText: `
                    SELECT DISTINCT
                        platform_content_name,
                        platform,
                        filename,
                        COUNT(*) OVER (PARTITION BY platform_content_name) AS occurrence_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND platform_content_name IS NOT NULL
                    AND (internal_series IS NULL OR internal_series = '')
                    ORDER BY occurrence_count DESC
                    LIMIT 100
                `
            }).execute();

            var unmatchedRecords = [];
            while (unmatchedRecordsQuery.next()) {
                unmatchedRecords.push({
                    platform: unmatchedRecordsQuery.getColumnValue('PLATFORM') || 'N/A',
                    filename: unmatchedRecordsQuery.getColumnValue('FILENAME') || 'N/A',
                    platform_content_name: unmatchedRecordsQuery.getColumnValue('PLATFORM_CONTENT_NAME') || 'N/A',
                    occurrence_count: unmatchedRecordsQuery.getColumnValue('OCCURRENCE_COUNT') || 0
                });
            }

            return {
                unmatchedCount: unmatchedCount,
                unmatchedRecords: unmatchedRecords
            };
        } catch (err) {
            throw "Function checkForUnmatchedTitles failed: " + err;
        }
    }

    // Function to send email notification
    function sendEmail(matchResults, unmatchedResults) {
        try {
            var hasIssues = (unmatchedResults.unmatchedCount > 0);

            // Only send email if there are issues
            if (!hasIssues) {
                return "No email sent - all series matched successfully";
            }

            // Build HTML email
            var html_message = '<html><body style="font-family: Arial, sans-serif;">';

            // Header
            html_message += '<h2 style="color: #FF0000;">🚨 Unmatched Content Titles</h2>';
            html_message += '<p>Date: ' + new Date().toISOString().split('T')[0] + '</p>';
            html_message += '<p>Table: ' + tableName + '</p>';
            html_message += '<p>Filter: ' + filterColumn + ' = ' + filterValue + '</p>';

            // Statistics
            html_message += '<h3>Matching Statistics</h3>';
            html_message += '<ul>';
            html_message += '<li>Successfully matched: ' + matchResults.matchedCount + '</li>';
            html_message += '<li style="color: #FF0000;">Unmatched: ' + unmatchedResults.unmatchedCount + '</li>';
            html_message += '</ul>';

            // Show unmatched titles
            html_message += '<h3 style="color: #FF0000;">Unmatched Content Names</h3>';
            html_message += '<p>The following titles do not contain any series names from dictionary.public.series:</p>';
            html_message += '<p><strong>Action required:</strong> Either add these series to the dictionary or verify the content names are correct.</p>';

            html_message += '<table style="border-collapse: collapse; width: 100%; font-size: 12px;">';
            html_message += '<thead>';
            html_message += '<tr style="background-color: #f2f2f2;">';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Platform</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Content Name (Title)</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Occurrences</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Filename</th>';
            html_message += '</tr>';
            html_message += '</thead>';
            html_message += '<tbody>';

            for (var i = 0; i < unmatchedResults.unmatchedRecords.length; i++) {
                var unmatchedItem = unmatchedResults.unmatchedRecords[i];

                html_message += '<tr>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + unmatchedItem.platform + '</td>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px; max-width: 400px;">' +
                               unmatchedItem.platform_content_name + '</td>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' +
                               unmatchedItem.occurrence_count + '</td>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' +
                               unmatchedItem.filename + '</td>';
                html_message += '</tr>';
            }

            html_message += '</tbody></table>';

            if (unmatchedResults.unmatchedCount > 100) {
                html_message += '<p><em>Note: Showing first 100 unique titles out of ' +
                               unmatchedResults.unmatchedCount + ' total unmatched records (sorted by frequency).</em></p>';
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
                    '🚨 Unmatched Content: ' + filterColumn + '=' + filterValue,
                    html_message
                ]
            });

            return "Email sent for " + unmatchedResults.unmatchedCount + " unmatched titles";
        } catch (err) {
            throw "Function sendEmail failed: " + err;
        }
    }

    try {
        // Step 1: Match series using CONTAINS approach
        var matchResults = matchSeriesInTitle();

        // Step 2: Check for unmatched titles
        var unmatchedResults = checkForUnmatchedTitles();

        // Step 3: Send email if there are issues
        var emailResult = sendEmail(matchResults, unmatchedResults);

        // Step 4: Get total count for return message
        var totalRecords = matchResults.matchedCount + unmatchedResults.unmatchedCount;

        // Step 5: Return success message with statistics
        return "Series matching completed. " +
               "Total: " + totalRecords + ", " +
               "Matched: " + matchResults.matchedCount + ", " +
               "Unmatched: " + unmatchedResults.unmatchedCount + ". " +
               emailResult;
    } catch (err) {
        return "ERROR: " + err.toString();
    }
$$;
