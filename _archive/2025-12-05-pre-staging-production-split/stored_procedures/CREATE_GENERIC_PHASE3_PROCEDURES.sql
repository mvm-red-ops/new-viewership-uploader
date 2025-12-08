-- ==============================================================================
-- PHASE 3 GENERIC PROCEDURES - CONSOLIDATED DEPLOYMENT FILE
-- ==============================================================================
-- This file contains all Phase 3 stored procedures updated for generic
-- platform_viewership table architecture.
--
-- Procedures in this file:
-- 1. move_data_to_final_table_dynamic_generic - Moves data from staging to final table
-- 2. handle_final_insert_dynamic_generic - Orchestrates Phase 3 execution
--
-- These procedures work with the generic platform_viewership table instead of
-- platform-specific tables (e.g., amagi_viewership, wurl_viewership).
--
-- Deploy by running this entire file in Snowflake.
-- ==============================================================================

-- ==============================================================================
-- PROCEDURE 1: move_data_to_final_table_dynamic_generic
-- ==============================================================================
-- Moves processed data from test_staging.public.platform_viewership to
-- staging_assets.public.episode_details_test_staging.
-- Handles both viewership and revenue data types.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE upload_db.public.move_data_to_final_table_dynamic_generic("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
let sql_command;
try {
    const platform = PLATFORM.toLowerCase();
    const type = TYPE.toLowerCase();
    const lowerFilename = FILENAME.toLowerCase();

    // Log procedure start
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Starting procedure - Platform: ${PLATFORM}, Type: ${TYPE}, Filename: ${FILENAME}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
    });

    // if type is viewership or viewership + revenue
    if (type.includes("viewership")) {
        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Starting viewership INSERT`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });

        sql_command = `
            INSERT INTO staging_assets.public.episode_details_test_staging(viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, minutes, hours, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views)
            SELECT id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), sum(tot_mov), sum(tot_hov), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Viewership', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views
            FROM test_staging.public.platform_viewership
            WHERE platform = '${PLATFORM}'
            AND deal_parent is not null
            AND processed is null
            AND ref_id is not null
            AND asset_series is not null
            AND tot_mov is not null
            AND tot_hov is not null
            AND LOWER(filename) = '${lowerFilename}'
            GROUP BY all;
        `;

        const stmt = snowflake.createStatement({sqlText: sql_command});
        stmt.execute();
        const rowsAffected = stmt.getNumRowsAffected();

        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Viewership INSERT completed. Rows affected: ${rowsAffected}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });
    }

    // if type is revenue or viewership + revenue
    if (type.includes("revenue")) {
        // Check if there are records that should match revenue criteria first
        const revenueCheckQuery = `
            SELECT COUNT(*) FROM test_staging.public.platform_viewership
            WHERE platform = '${PLATFORM}'
            AND deal_parent is not null
            AND processed is null
            AND ref_id is not null
            AND asset_series is not null
            AND revenue is not null
            AND revenue > 0
            AND LOWER(filename) = '${lowerFilename}'
        `;
        const revenueCheckResult = snowflake.execute({sqlText: revenueCheckQuery});
        let revenueCount = 0;
        if (revenueCheckResult.next()) {
            revenueCount = revenueCheckResult.getColumnValue(1);
        }

        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Revenue records found: ${revenueCount}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });

        if (revenueCount > 0) {
            sql_command = `
                insert into staging_assets.public.episode_details_test_staging(
                    viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views,
                    register_name, payment_amount, revenue_amount, payment_date, payment_type, payment_title, payment_description, payment_department, payment_adjustment, payment_quarter, payment_year, payment_month, payment_support_category, payment_filename
                )
                select
                    id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Revenue', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views,
                    CONCAT(partner, ' Revenue ', territory), revenue, revenue, year_month_day, '', '', '', '', 'False', quarter, year, month, 'Revenue', filename
                from test_staging.public.platform_viewership
                WHERE platform = '${PLATFORM}'
                AND deal_parent is not null
                AND processed is null
                AND ref_id is not null
                AND asset_series is not null
                AND revenue is not null
                AND revenue > 0
                AND LOWER(filename) = '${lowerFilename}'
                GROUP BY ALL
            `;

            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`Executing revenue INSERT for ${revenueCount} records`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });

            const revenueStmt = snowflake.createStatement({sqlText: sql_command});
            revenueStmt.execute();
            const revenueRowsAffected = revenueStmt.getNumRowsAffected();

            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`Revenue INSERT completed. Rows affected: ${revenueRowsAffected}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });
        } else {
            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`No revenue records found - skipping revenue INSERT`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });
        }
    }

    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Procedure completed successfully`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
    });

    return "Data moved successfully.";

} catch (error) {
    const errorMessage = "Error in sql query: " + sql_command + " error: " + error;
    console.error("Error executing SQL command:", error.message);
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [`PROCEDURE FAILED: ${errorMessage}`, 'move_data_to_final_table_dynamic_generic', PLATFORM, error.message]
    });
    return "Error executing SQL command: " + error.message;
}
$$;

GRANT USAGE ON PROCEDURE upload_db.public.move_data_to_final_table_dynamic_generic(VARCHAR, VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PROCEDURE 2: handle_final_insert_dynamic_generic
-- ==============================================================================
-- Orchestrates Phase 3 execution:
-- 1. Validates data (if validation procedure exists)
-- 2. Moves data to final table
-- 3. Updates phase to 3
-- ==============================================================================

CREATE OR REPLACE PROCEDURE upload_db.public.handle_final_insert_dynamic_generic("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function executeStoredProcedure(procName, args) {
        try {
            var sqlCommand = `CALL ` + procName + (args.length > 0 ? `(${args.map(arg => arg === null ? 'NULL' : `'${String(arg).replace(/'/g, "''")}'`).join(',')})` : '()') + `;`;
            var result = snowflake.createStatement({sqlText: sqlCommand}).execute();
            if (result.next()) {
                return { success: true, message: procName + " executed successfully.", data: result.getColumnValue(1) };
            }
            return { success: true, message: procName + " executed successfully." };
        } catch (err) {
            // Logging the error to the console
            console.error("Error executing " + procName + ": " + err.message);
            // Returning error details for further handling
            return { success: false, message: "Error executing " + procName + ": " + err.message };
        }
    }

    function sendValidationErrorEmail(platform, filename, validationData) {
        try {
            // Format date for email
            var today = new Date();
            var dateStr = today.toISOString().split("T")[0];

            // Build HTML email content
            var html_message = '<html><body style="font-family: Arial, sans-serif;">';
            html_message += '<h2 style="color: #FF0000;">🚨 URGENT! Validation Errors in Viewership Data</h2>';
            html_message += '<p><strong>Platform:</strong> ' + platform + '</p>';
            if (filename) {
                html_message += '<p><strong>Filename:</strong> ' + filename + '</p>';
            }
            html_message += '<p><strong>Date:</strong> ' + dateStr + '</p>';
            html_message += '<p><strong>Total Records Checked:</strong> ' + validationData.validationCount + '</p>';
            html_message += '<p><strong>Total Errors Found:</strong> ' + validationData.errors.length + '</p>';

            // Show error summary
            html_message += "<h3>Error Summary</h3>";

            // Group errors by type
            var errorTypes = {};
            validationData.errors.forEach(function(err) {
                if (!errorTypes[err.error]) {
                    errorTypes[err.error] = 0;
                }
                errorTypes[err.error]++;
            });

            html_message += "<ul>";
            for (var errorType in errorTypes) {
                html_message += '<li>' + errorType + ': ' + errorTypes[errorType] + ' records</li>';
            }
            html_message += "</ul>";

            // Show detailed errors (limit to first 50 for email size)
            var errorLimit = Math.min(50, validationData.errors.length);
            html_message += '<h3>Detailed Errors (showing first ' + errorLimit + ' of ' + validationData.errors.length + ')</h3>';
            html_message += '<table style="border-collapse: collapse; width: 100%;">';
            html_message += '<thead>';
            html_message += '<tr style="background-color: #f2f2f2;">';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Record ID</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Error Type</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Details</th>';
            html_message += '</tr>';
            html_message += '</thead>';
            html_message += '<tbody>';

            // Add rows for each error (up to limit)
            for (var i = 0; i < errorLimit; i++) {
                var err = validationData.errors[i];
                html_message += '<tr>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + err.id + '</td>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + err.error + '</td>';

                // Build details column based on error fields
                var details = "";
                for (var key in err) {
                    if (key !== 'id' && key !== 'error') {
                        details += '<strong>' + key + ':</strong> ' + err[key] + '<br>';
                    }
                }

                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + details + '</td>';
                html_message += '</tr>';
            }

            html_message += '</tbody></table>';
            html_message += '<p><strong>Action Required:</strong> Please fix these validation errors before attempting to insert the data again.</p>';
            html_message += '</body></html>';

            // Send email using the monitoring procedure
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
                    "🚨 URGENT: Viewership Validation Errors - " + platform + (filename ? ' - ' + filename : ''),
                    html_message
                ]
            });

            return { success: true, message: "Validation error email sent." };
        } catch (err) {
            console.error("Error sending validation error email: " + err.message);
            return { success: false, message: "Error sending validation error email: " + err.message };
        }
    }

    // First, run the validation procedure (if it exists)
    // Note: validate_viewership_for_insert may need to be created or updated for generic architecture
    try {
        var validationResult = executeStoredProcedure("upload_db.public.validate_viewership_for_insert", [PLATFORM, FILENAME]);

        if (validationResult.success && validationResult.data) {
            // Check validation results
            var validationData = validationResult.data;
            if (!validationData.valid) {
                // Send email notification about validation errors
                var emailResult = sendValidationErrorEmail(PLATFORM, FILENAME, validationData);

                // Log the validation failure and return - THIS TERMINATES THE PROCESS
                var errorCount = validationData.errors.length;
                var recordCount = validationData.validationCount;
                var errorMessage = `Validation failed: ${errorCount} errors found in ${recordCount} records. Email notification sent.`;
                console.error(errorMessage);

                // Return detailed error message
                return errorMessage;
            }

            console.log(`Validation passed: ${validationData.validationCount} records checked.`);
        }
    } catch (validationErr) {
        // If validation procedure doesn't exist or fails, log and continue
        console.log("Validation step skipped or failed: " + validationErr.message);
    }

    // If validation passes or is skipped, proceed with the regular procedure sequence
    var procedures = [
        { name: `upload_db.public.move_data_to_final_table_dynamic_generic`, args: [PLATFORM, TYPE, FILENAME] },
        { name: "upload_db.public.set_phase_generic", args: [PLATFORM, 3, FILENAME] },
    ];

    for (var i = 0; i < procedures.length; i++) {
        var proc = procedures[i];
        var result = executeStoredProcedure(proc.name, proc.args);
        if (!result.success) {
            // If a procedure fails, log the failure and halt further execution
            console.error(result.message);
            return result.message;
        }
    }

    console.log("All procedures executed successfully.");
    return "All procedures executed successfully.";
$$;

GRANT USAGE ON PROCEDURE upload_db.public.handle_final_insert_dynamic_generic(STRING, STRING, STRING) TO ROLE web_app;

-- ==============================================================================
-- DEPLOYMENT COMPLETE
-- ==============================================================================
-- Phase 3 generic procedures have been created.
--
-- Next steps:
-- 1. Update Lambda to call handle_final_insert_dynamic_generic instead of handle_final_insert_dynamic
-- 2. Deploy Lambda changes
-- 3. Test Phase 3 with an upload
-- ==============================================================================
