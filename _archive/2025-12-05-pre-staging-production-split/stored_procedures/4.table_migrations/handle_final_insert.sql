GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.HANDLE_FINAL_INSERT_DYNAMIC(STRING, STRING, STRING) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.HANDLE_FINAL_INSERT_DYNAMIC("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR DEFAULT null)
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
    
    // First, run the validation procedure
    var validationResult = executeStoredProcedure("upload_db.public.validate_viewership_for_insert", [PLATFORM, FILENAME]);
    
    if (!validationResult.success) {
        return "Failed to execute validation: " + validationResult.message;
    }
    
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
    
    // If validation passes, proceed with the regular procedure sequence
    var procedures = [
        { name: `upload_db.public.move_data_to_final_table_dynamic`, args: [PLATFORM, TYPE, FILENAME] },
        { name: "upload_db.public.set_phase_dynamic", args: [PLATFORM, 3] }, 
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