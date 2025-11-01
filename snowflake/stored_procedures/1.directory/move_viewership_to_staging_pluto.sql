GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING_PLUTO(string) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING_PLUTO(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Helper function to execute a stored procedure and handle errors, now with arguments
        function executeStoredProcedure(procName, args) {
            try {
                var sqlCommand = `CALL ` + procName + (args.length > 0 ? `(${args.map(arg => `'${arg}'`).join(', ')})` : '()');  
                snowflake.execute({sqlText: sqlCommand});
                return { success: true, message: procName + " executed successfully." };
            } catch (err) {
                // Logging the error to the console
                console.error("Error executing " + procName + ": " + err.message);
                // Returning error details for further handling
                return { success: false, message: "Error executing " + procName + ": " + err.message + "sql command: " + sqlCommand };
            }
        }

        let fileName = FILENAME.toUpperCase();

        var procedures = [
            { name: "upload_db.public.initial_sanitization_staging_pluto", args: [fileName] },
            { name: "upload_db.public.move_sanitized_data_to_staging", args: ["Pluto", fileName] },
            { name: "upload_db.public.set_phase_dynamic", args: ["Pluto", 0] }
        ];

        for (var i = 0; i < procedures.length; i++) {
            var proc = procedures[i];
            var result = executeStoredProcedure(proc.name, proc.args);
            if (!result.success) {
                // If a procedure fails, log the failure and halt further execution
                console.error(result.message);
                // Optionally, return or throw an error to indicate failure
                throw result.message;
            }
        }

        // If all procedures are executed successfully
        console.log("All procedures executed successfully.");
        return "All procedures executed successfully.";
    } catch (error) {
        const errorMessage = "Error: " + error;
        // Log the error message to an error log table
        snowflake.execute({sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)", binds: [errorMessage, 'move_viewership_to_staging_pluto', 'Pluto']});
        return error;
    
    }
$$;