CREATE OR REPLACE PROCEDURE upload_db.public.move_viewership_to_staging_amagi()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Helper function to execute a stored procedure and handle errors, now with arguments
    function executeStoredProcedure(procName, args) {
        try {
            var sqlCommand = `CALL ` + procName + (args.length > 0 ? `(${args.join(',')})` : '()') + `;`;
            snowflake.execute({sqlText: sqlCommand});
            return { success: true, message: procName + " executed successfully." };
        } catch (err) {
            // Logging the error to the console
            console.error("Error executing " + procName + ": " + err.message);
            // Returning error details for further handling
            return { success: false, message: "Error executing " + procName + ": " + err.message };
        }
    }

    var procedures = [
        { name: "upload_db.public.initial_sanitization_staging_amagi", args: [] },
        { name: "upload_db.public.move_sanitized_data_to_staging", args: ["'Amagi'"] },
        { name: "upload_db.public.set_phase_dynamic", args: ["Amagi", 0] } 
    ];

    for (var i = 0; i < procedures.length; i++) {
        var proc = procedures[i];
        var result = executeStoredProcedure(proc.name, proc.args);
        if (!result.success) {
            // If a procedure fails, log the failure and halt further execution
            console.error(result.message);
            // Optionally, return or throw an error to indicate failure
            return result.message;
        }
    }

    // If all procedures are executed successfully
    console.log("All procedures executed successfully.");
    return "All procedures executed successfully.";
$$;
