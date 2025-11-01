GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING_YOUTUBE(string) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING_YOUTUBE("FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Helper function to execute a stored procedure and handle errors, now with arguments
    function executeStoredProcedure(procName, args) {
        try {
            var sqlCommand = `CALL ` + procName + (args.length > 0 ? `(${args.map(arg => `'${arg}'`).join(', ')})` : '()');  
            snowflake.execute({sqlText: sqlCommand});
            return "Succeeded";
        } catch (err) {
            // Logging the error to the console
            console.error("Error executing " + procName + ": " + err.message);
            // Returning error details for further handling
            return { success: false, message: "Error executing " + procName + ": " + err.message + "sql command: " + sqlCommand };
        }
    }

    const lowerFilename = FILENAME.toLowerCase();
    const procedures = [
            { name: "UPLOAD_DB.public.set_year_month_day_with_year_month_dynamic", args: ["Youtube", lowerFilename] },
            { name: "UPLOAD_DB.public.set_hov_mov_dynamic", args: ["Youtube"] },
            { name: "TEST_STAGING.PUBLIC.SET_QUARTER_DYNAMIC", args: ["Youtube"] },
            { name: "UPLOAD_DB.public.set_phase_dynamic", args: ["Youtube", 1] }
        ];

    try {
        for (var i = 0; i < procedures.length; i++) {
            var proc = procedures[i];
            var result = executeStoredProcedure(proc.name, proc.args);
            if (result !== "Succeeded") {
                return result; // This will be the error message from the failed procedure
            }
        }

        return "All procedures executed successfully.";
    } catch (err) {
        const errorMessage = "Error: " + err ;
        // Log the error message to an error log table
        snowflake.execute({sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)", binds: [errorMessage, "NORMALIZE_DATA_IN_STAGING_YOUTUBE", "Youtube"]});
        return err;
    }
$$;