--create upload_db.public.normalize_data_in_staging_pluto
--set deal_parent = 29 
--set channel id set_channel_dynamic_platform based on channel column
--set territory based on territory column and joining on territory dictionary like in upload_db.public.set_territory_wurl()


-- Pluto Normalize data
DROP PROCEDURE IF EXISTS upload_db.public.normalize_data_in_staging_pluto();
GRANT USAGE ON PROCEDURE upload_db.public.normalize_data_in_staging_pluto(STRING) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE upload_db.public.normalize_data_in_staging_pluto(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Helper function to call stored procedures and handle errors, now also accepts arguments
    function callProcedure(procName, args) {
        try {
            var sql_command = `CALL ` + procName + (args ? `(${args.map(arg => `'${arg}'`).join(',')})` : '()') + `;`;
            snowflake.execute({sqlText: sql_command});
            return "Succeeded";
        } catch (err) {
            // Rethrow the error to be caught by the outer try-catch
            throw "Procedure " + procName + " failed: " + err;
        }
    }

    const lowerFilename = FILENAME.toLowerCase();
    try {
        // List of procedures to call in sequence, with arguments where needed
        var procedures = [
            { name: "upload_db.public.set_territory_pluto", args: [] },
            { name: "upload_db.public.set_channel_deal_parent_pluto", args: [] },
            { name: "upload_db.public.set_ymd_pluto", args: [] },
            { name: "upload_db.public.set_quarter_pluto", args: [] },
            { name: "upload_db.public.set_hours_pluto", args: [] },
            { name: "upload_db.public.set_phase_dynamic", args: ["Pluto", 1] } 
        ];

        for (var i = 0; i < procedures.length; i++) {
            var proc = procedures[i];
            var result = callProcedure(proc.name, proc.args);
            if (result !== "Succeeded") {
                return result;
            }
        }

        return "All procedures executed successfully.";
    } catch (err) {
        // Catch and return any errors thrown by the callProcedure function
        return err.toString();
    }
$$;