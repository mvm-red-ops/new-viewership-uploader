GRANT USAGE ON PROCEDURE upload_db.public.normalize_data_in_staging_wurl(STRING) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE upload_db.public.normalize_data_in_staging_wurl(filename STRING)
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
        var procedures = [
            { name: "upload_db.public.set_channel_wurl", args: [] },
            { name: "upload_db.public.set_deal_parent", args: ["Wurl", '["platform_partner_name", "platform_channel_name"]'] },
            { name: "upload_db.public.set_platform_wurl", args: [] },
            { name: "upload_db.public.set_viewership_partner", args: ["Wurl"] },
            { name: "upload_db.public.set_territory_wurl", args: [] },
            { name: "upload_db.public.set_date_columns_dynamic", args: ["Wurl"] },
            { name: "UPLOAD_DB.public.set_hov_mov_dynamic", args: ["Wurl"] },
            { name: "upload_db.public.validate_nulls", args: [] },
            { name: "upload_db.public.set_phase_dynamic", args: ["Wurl", 1] } 
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
