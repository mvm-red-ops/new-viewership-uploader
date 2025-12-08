GRANT USAGE ON PROCEDURE upload_db.public.normalize_data_in_staging_amagi(STRING) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE upload_db.public.normalize_data_in_staging_amagi(filename STRING)
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
            { name: "upload_db.public.set_channel_dynamic_platform_amagi", args: ['Amagi'] },
            { name: "upload_db.public.set_deal_parent", args: ["Amagi", '["platform","domain","platform_channel_name"]'] },
            { name: "upload_db.public.set_viewership_partner", args: ["Amagi"] },
            { name: "upload_db.public.set_territory_amagi", args: [] },
            { name: "upload_db.public.set_date_columns_dynamic", args: ['Amagi'] },
            { name: "upload_db.public.set_hov_mov_dynamic", args: ['Amagi'] },
            { name: "upload_db.public.validate_nulls", args: ['Amagi'] },
            { name: "upload_db.public.set_phase_dynamic", args: ['Amagi', 1] }
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
