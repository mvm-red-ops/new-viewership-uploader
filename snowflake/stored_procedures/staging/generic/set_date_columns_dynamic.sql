CREATE OR REPLACE PROCEDURE "SET_DATE_COLUMNS_DYNAMIC"("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    // Function to execute a given SQL command and return a status message
    function executeSqlCommand(sql_command) {
        try {
            snowflake.execute({sqlText: sql_command});
            return "Succeeded";
        } catch (err) {
            return "Failed: " + err;
        }
    }
    const platform = PLATFORM;
    const filename = FILENAME;

    // First, check if there is a date column with VALID data
    // Exclude placeholder dates like 1970-08-23 (Unix epoch) which indicate no real date
    var checkDateQuery = `
        SELECT COUNT(*) as cnt
        FROM TEST_STAGING.public.platform_viewership
        WHERE platform = ''${platform}''
          AND filename = ''${filename}''
          AND date IS NOT NULL
          AND TRIM(date) != ''''
          AND YEAR(TRY_CAST(date AS DATE)) >= 2000
    `;

    var dateCheckResult = snowflake.execute({sqlText: checkDateQuery});
    var hasDateColumn = false;
    if (dateCheckResult.next()) {
        hasDateColumn = dateCheckResult.getColumnValue(''CNT'') > 0;
    }

    var updateCommands = [];

    if (!hasDateColumn) {
        // No date column - check if we have month and year to construct year_month_day
        console.log(''No date column found - checking for month/year columns'');

        // Convert month name to numeric format
        updateCommands.push(`
            UPDATE TEST_STAGING.public.platform_viewership
            SET month = CASE LOWER(month)
                WHEN ''january'' THEN ''1''
                WHEN ''february'' THEN ''2''
                WHEN ''march'' THEN ''3''
                WHEN ''april'' THEN ''4''
                WHEN ''may'' THEN ''5''
                WHEN ''june'' THEN ''6''
                WHEN ''july'' THEN ''7''
                WHEN ''august'' THEN ''8''
                WHEN ''september'' THEN ''9''
                WHEN ''october'' THEN ''10''
                WHEN ''november'' THEN ''11''
                WHEN ''december'' THEN ''12''
                ELSE month  -- If month is already numeric
            END
            WHERE platform = ''${platform}''
              AND filename = ''${filename}''
              AND processed IS NULL
              AND month IS NOT NULL
        `);

        // Set year_month_day from year + month (with day = 01)
        updateCommands.push(`
            UPDATE TEST_STAGING.public.platform_viewership
            SET year_month_day = CONCAT(
                LPAD(CAST(year AS VARCHAR), 4, ''0''),
                LPAD(month, 2, ''0''),
                ''01''
            )
            WHERE platform = ''${platform}''
              AND filename = ''${filename}''
              AND processed IS NULL
              AND month IS NOT NULL
              AND year IS NOT NULL
              AND (year_month_day IS NULL OR year_month_day = '''')
        `);

        // Set week from year_month_day
        updateCommands.push(`
            UPDATE TEST_STAGING.public.platform_viewership
            SET week = UPLOAD_DB.public.get_week_start(TO_DATE(year_month_day, ''YYYYMMDD''))
            WHERE platform = ''${platform}''
              AND filename = ''${filename}''
              AND processed IS NULL
              AND year_month_day IS NOT NULL
        `);

        // Set day from year_month_day
        updateCommands.push(`
            UPDATE TEST_STAGING.public.platform_viewership
            SET day = UPLOAD_DB.public.get_day_from_mm_dd_yyyy(TO_DATE(year_month_day, ''YYYYMMDD''))
            WHERE platform = ''${platform}''
              AND filename = ''${filename}''
              AND processed IS NULL
              AND year_month_day IS NOT NULL
        `);

        // Set quarter based on month
        updateCommands.push(`
            UPDATE TEST_STAGING.public.platform_viewership
            SET quarter = CASE
                WHEN CAST(month AS INT) IN (1, 2, 3) THEN ''q1''
                WHEN CAST(month AS INT) IN (4, 5, 6) THEN ''q2''
                WHEN CAST(month AS INT) IN (7, 8, 9) THEN ''q3''
                WHEN CAST(month AS INT) IN (10, 11, 12) THEN ''q4''
            END
            WHERE platform = ''${platform}''
              AND filename = ''${filename}''
              AND processed IS NULL
              AND month IS NOT NULL
        `);

        // Year should already be set from the upload, so skip that

    } else {
        // Has date column - use original logic
        console.log(''Date column found - using standard date processing'');

        // Update statement for getting the full date
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET full_date = UPLOAD_DB.public.get_full_date(date)
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the week start date
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET week = UPLOAD_DB.public.get_week_start(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the quarter
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET quarter = UPLOAD_DB.public.get_quarter_from_mm_dd_yyyy(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the year
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET year = UPLOAD_DB.public.get_year_from_mm_dd_yyyy(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the month
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET month = UPLOAD_DB.public.get_month_from_mm_dd_yyyy(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the first of the month
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET year_month_day = UPLOAD_DB.public.get_first_of_month_from_mm_dd_yyyy(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);

        // Update statement for setting the day
        updateCommands.push(`UPDATE TEST_STAGING.public.platform_viewership
         SET day = UPLOAD_DB.public.get_day_from_mm_dd_yyyy(UPLOAD_DB.public.get_full_date(date))
         WHERE platform = ''${platform}''
           AND filename = ''${filename}''
           AND processed IS NULL`);
    }
    var resultMessage = "";
    for (var i = 0; i < updateCommands.length; i++) {
        resultMessage = executeSqlCommand(updateCommands[i]);
        if (resultMessage !== "Succeeded") {
            return "Error executing update command: " + resultMessage;
        }
    }
    return "All date columns set successfully for " + platform + " - " + filename;
';
