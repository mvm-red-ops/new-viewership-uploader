-- Procedure to normalize territory abbreviations to full names
-- Uses dictionary.public.territories to map abbreviations like "us" to "United States"

CREATE OR REPLACE PROCEDURE {{STAGING_DB}}.public.normalize_territory_generic(
    input_platform VARCHAR,
    input_domain VARCHAR,
    input_filename VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var platform = INPUT_PLATFORM;
var domain = INPUT_DOMAIN;
var filename = INPUT_FILENAME;

try {
    // Log start
    snowflake.execute({
        sqlText: `
            INSERT INTO upload_db.public.error_log_table (platform, domain, log_time, log_message, procedure_name)
            VALUES (?, ?, CURRENT_TIMESTAMP(), ?, 'normalize_territory_generic')
        `,
        binds: [platform, domain, `Starting territory normalization for ${filename}`]
    });

    // Get all unique territory abbreviations that need normalization
    var getAbbrevsStmt = snowflake.execute({
        sqlText: `
            SELECT DISTINCT territory
            FROM {{STAGING_DB}}.public.platform_viewership
            WHERE platform = ?
              AND domain = ?
              AND LOWER(filename) = LOWER(?)
              AND territory IS NOT NULL
              AND processed IS NULL
        `,
        binds: [platform, domain, filename]
    });

    var territoryMap = {};
    var unmatchedCount = 0;

    // For each territory abbreviation, look up the canonical full name
    while (getAbbrevsStmt.next()) {
        var abbrev = getAbbrevsStmt.getColumnValue(1);

        // Look up this abbreviation in dictionary.territories
        var lookupStmt = snowflake.execute({
            sqlText: `
                SELECT DISTINCT ID
                FROM dictionary.public.territories
                WHERE UPPER(NAME) = UPPER(?)
                  AND ACTIVE = TRUE
                LIMIT 1
            `,
            binds: [abbrev]
        });

        if (lookupStmt.next()) {
            var territoryId = lookupStmt.getColumnValue(1);

            // Get the canonical full name for this territory ID (longest name)
            var canonicalStmt = snowflake.execute({
                sqlText: `
                    SELECT NAME
                    FROM dictionary.public.territories
                    WHERE ID = ?
                      AND ACTIVE = TRUE
                    ORDER BY LENGTH(NAME) DESC, NAME
                    LIMIT 1
                `,
                binds: [territoryId]
            });

            if (canonicalStmt.next()) {
                var canonicalName = canonicalStmt.getColumnValue(1);
                territoryMap[abbrev] = canonicalName;
            }
        } else {
            // No match found - territory will stay as-is
            unmatchedCount++;
        }
    }

    // Update all territories with their canonical names
    var updateCount = 0;
    for (var abbrev in territoryMap) {
        var canonicalName = territoryMap[abbrev];

        var updateStmt = snowflake.execute({
            sqlText: `
                UPDATE {{STAGING_DB}}.public.platform_viewership
                SET territory = ?
                WHERE platform = ?
                  AND domain = ?
                  AND LOWER(filename) = LOWER(?)
                  AND territory = ?
                  AND processed IS NULL
            `,
            binds: [canonicalName, platform, domain, filename, abbrev]
        });

        updateCount += updateStmt.getNumRowsAffected();
    }

    var resultMsg = `Territory normalization complete: Updated ${updateCount} records`;
    if (unmatchedCount > 0) {
        resultMsg += `, ${unmatchedCount} unique territories had no match in dictionary.territories`;
    }

    // Log completion
    snowflake.execute({
        sqlText: `
            INSERT INTO upload_db.public.error_log_table (platform, domain, log_time, log_message, procedure_name)
            VALUES (?, ?, CURRENT_TIMESTAMP(), ?, 'normalize_territory_generic')
        `,
        binds: [platform, domain, resultMsg]
    });

    return resultMsg;

} catch (err) {
    var errorMsg = `ERROR in normalize_territory_generic: ${err.message}`;

    snowflake.execute({
        sqlText: `
            INSERT INTO upload_db.public.error_log_table (platform, domain, log_time, log_message, procedure_name)
            VALUES (?, ?, CURRENT_TIMESTAMP(), ?, 'normalize_territory_generic')
        `,
        binds: [platform, domain, errorMsg]
    });

    return errorMsg;
}
$$;
