-- SET_DEAL_PARENT_GENERIC (Production)
-- Best-specificity match: NULL on either side acts as wildcard.
-- More specific active_deals rows (non-NULL partner/channel/territory) win over catch-alls.
-- Scoring: partner=4, channel=2, territory=1 → max specificity=7 beats NULL/NULL/NULL=0.

CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_DEAL_PARENT_GENERIC("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    const updateSql = `
        UPDATE NOSEY_PROD.public.platform_viewership v
        SET
            deal_parent  = best_match.deal_parent,
            partner      = best_match.internal_partner,
            channel      = best_match.internal_channel,
            territory    = best_match.internal_territory,
            channel_id   = best_match.internal_channel_id,
            territory_id = best_match.internal_territory_id
        FROM (
            SELECT
                v2.id,
                ad.deal_parent,
                ad.internal_partner,
                ad.internal_channel,
                ad.internal_territory,
                ad.internal_channel_id,
                ad.internal_territory_id,
                -- Specificity score: more non-NULL active_deals fields that matched = higher score = wins
                (CASE WHEN ad.platform_partner_name IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN ad.platform_channel_name IS NOT NULL THEN 2 ELSE 0 END +
                 CASE WHEN ad.platform_territory    IS NOT NULL THEN 1 ELSE 0 END) AS specificity
            FROM NOSEY_PROD.public.platform_viewership v2
            JOIN dictionary.public.active_deals ad
              ON UPPER(ad.platform) = UPPER(v2.platform)
             AND UPPER(ad.domain)   = UPPER(v2.domain)
             AND ad.active = true
             -- NULL on either side = wildcard for that field
             AND (ad.platform_partner_name IS NULL
                  OR v2.platform_partner_name IS NULL
                  OR UPPER(v2.platform_partner_name) = UPPER(ad.platform_partner_name))
             AND (ad.platform_channel_name IS NULL
                  OR v2.platform_channel_name IS NULL
                  OR UPPER(v2.platform_channel_name) = UPPER(ad.platform_channel_name))
             AND (ad.platform_territory IS NULL
                  OR v2.platform_territory IS NULL
                  OR UPPER(v2.platform_territory) = UPPER(ad.platform_territory))
            WHERE v2.platform = ''${platform}''
              AND v2.filename = ''${filename}''
              AND v2.deal_parent IS NULL
              AND v2.processed IS NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY v2.id ORDER BY specificity DESC) = 1
        ) best_match
        WHERE v.id = best_match.id
    `;

    console.log("Setting deal_parent from active_deals (best-specificity match)...");
    const stmt = snowflake.createStatement({sqlText: updateSql});
    stmt.execute();
    const rowsAffected = stmt.getNumRowsAffected();
    console.log(`Updated deal_parent for ${rowsAffected} records`);

    snowflake.execute({
        sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Set deal_parent for ${rowsAffected} records`, ''set_deal_parent_generic'', PLATFORM]
    });

    const checkSql = `
        SELECT COUNT(*) as unmatched_count
        FROM NOSEY_PROD.public.platform_viewership
        WHERE platform = ''${platform}''
          AND filename = ''${filename}''
          AND deal_parent IS NULL
          AND processed IS NULL
    `;
    const checkResult = snowflake.createStatement({sqlText: checkSql}).execute();
    let unmatchedCount = 0;
    if (checkResult.next()) {
        unmatchedCount = checkResult.getColumnValue(1);
    }
    if (unmatchedCount > 0) {
        console.log(`Warning: ${unmatchedCount} records still have NULL deal_parent`);
        snowflake.execute({
            sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`WARNING: ${unmatchedCount} records have no matching active deals`, ''set_deal_parent_generic'', PLATFORM]
        });
    }

    return `Successfully set deal_parent for ${rowsAffected} records. ${unmatchedCount} records remain without deal_parent.`;
} catch (error) {
    const errorMessage = "Error in set_deal_parent_generic: " + error.message;
    console.error(errorMessage);
    snowflake.execute({
        sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, ''set_deal_parent_generic'', PLATFORM, error.message]
    });
    return errorMessage;
}
';

GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_DEAL_PARENT_GENERIC(VARCHAR, VARCHAR) TO ROLE web_app;
