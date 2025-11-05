-- ==============================================================================
-- PROCEDURE: set_deal_parent_normalized_generic
-- ==============================================================================
-- Fallback procedure to set deal_parent using NORMALIZED fields
-- Runs AFTER primary matching (which uses RAW platform_* fields)
-- Matches on: normalized partner, channel, territory
-- Only processes records where deal_parent IS NULL
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_deal_parent_normalized_generic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    // Fallback: Match using NORMALIZED fields (partner, channel, territory)
    // for records that didn't match on platform_* fields
    const updateSql = `
        UPDATE {{STAGING_DB}}.public.platform_viewership v
        SET
            deal_parent = ad.deal_parent,
            partner = ad.internal_partner,
            channel = ad.internal_channel,
            territory = ad.internal_territory,
            channel_id = ad.internal_channel_id,
            territory_id = ad.internal_territory_id
        FROM dictionary.public.active_deals ad
        WHERE ad.platform = '${platform}'
          AND UPPER(ad.domain) = UPPER(v.domain)
          AND ad.active = true
          AND (v.partner IS NULL OR UPPER(v.partner) = UPPER(ad.internal_partner))
          AND (v.channel IS NULL OR UPPER(v.channel) = UPPER(ad.internal_channel))
          AND (v.territory IS NULL OR UPPER(v.territory) = UPPER(ad.internal_territory))
          AND v.filename = '${filename}'
          AND v.platform = '${platform}'
          AND v.deal_parent IS NULL
          AND v.processed IS NULL
    `;

    console.log("Fallback: Setting deal_parent using normalized fields...");
    const stmt = snowflake.createStatement({sqlText: updateSql});
    stmt.execute();
    const rowsAffected = stmt.getNumRowsAffected();
    console.log(`Fallback matched ${rowsAffected} records using normalized fields`);

    // Count how many still don't have deal_parent
    const countUnmatchedSql = `
        SELECT COUNT(*) as unmatched
        FROM {{STAGING_DB}}.public.platform_viewership
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND deal_parent IS NULL
          AND processed IS NULL
    `;
    const countStmt = snowflake.createStatement({sqlText: countUnmatchedSql});
    const countResult = countStmt.execute();
    countResult.next();
    const unmatchedCount = countResult.getColumnValue(1);

    const resultMsg = `Fallback set deal_parent for ${rowsAffected} records using normalized fields. ${unmatchedCount} records remain without deal_parent.`;

    // Log to error table
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [resultMsg, 'set_deal_parent_normalized_generic', PLATFORM]
    });

    return resultMsg;

} catch (error) {
    const errorMessage = "Error in set_deal_parent_normalized_generic: " + error.message;
    console.error(errorMessage);

    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'set_deal_parent_normalized_generic', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_deal_parent_normalized_generic(VARCHAR, VARCHAR) TO ROLE web_app;
