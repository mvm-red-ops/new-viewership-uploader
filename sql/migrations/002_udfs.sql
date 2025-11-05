-- ==============================================================================
-- MIGRATION 002: User-Defined Functions (UDFs)
-- ==============================================================================
-- Purpose: Create all UDFs needed by stored procedures
-- Dependencies: 001_schema_tables.sql
-- Idempotent: Yes (uses CREATE OR REPLACE)
-- ==============================================================================

-- ==============================================================================
-- EXTRACT_PRIMARY_TITLE
-- ==============================================================================
-- Extracts the primary title from a comma-separated list of titles with
-- language suffixes (e.g., "Title-en, Title-es, Title-fr")
--
-- Logic:
--   1. Prefer English title (suffix: -en)
--   2. If no English, take first title and strip language suffix
--   3. Return null if no titles
-- ==============================================================================

CREATE OR REPLACE FUNCTION {{UPLOAD_DB}}.PUBLIC.EXTRACT_PRIMARY_TITLE(TITLES VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS '
if (!TITLES) return null;

// Split by comma
const titleList = TITLES.split(",").map(t => t.trim());

// Look for English title first (with -en suffix)
const englishTitle = titleList.find(t => t.endsWith("-en"));
if (englishTitle) {
    // Remove the -en suffix
    return englishTitle.substring(0, englishTitle.length - 3);
}

// If no English title, take the first one and remove language suffix
if (titleList.length > 0) {
    const firstTitle = titleList[0];
    // Check if it has a language suffix (-xx)
    const langSuffixMatch = firstTitle.match(/^(.+)-[a-z]{2}$/);
    if (langSuffixMatch) {
        return langSuffixMatch[1]; // Return the part before the language suffix
    }
    return firstTitle; // Return as is if no language suffix
}

return null;
';

-- ==============================================================================
-- Add more UDFs here as needed
-- ==============================================================================
