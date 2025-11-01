CREATE OR REPLACE FUNCTION upload_db.public.extract_primary_title(titles STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
if (!TITLES) return null;

// Split by comma
const titleList = TITLES.split(',').map(t => t.trim());

// Look for English title first (with -en suffix)
const englishTitle = titleList.find(t => t.endsWith('-en'));
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
$$;