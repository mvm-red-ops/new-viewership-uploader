# SET_DATE_COLUMNS_DYNAMIC Delegate

## Specialty

Date field calculations and normalization (year, month, week, quarter, day, year_month_day)

## Procedure Details

**File:** `snowflake/stored_procedures/generic/set_date_columns_dynamic.sql`

**Deployment Script:** `deploy_date_proc.py`

**Database:** UPLOAD_DB.PUBLIC

**Procedure Name:** `SET_DATE_COLUMNS_DYNAMIC(platform VARCHAR, filename VARCHAR)`

## Purpose

Sets date-related columns based on either:
1. A populated `date` field (daily data), OR
2. Populated `month` and `year` fields (monthly/quarterly data)

## Critical Code Sections

### Date Column Check (Lines ~45-60)

**IMPORTANT LOGIC - Handle NULL dates**

```javascript
// Check if there is a date column with data
var checkDateQuery = `
    SELECT COUNT(*) as cnt
    FROM {{STAGING_DB}}.public.platform_viewership
    WHERE platform = '${platform}'
      AND filename = '${filename}'
      AND date IS NOT NULL
      AND TRIM(date) != ''
`;

var dateCheckResult = snowflake.execute({sqlText: checkDateQuery});
var hasDateColumn = false;
if (dateCheckResult.next()) {
    hasDateColumn = dateCheckResult.getColumnValue('CNT') > 0;
}
```

**Why Important:**
- Some platforms provide daily data (date populated)
- Some platforms provide monthly/quarterly data (date is NULL, month/year populated)
- Must handle both cases correctly

**Historical Issue (December 2, 2025):**
- This check was MISSING initially
- Result: Monthly/quarterly data had year/month/quarter set, then overwritten to NULL
- User observed: "i saw the year, month, quarter in the tes_stiang platfomr-viewership table for a second but then it all went balnk"

### Path 1: Data with Date Field (Lines ~65-100)

If `hasDateColumn = true`:

```javascript
// Derive year, month, week, etc. from date field
updateCommands.push(`
    UPDATE {{STAGING_DB}}.public.platform_viewership
    SET year = YEAR(TRY_TO_DATE(date)),
        month = MONTH(TRY_TO_DATE(date)),
        week = WEEK(TRY_TO_DATE(date)),
        quarter = CONCAT('q', QUARTER(TRY_TO_DATE(date))),
        day = DAY(TRY_TO_DATE(date)),
        year_month_day = TRY_TO_DATE(date)
    WHERE platform = '${platform}'
      AND filename = '${filename}'
      AND processed IS NULL
      AND date IS NOT NULL
`);
```

### Path 2: Data with Month/Year Only (Lines ~105-145)

**ADDED December 2, 2025**

If `hasDateColumn = false`:

```javascript
if (!hasDateColumn) {
    // No date column - check if we have month and year
    console.log('No date column found - checking for month/year columns');

    // Convert month name to numeric format
    updateCommands.push(`
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET month = CASE LOWER(month)
            WHEN 'january' THEN '1'
            WHEN 'february' THEN '2'
            WHEN 'march' THEN '3'
            WHEN 'april' THEN '4'
            WHEN 'may' THEN '5'
            WHEN 'june' THEN '6'
            WHEN 'july' THEN '7'
            WHEN 'august' THEN '8'
            WHEN 'september' THEN '9'
            WHEN 'october' THEN '10'
            WHEN 'november' THEN '11'
            WHEN 'december' THEN '12'
            ELSE month
        END
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND processed IS NULL
          AND month IS NOT NULL
    `);

    // Set quarter based on month
    updateCommands.push(`
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET quarter = CASE
            WHEN CAST(month AS INT) IN (1, 2, 3) THEN 'q1'
            WHEN CAST(month AS INT) IN (4, 5, 6) THEN 'q2'
            WHEN CAST(month AS INT) IN (7, 8, 9) THEN 'q3'
            WHEN CAST(month AS INT) IN (10, 11, 12) THEN 'q4'
        END
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND processed IS NULL
          AND month IS NOT NULL
    `);

    // Set year_month_day using year and month
    updateCommands.push(`
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET year_month_day = year || '-' || LPAD(month, 2, '0') || '-01'
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND processed IS NULL
          AND year IS NOT NULL
          AND month IS NOT NULL
    `);
}
```

**Why Important:**
- Handles platforms like Tubi VOD that provide monthly aggregated data
- Converts month names ("July") to numeric format ("7")
- Calculates quarter from month
- Sets year_month_day to first day of month (e.g., "2025-07-01")

## Dependencies

### Upstream (What Affects This)
- **MOVE_STREAMLIT_DATA_TO_STAGING Delegate** - Must copy data first
- **Streamlit Transformation Delegate** - May set initial date/month/year values

### Downstream (What This Affects)
- **ANALYZE_AND_PROCESS Delegate** - Uses date fields for filtering
- **HANDLE_FINAL_INSERT Delegate** - Inserts date fields to EPISODE_DETAILS
- All reporting that depends on year/month/quarter fields

## Common Issues

### Issue: Date fields showing NULL for monthly/quarterly data

**Symptoms:**
- Data has year='2025', month='July', quarter='Q3' initially
- After normalization, all date fields are NULL

**Root Cause:** Missing NULL date check (before December 2, 2025 fix)

**Solution:** Deploy updated procedure with NULL date handling (FIXED)

**Verification:**
```sql
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN year IS NOT NULL THEN 1 ELSE 0 END) as has_year,
    SUM(CASE WHEN month IS NOT NULL THEN 1 ELSE 0 END) as has_month,
    SUM(CASE WHEN quarter IS NOT NULL THEN 1 ELSE 0 END) as has_quarter
FROM TEST_STAGING.PUBLIC.platform_viewership
WHERE platform = 'Tubi' AND filename = 'tubi_vod_july.csv';
```

### Issue: Month names not converted to numeric

**Symptoms:**
- month field contains "July" instead of "7"
- Quarter calculation fails

**Root Cause:** Month name to number conversion not applied

**Solution:** Verify CASE statement for month conversion is present (line ~105)

## Testing Checklist

Before deploying changes to this procedure:

- [ ] Test with daily data (date field populated)
  - [ ] Verify year/month/quarter derived from date
  - [ ] Verify year_month_day matches date
- [ ] Test with monthly data (month/year populated, date is NULL)
  - [ ] Verify month name converts to number
  - [ ] Verify quarter calculated from month
  - [ ] Verify year_month_day set to first of month
- [ ] Test with quarterly data (quarter/year populated)
- [ ] Check error logs for any issues
- [ ] Verify no records have NULL date fields after processing

## Escalation Triggers

**Escalate to Snowflake Governor if:**
- Date format requirements change
- New date field types need support (e.g., week-based data)
- Precedence rules need clarification (what if both date AND month/year exist?)

**Escalate to Testing Governor if:**
- New test cases needed for different date formats
- Regression detected in date calculations

**Escalate to President if:**
- Streamlit is providing dates in unexpected formats
- Cross-domain issue with how dates are used downstream

## Questions for Constitutional Convention

- [ ] What date formats must be supported?
- [ ] What is the precedence if both `date` and `month/year` are populated?
- [ ] How should weekly data be handled?
- [ ] How should quarterly-only data (no month) be handled?
- [ ] What should happen if date parsing fails?
- [ ] Should we support fiscal years vs. calendar years?

## Knowledge Base

**Last Updated:** December 2, 2025

**Recent Changes:**
- ✅ Added NULL date check and monthly/quarterly data handling
- ✅ Added month name to numeric conversion
- ✅ Added quarter calculation from month
- ✅ Added year_month_day setting for monthly data

**Test Results (Tubi VOD July):**
- Before fix: 0/321 records had year/month/quarter
- After fix: 321/321 records have year/month/quarter

**Deployment Status:**
- ✅ Deployed to STAGING: December 2, 2025
- ✅ Deployed to PROD: December 2, 2025
