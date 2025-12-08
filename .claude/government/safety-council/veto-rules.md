# Safety Council Veto Rules

## Purpose

The Safety Council has absolute veto power over changes that would violate critical red lines. These rules exist to prevent catastrophic mistakes that could break production systems.

## Critical Red Lines

### 1. MOVE_STREAMLIT_DATA_TO_STAGING Filter Protection

**RULE:** The `processed IS NULL OR processed = FALSE` filter in MOVE_STREAMLIT_DATA_TO_STAGING (line 69) SHALL NEVER be removed or modified.

**Rationale:** This filter is essential for the data pipeline. Removing it would cause:
- Duplicate data copying
- Reprocessing of already-processed records
- Data integrity issues

**Violation Example (December 2, 2025):**
- President attempted to remove this filter
- User explicitly rejected: "dont chang or remove the filter in a crucial stored proc"
- Filter was not deployed (caught in time)

**Override Procedure:** NONE - This is an absolute red line

**File:** `snowflake/stored_procedures/generic/move_streamlit_data_to_staging.sql`

**Protected Code:**
```javascript
sql_command = `
    INSERT INTO test_staging.public.platform_viewership (${columns.join(", ")})
    SELECT ${columns.join(", ")}
    FROM upload_db.public.platform_viewership
    WHERE UPPER(platform) = '${upperPlatform}'
      AND LOWER(filename) = '${lowerFilename}'
      AND (processed IS NULL OR processed = FALSE)  // ← PROTECTED
      AND (phase IS NULL OR phase = '');
`;
```

---

### 2. UNION Fallback Pattern Protection

**RULE:** Bucket procedures with UNION fallback patterns SHALL NOT have the UNION section removed.

**Rationale:** UNION fallbacks provide resilience by trying strict matching first, then looser matching. Removing them causes matching failures.

**Violation Example (December 2, 2025):**
- PROCESS_VIEWERSHIP_REF_ID_SERIES_GENERIC was deployed WITHOUT the UNION fallback
- Result: 321 records matched to REF_ID_SERIES bucket, but 0 rows updated
- Required redeployment with complete UNION pattern

**Affected Procedures:**
- `ref_id_series.sql` (lines 92-127)
- `full_data.sql` (verify UNION exists)
- Other bucket procedures (to be catalogued in Constitutional Convention)

**Protected Pattern:**
```sql
UPDATE ... FROM (
    -- First attempt: Strict matching
    SELECT ...
    WHERE ... AND (title_match_condition)

    UNION  // ← PROTECTED

    -- Fallback: Looser matching
    SELECT ...
    WHERE ...
      AND NOT EXISTS (already matched check)
) q
```

**Override Procedure:** Requires:
1. Snowflake Governor approval
2. Testing Governor verification that alternative matching works
3. Full test results showing 100% match rate
4. Explicit Citizen approval

---

### 3. Template Variable Protection

**RULE:** Database name template variables SHALL NOT be replaced with hardcoded values.

**Template Variables:**
- `{{UPLOAD_DB}}` → `UPLOAD_DB`
- `{{STAGING_DB}}` → `TEST_STAGING` (staging) or `STAGING` (prod)
- `{{ASSETS_DB}}` → `STAGING_ASSETS`
- `{{METADATA_DB}}` → `METADATA_MASTER_CLEANED_STAGING`
- `{{EPISODE_DETAILS_TABLE}}` → `EPISODE_DETAILS_TEST_STAGING` (staging) or `EPISODE_DETAILS` (prod)

**Rationale:** Template variables enable:
- Staging vs production separation
- Environment portability
- Deployment flexibility

**Override Procedure:** Requires:
1. Snowflake Governor + Lambda Governor approval
2. Documentation of why hardcoding is necessary
3. Explicit Citizen approval

---

### 4. Phase Transition Logic Protection

**RULE:** The phase progression system (0 → 1 → 2 → 3) SHALL NOT be modified without full pipeline testing.

**Phase Definitions:**
- Phase 0: Data in UPLOAD_DB, processed=FALSE
- Phase 1: Data copied to STAGING, normalized
- Phase 2: Content matching and bucket processing
- Phase 3: Validation and final insert to EPISODE_DETAILS

**Rationale:** Phase transitions control the entire data flow. Breaking this breaks the entire pipeline.

**Override Procedure:** Requires:
1. Snowflake Governor approval
2. Testing Governor full pipeline verification
3. Lambda Governor approval (affects orchestration)
4. Documentation of all downstream effects
5. Explicit Citizen approval

---

### 5. Grant Permissions Protection

**RULE:** GRANT statements at the end of stored procedures SHALL NOT be removed.

**Standard Grant:**
```sql
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.PROCEDURE_NAME(...) TO ROLE WEB_APP;
```

**Rationale:** Without grants, the Streamlit app (running as WEB_APP role) cannot execute procedures.

**Override Procedure:** Only if replacing with different grant. Requires:
1. Snowflake Governor verification
2. Streamlit Governor confirmation app can still execute
3. Testing in staging environment first

---

## Pending Red Lines

The following areas need red lines established in Constitutional Convention:

### Snowflake Domain
- [ ] Bucket matching criteria modifications
- [ ] Date calculation logic changes
- [ ] Deal parent matching logic
- [ ] Internal series matching logic
- [ ] Viewership metrics calculations
- [ ] Error logging requirements
- [ ] TEMPORARY table naming conventions

### Lambda Domain
- [ ] Orchestration sequence modifications
- [ ] Retry logic changes
- [ ] Error handling patterns

### Streamlit Domain
- [ ] Data transformation auto-detection
- [ ] Upload validation rules
- [ ] processed flag setting

### Testing Domain
- [ ] Required tests before deployment
- [ ] Staging validation requirements
- [ ] Rollback trigger criteria

---

## Veto Process

### When a Change is Proposed

1. **Automatic Check:** Safety Council reviews proposal against all red lines
2. **If Violation Detected:**
   - IMMEDIATE VETO
   - Explanation of which red line was violated
   - Reference to rationale and past incidents
   - Suggestion of alternative approach if available
3. **If No Violation:** Change proceeds through normal governance channels

### Veto Communication

**Format:**
```
🛑 SAFETY COUNCIL VETO

Red Line Violated: [Name of red line]
Proposed Change: [Description]
Violation: [What specifically violates the red line]
Rationale: [Why this red line exists]
Past Incident: [Reference if applicable]
Alternative: [Suggestion if available]

This veto can only be overridden by explicit Citizen approval after full risk explanation.
```

### Veto Override

Only the Citizen (User) can override a Safety Council veto, and ONLY after:
1. Full explanation of risks
2. Documentation of why the change is necessary
3. Rollback plan in place
4. Explicit acknowledgment of risks

---

## Maintenance

This document shall be updated:
- After each incident or near-miss
- During Constitutional Conventions
- When new patterns emerge as critical
- When Governors identify new red lines

**Last Updated:** December 2, 2025 (Initial draft)

**Next Review:** Constitutional Convention (pending)
