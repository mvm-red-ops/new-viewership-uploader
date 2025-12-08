# Constitutional Convention Agenda

## Purpose

This Constitutional Convention will establish the foundational rules, red lines, and procedures for the viewership uploader government system.

## Format

The President (Claude) will facilitate sessions with each Governor. Each Governor will ask the Citizen (User) detailed questions about their domain. The Citizen's answers will be codified into the Constitution and Governor-specific rules.

## Session Structure

### Session 1: Snowflake Governor (HIGHEST PRIORITY)

**Duration:** ~30-45 minutes

**Topics:**
1. Stored procedure red lines and no-go zones
2. Bucket matching rules and modification criteria
3. Date handling requirements
4. Template variables and environment differences
5. Phase transition logic rules
6. Testing requirements before deployment
7. Rollback procedures

**Expected Outcomes:**
- Codified red lines in Safety Council veto-rules.md
- Snowflake-specific rules in governors/snowflake/constitution.md
- Complete delegate knowledge bases for all 15+ procedures
- Testing checklist for stored procedure changes

---

### Session 2: Testing Governor (HIGH PRIORITY)

**Duration:** ~20-30 minutes

**Topics:**
1. Required tests before any deployment
2. Staging vs. prod deployment rules
3. Verification checklist after deployment
4. Rollback triggers and procedures
5. Test data management
6. Regression test requirements

**Expected Outcomes:**
- Testing requirements codified in governors/testing/constitution.md
- Deployment checklist
- Rollback procedures documented
- Regression test suite planned

---

### Session 3: Lambda Governor

**Duration:** ~15-20 minutes

**Topics:**
1. Procedure execution sequence
2. Retry logic and error handling rules
3. Integration points with Snowflake/Streamlit
4. Timeout configurations
5. Logging requirements
6. Deployment procedures

**Expected Outcomes:**
- Lambda execution flow documented in governors/lambda/constitution.md
- Error handling rules established
- Integration contracts with other governors

---

### Session 4: Streamlit Governor

**Duration:** ~15-20 minutes

**Topics:**
1. When processed flag is set
2. Upload validation rules
3. Transformation modification criteria
4. Column mapping requirements
5. User error handling
6. Integration with Lambda

**Expected Outcomes:**
- Streamlit rules codified in governors/streamlit/constitution.md
- Upload flow documented step-by-step
- Validation rules established

---

## Snowflake Governor - Detailed Questions

### General Architecture

**Q1:** What are the absolute no-go zones in stored procedures? What code should NEVER be modified under any circumstances?

**Examples to discuss:**
- The `processed IS NULL OR processed = FALSE` filter in MOVE_STREAMLIT_DATA_TO_STAGING
- UNION fallback patterns in bucket procedures
- Template variable replacements
- Phase transition logic

**Q2:** Which stored procedures are most critical and need the highest level of protection?

**Q3:** What dependencies exist between procedures that we must preserve?
- Which procedures call other procedures?
- What is the correct execution order?
- What happens if a procedure fails mid-pipeline?

**Q4:** What is the phase progression system (0→1→2→3) and when can it be modified?

---

### Bucket Matching

**Q5:** When is it acceptable to modify bucket matching criteria?
- Do changes require full regression testing?
- What approval is needed?

**Q6:** Are UNION fallback patterns required in ALL bucket procedures?
- Which buckets currently have UNION?
- Which need to be added?
- When can a UNION be removed (if ever)?

**Q7:** What testing is required before deploying bucket procedure changes?
- Required test platforms?
- Required test scenarios?
- What constitutes a successful test?

**Q8:** How do we handle cases where no bucket matches a record?
- Is this acceptable?
- Should it trigger an alert?
- What happens to unmatched records?

**Q9:** What is the bucket selection precedence?
- If a record qualifies for multiple buckets, which takes priority?
- Is the current order (FULL_DATA → REF_ID_SERIES → etc.) correct?

---

### Date Handling

**Q10:** What date formats must be supported?
- Daily data (date field)
- Monthly data (month/year fields)
- Quarterly data (quarter/year fields)
- Weekly data?
- Any others?

**Q11:** What is the precedence when both `date` and `month/year` are populated?
- Which takes priority?
- Should we validate they match?

**Q12:** How should NULL dates be handled?
- Is NULL acceptable for some platforms?
- What validations should fail vs. warn?

**Q13:** How should quarterly-only data (no month) be handled?
- Set month to first month of quarter?
- Leave month NULL?

**Q14:** What should happen if date parsing fails?
- Fail the entire upload?
- Mark record with error flag?
- Use fallback logic?

**Q15:** Should we support fiscal years vs. calendar years?

---

### Data Movement & Phase Transitions

**Q16:** Under what circumstances (if any) can the `processed` filter in MOVE_STREAMLIT_DATA_TO_STAGING be modified?
- What would need to change to make this safe?
- Is this an absolute red line?

**Q17:** What filters are absolutely critical and must never be removed from any procedure?

**Q18:** When is it safe to reprocess data vs. when must we start fresh?
- Can we re-run procedures on already processed data?
- What flags control this?

**Q19:** What are the rollback procedures for each phase?
- How do we undo phase 1 (normalization)?
- How do we undo phase 2 (content matching)?
- How do we undo phase 3 (final insert)?

**Q20:** What happens if a procedure fails mid-phase?
- Does data stay in current phase?
- Do we roll back to previous phase?
- How do we resume?

---

### Template Variables & Environments

**Q21:** When can template variables be replaced with hardcoded values (if ever)?
- Are there any scenarios where this is acceptable?
- Or is this an absolute red line?

**Q22:** What are the exact differences between STAGING and PROD environments?
- Database names?
- Table names?
- Any logic differences?

**Q23:** How do we ensure staging accurately mirrors prod?
- What validations are needed?
- How often should we verify?

**Q24:** What testing must happen in staging before promoting to prod?
- Minimum test cases?
- Required platforms to test?
- How long to monitor staging?

---

### Error Handling & Logging

**Q25:** What level of logging is required in stored procedures?
- Every UPDATE statement?
- Only errors?
- Row counts?

**Q26:** When should procedures fail vs. continue with partial success?
- If bucket matching fails for some records, continue?
- If date parsing fails for some records, fail entire batch?

**Q27:** How are errors communicated to Lambda/Streamlit?
- ERROR_LOG_TABLE entries?
- Return codes?
- Exceptions?

**Q28:** What triggers an automatic rollback?
- Any error?
- Specific error types?
- Manual only?

---

### Performance & Optimization

**Q29:** When can we modify query patterns for performance?
- Does performance optimization require same testing as logic changes?
- What approval is needed?

**Q30:** Are there any optimizations that are off-limits?
- Removing joins?
- Changing TEMPORARY table usage?
- Removing NOT EXISTS checks?

**Q31:** What are acceptable query execution times?
- How long should normalization take?
- How long should bucket matching take?
- When should we optimize?

**Q32:** When should we use TEMPORARY tables vs. permanent tables?
- Are buckets always TEMPORARY?
- Can this change?

---

### Grants & Permissions

**Q33:** Can GRANT statements at the end of procedures ever be removed?
- What if we're changing the role?
- Is WEB_APP always the correct role?

**Q34:** What roles need access to what procedures?
- Document the permission model

---

### Deployment & Rollback

**Q35:** What is the step-by-step deployment procedure for stored procedures?
- Who can deploy?
- What verification is required?
- How do we verify success?

**Q36:** What is the rollback procedure for a bad deployment?
- How do we revert to previous version?
- What if data was already processed with new version?

**Q37:** Can we ever skip staging and deploy directly to prod?
- Emergency situations?
- Or never?

---

### Known Issues & Edge Cases

**Q38:** December 2, 2025 fixes - How do we prevent regression?
- Date fields going NULL - what automated test would catch this?
- REF_ID_SERIES UNION missing - how do we verify UNION exists?
- Should DDL verification be part of deployment?

**Q39:** What other edge cases should we watch for?
- Platform-specific quirks?
- Data quality issues that procedures must handle?

**Q40:** What are the most common failure modes?
- Historically, what breaks most often?
- What preventive measures should we take?

---

## Testing Governor - Detailed Questions

### Testing Requirements

**Q1:** What tests are required before ANY deployment?
- Unit tests for individual procedures?
- Integration tests for full pipeline?
- Both?

**Q2:** What constitutes sufficient testing for different change types?
- Bug fix: [testing required]
- New feature: [testing required]
- Performance optimization: [testing required]
- Refactoring: [testing required]

**Q3:** When can we skip tests (if ever)?
- Documentation changes?
- Emergency hotfixes?
- Never?

**Q4:** How do we test changes that affect multiple components?
- End-to-end testing required?
- Component testing sufficient?

---

### Staging Environment

**Q5:** What is the exact difference between staging and prod?
- Document all differences

**Q6:** What testing must happen in staging before prod deployment?
- Minimum test cases?
- Required test platforms?
- Required test data?

**Q7:** How long should we monitor staging before promoting to prod?
- Hours? Days?
- Depends on change type?

**Q8:** What criteria must be met before promoting staging to prod?
- Zero errors?
- Specific success metrics?

---

### Deployment Process

**Q9:** What is the step-by-step deployment procedure?
- Checklist format
- Who approves each step?

**Q10:** What verification happens after deployment?
- DDL checks?
- Test execution?
- Data validation?

**Q11:** How do we know a deployment was successful?
- Specific checks
- Success criteria

---

### Test Data

**Q12:** What test data should we maintain?
- Sample files for each platform?
- Edge cases?
- Regression test data?

**Q13:** How do we test edge cases?
- Monthly/quarterly data
- Missing columns
- Invalid data
- Large files

**Q14:** How do we test with real data safely?
- Copy to staging?
- Anonymize?
- Sample subset?

**Q15:** What platforms/scenarios must be tested?
- List all required test cases

---

### Verification

**Q16:** What checks must pass before considering deployment successful?
- Procedure exists?
- DDL matches source?
- Test execution passes?
- Data validation passes?

**Q17:** How do we verify DDL matches source code?
- Automated check?
- Manual review?
- Required keywords (like UNION)?

**Q18:** How do we verify data is processed correctly?
- Row counts?
- Field population?
- Sample data review?

**Q19:** What error logs indicate a problem?
- What patterns to watch for?
- What triggers rollback?

---

### Rollback

**Q20:** What triggers an immediate rollback?
- List specific criteria

**Q21:** What is the rollback procedure for each component type?
- Stored procedures: [procedure]
- Lambda: [procedure]
- Streamlit: [procedure]

**Q22:** How do we test rollback procedures?
- Practice rollbacks?
- Automated tests?

**Q23:** How do we prevent data loss during rollback?
- Backup procedures
- Safe rollback steps

---

## Lambda Governor - Questions

[20 questions covering orchestration, error handling, integration, deployment]

## Streamlit Governor - Questions

[20 questions covering upload flow, transformations, validation, integration]

---

## Post-Convention Deliverables

After all sessions complete:

1. **Updated Constitution** - Core rules codified from all sessions
2. **Safety Council Veto Rules** - Comprehensive red line catalog
3. **Governor Constitutions** - Domain-specific rules for each governor
4. **Delegate Knowledge Bases** - Complete documentation for all 30+ delegates
5. **Testing Checklist** - Required tests for each change type
6. **Deployment Procedures** - Step-by-step for each component
7. **Rollback Procedures** - Step-by-step for each component
8. **Communication Protocols** - Escalation paths and voting mechanisms

---

## Notes for President

**During Convention:**
- Take detailed notes on all answers
- Ask follow-up questions for clarification
- Identify contradictions or gaps
- Propose solutions for user approval
- Document examples from past incidents

**After Convention:**
- Codify all answers into appropriate documents
- Create cross-references between related rules
- Build searchable knowledge base
- Test governance with sample scenarios
- Get user approval on final structure

**Remember:**
- This is about preventing future mistakes like the processed filter incident
- The goal is clarity, not bureaucracy
- When in doubt, ask the user
- Document the "why" not just the "what"
