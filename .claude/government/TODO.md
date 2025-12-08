# Government System TODO List

## Tomorrow's Priority: Constitutional Convention

### Preparation (TONIGHT - Before User Returns)
- [x] Create government directory structure
- [x] Write constitution.md with core governance rules
- [x] Create Safety Council veto-rules.md
- [x] Create Governor READMEs for all 4 governors
- [x] Create delegate knowledge bases for critical procedures
- [x] Document recent incidents as examples
- [ ] Create Constitutional Convention agenda
- [ ] Prepare question templates for each Governor

### Constitutional Convention (TOMORROW - With User)

**Snowflake Governor Questions:**
- [ ] Hold session with user to clarify Snowflake red lines
- [ ] Document answers in governors/snowflake/constitution.md
- [ ] Catalog all procedure dependencies
- [ ] Establish bucket procedure modification rules
- [ ] Clarify testing requirements for stored procedure changes
- [ ] Document rollback procedures

**Lambda Governor Questions:**
- [ ] Hold session with user to clarify Lambda orchestration
- [ ] Document procedure execution sequence
- [ ] Establish retry and error handling rules
- [ ] Clarify integration points with Snowflake/Streamlit

**Streamlit Governor Questions:**
- [ ] Hold session with user to clarify UI/upload rules
- [ ] Document when processed flag is set
- [ ] Establish transformation modification rules
- [ ] Clarify validation requirements

**Testing Governor Questions:**
- [ ] Hold session with user to clarify testing requirements
- [ ] Document required tests for each change type
- [ ] Establish deployment verification checklist
- [ ] Create rollback trigger criteria

### Post-Convention Tasks
- [ ] Codify all answers into constitution and governor-specific rules
- [ ] Build complete delegate knowledge bases
- [ ] Implement communication protocols
- [ ] Create escalation procedures
- [ ] Test governance system with sample scenario
- [ ] Get user approval on final governance structure

## Delegate Knowledge Base Expansion

### Snowflake Delegates (Priority)
- [x] MOVE_STREAMLIT_DATA_TO_STAGING
- [x] SET_DATE_COLUMNS_DYNAMIC
- [x] REF_ID_SERIES Bucket
- [ ] NORMALIZE_DATA_IN_STAGING (orchestrator)
- [ ] ANALYZE_AND_PROCESS (orchestrator)
- [ ] SET_DEAL_PARENT
- [ ] SET_REF_ID
- [ ] CALCULATE_METRICS
- [ ] SET_INTERNAL_SERIES
- [ ] FULL_DATA Bucket
- [ ] REF_ID_ONLY Bucket
- [ ] SERIES_SEASON_EPISODE Bucket
- [ ] SERIES_ONLY Bucket
- [ ] TITLE_ONLY Bucket
- [ ] HANDLE_FINAL_INSERT

### Lambda Delegates
- [ ] Main Orchestrator
- [ ] Error Handler
- [ ] Logging
- [ ] Streamlit Integration
- [ ] Snowflake Integration

### Streamlit Delegates
- [ ] File Upload
- [ ] Column Mapping
- [ ] Transformation
- [ ] Lambda Trigger
- [ ] Database Write
- [ ] Form
- [ ] Validation
- [ ] Error Display

### Testing Delegates
- [ ] Unit Test
- [ ] Integration Test
- [ ] Data Validation
- [ ] Staging Deployment
- [ ] Production Deployment
- [ ] Rollback
- [ ] DDL Verification
- [ ] Data Verification
- [ ] Error Log

## Communication & Escalation

- [ ] Implement speech request system (delegate → President)
- [ ] Implement voting mechanism (inter-governor decisions)
- [ ] Create escalation templates
- [ ] Document communication flow examples
- [ ] Test escalation with sample scenarios

## Documentation

- [ ] Link all delegate docs to their source files
- [ ] Create quick reference guide for President
- [ ] Document common scenarios and which Governor to consult
- [ ] Create incident response playbook
- [ ] Update TROUBLESHOOTING.md with governance references

## Safety Council

- [ ] Expand veto rules based on Constitutional Convention
- [ ] Create automated checks for red line violations
- [ ] Document override procedures
- [ ] Create incident response protocols
- [ ] Establish post-incident review process

## Testing & Validation

- [ ] Test governance with mock proposed change (safe)
- [ ] Test governance with mock proposed change (should be vetoed)
- [ ] Verify all escalation paths work
- [ ] Verify all delegates can raise concerns
- [ ] Test voting mechanism
- [ ] Test Constitutional Convention process

## Long-term Improvements

- [ ] Consider MCP server for Snowflake domain knowledge
- [ ] Consider MCP server for deployment automation
- [ ] Build automated DDL verification
- [ ] Create regression test suite for known issues
- [ ] Implement automated rollback triggers

---

## Status Tracking

**Current Phase:** Skeleton Complete, Awaiting Constitutional Convention

**Blockers:** None - Ready for user return tomorrow

**Next Milestone:** Complete Constitutional Convention with all 4 Governors

**Expected Timeline:** Constitutional Convention tomorrow, full system operational by end of week

---

## Notes for President

### How to Use This Government

**When User Makes a Request:**

1. **Identify Domain** - Which Governor owns this area?
   - Stored procedures → Snowflake Governor
   - Lambda orchestration → Lambda Governor
   - Streamlit UI/upload → Streamlit Governor
   - Deployment/testing → Testing Governor

2. **Consult Relevant Governor** - Review their README and red lines

3. **Check Safety Council** - Would this violate any veto rules?

4. **Consult Delegates** - For detailed changes, review specific delegate knowledge bases

5. **Draft Proposal** - Include:
   - What needs to change
   - Which Governor(s) reviewed
   - Any delegate concerns raised
   - Safety Council status (clear / veto)
   - Testing requirements
   - Rollback plan

6. **Present to User** - Clear, concise proposal with all internal discussion complete

**Red Flags to Watch For:**

- ❌ Removing filters from MOVE_STREAMLIT_DATA_TO_STAGING
- ❌ Removing UNION fallbacks from bucket procedures
- ❌ Hardcoding database names (removing template variables)
- ❌ Modifying phase transition logic
- ❌ Removing GRANT statements
- ❌ Deploying to PROD without staging verification
- ❌ Making changes without consulting relevant Governor

**When in Doubt:**
- Consult the Governor
- Check Safety Council veto rules
- Ask delegates for input
- Present multiple options to user rather than making assumption
