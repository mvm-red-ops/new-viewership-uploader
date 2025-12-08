# First Government Session: Code Audit

This document outlines how to conduct the first government session with the Snowflake Governor.

## Prerequisites

✅ **Completed**:
- Government directory structure created
- Snowflake Governor constitution written
- Knowledge hierarchy established (CANONICAL, REFERENCE, DIAGNOSTIC)
- Bloat files archived

## Session Objective

The Snowflake Governor will perform a **comprehensive code audit** of all stored procedures in the codebase, categorizing each file and providing cleanup recommendations.

## How to Conduct the Session

### Option 1: Manual Review (Oracle-Led)

The Oracle (user) reviews the stored procedures directory structure directly and works with Mr. President (Claude) to categorize files.

**Process**:
1. List all SQL files in `snowflake/stored_procedures/`
2. For each file, determine:
   - **CANONICAL**: Active, production-ready code
   - **DEPRECATED**: Old code no longer in use
   - **REDUNDANT**: Duplicate functionality
   - **ONE-OFF**: Temporary/diagnostic script
3. Document findings in audit report
4. Get Oracle approval for recommendations

### Option 2: Automated Review (Agent-Led)

Spawn an exploration agent to systematically review the codebase.

**Process**:
1. Use Task tool with `subagent_type="Explore"`
2. Provide agent with:
   - Snowflake Governor constitution
   - Knowledge hierarchy context
   - Audit criteria (CANONICAL/DEPRECATED/REDUNDANT/ONE-OFF)
3. Agent generates structured report
4. Oracle reviews and approves recommendations

### Option 3: Interactive Government Session

Use the MCP Government architecture (when fully implemented) to spawn a governor-specific agent.

**Process** (Future):
1. Activate Snowflake Governor with full constitutional authority
2. Governor autonomously reviews stored procedures
3. Governor generates audit report with recommendations
4. Governor presents findings to Oracle for consensus
5. Upon approval, Governor executes cleanup

## Audit Criteria

### CANONICAL
**Definition**: Production-ready code that is actively used and maintained

**Indicators**:
- Located in `snowflake/stored_procedures/staging/` or `production/`
- Referenced in Lambda code (`aws/lambda-master/register-data-processing-lambda/snowflake-helpers.js`)
- Referenced in deployment scripts (`deploy_staging.py`, `deploy_production.py`)
- Has clear purpose documented in CANONICAL.md
- Used in current data processing pipeline

**Examples**:
- `normalize_data_in_staging_generic.sql`
- `analyze_and_process_viewership_data_generic.sql`
- `handle_final_insert_dynamic_generic.sql`

### DEPRECATED
**Definition**: Old code no longer in active use but preserved for historical reference

**Indicators**:
- Superseded by newer generic procedures
- Platform-specific when generic version exists
- Not referenced in current Lambda code
- Documented in REFERENCE.md

**Examples**:
- Old platform-specific normalization procedures
- Procedures targeting deprecated table structures
- Old data migration scripts

### REDUNDANT
**Definition**: Duplicate functionality that should be consolidated

**Indicators**:
- Nearly identical to another procedure
- Same logic exists in multiple files
- Can be replaced by calling existing procedure
- Legacy copies not removed during refactoring

**Examples**:
- Multiple versions of same procedure with minor differences
- Copy-pasted procedures with only database name changes

### ONE-OFF
**Definition**: Temporary scripts for debugging or one-time operations

**Indicators**:
- File names with `test_`, `debug_`, `temp_`, `fix_` prefixes
- Not in standard procedure directories
- Hardcoded values for specific data fixes
- Created for specific bug investigation

**Examples**:
- `cleanup_tubi_test.sql` (already archived)
- `fix_tubi_data.sql` (already archived)

## Expected Audit Output

### Report Structure

```markdown
# Snowflake Stored Procedures Audit Report
Generated: [Date]
Audited by: Snowflake Governor

## Summary
- Total Files: X
- CANONICAL: X
- DEPRECATED: X
- REDUNDANT: X
- ONE-OFF: X

## Detailed Findings

### CANONICAL (Production-Ready)
#### Staging Procedures (`snowflake/stored_procedures/staging/generic/`)
1. `normalize_data_in_staging_generic.sql`
   - Purpose: Wrapper for all normalization steps including ref_id mapping
   - Used by: Lambda Streamlit processing path
   - Status: ✅ Active

2. `set_deal_parent_generic.sql`
   - Purpose: Sets deal/partner/channel/territory from DICTIONARY
   - Used by: Normalization procedures
   - Status: ✅ Active

[... continue for all canonical procedures ...]

#### Production Procedures (`snowflake/stored_procedures/production/generic/`)
[Same structure as staging, but targeting production databases]

### DEPRECATED
1. `normalize_tubi_data.sql` (if exists)
   - Location: [path]
   - Reason: Superseded by generic version
   - Recommendation: Archive to `_archive/2025-12-05-pre-government/`

[... continue for all deprecated files ...]

### REDUNDANT
1. [File A] and [File B]
   - Duplication: [describe overlap]
   - Recommendation: Keep [File A], remove [File B]

[... continue for all redundant pairs ...]

### ONE-OFF
1. [File]
   - Location: [path]
   - Purpose: [describe one-off use]
   - Recommendation: Archive or delete if already executed

[... continue for all one-off scripts ...]

## Recommendations

### Immediate Actions
1. Archive deprecated files to `_archive/deprecated-procedures/`
2. Remove redundant files (keep the canonical version)
3. Move one-off scripts to `sql/diagnostics/` or archive

### Documentation Updates
1. Update CANONICAL.md with any missing procedures
2. Update REFERENCE.md with newly deprecated items
3. Update DIAGNOSTIC.md with patterns from one-off scripts

### Code Consolidation
1. [Specific consolidation recommendations]
2. [Opportunities to reduce duplication]

## Risk Assessment
[Note any high-risk changes that require extra caution]

## Next Steps
[Propose sequencing for implementing recommendations]
```

## Post-Audit Actions

After Oracle approves the audit report:

1. **Execute Archival**: Move deprecated and one-off files to appropriate archive locations
2. **Update Documentation**: Modify CANONICAL.md, REFERENCE.md, and DIAGNOSTIC.md
3. **Remove Redundancies**: Delete redundant files (with Oracle approval for each)
4. **Update Deployment Scripts**: Ensure deploy scripts don't reference removed files
5. **Test Pipeline**: Verify staging and production pipelines still work
6. **Commit Changes**: Create git commit documenting cleanup

## Current Directory Structure

```
snowflake/stored_procedures/
├── staging/
│   └── generic/
│       ├── normalize_data_in_staging_generic.sql ✅ CANONICAL
│       ├── normalize_data_in_staging.sql ✅ CANONICAL
│       ├── set_deal_parent_generic.sql ✅ CANONICAL
│       ├── set_ref_id_from_platform_content_id.sql ✅ CANONICAL
│       ├── calculate_viewership_metrics.sql ✅ CANONICAL
│       ├── set_date_columns_dynamic.sql ✅ CANONICAL
│       ├── set_phase_generic.sql ✅ CANONICAL
│       ├── analyze_and_process_viewership_data_generic.sql ✅ CANONICAL
│       ├── set_internal_series_generic.sql ✅ CANONICAL
│       ├── handle_final_insert_dynamic_generic.sql ✅ CANONICAL
│       ├── move_viewership_to_staging.sql ✅ CANONICAL
│       └── move_streamlit_data_to_staging.sql ✅ CANONICAL
├── production/
│   └── generic/
│       └── [Same files as staging, targeting production DBs] ✅ CANONICAL
├── deploy_staging.py ✅ CANONICAL
└── deploy_production.py ✅ CANONICAL
```

## Governor Authority

Per the Snowflake Governor constitution:

**Autonomy** (Can decide without Oracle approval):
- Categorizing code as CANONICAL/DEPRECATED/REDUNDANT/ONE-OFF
- Recommending refactoring approaches
- Identifying redundancies
- Flagging data integrity issues

**Requires Oracle Approval**:
- Deleting any files
- Modifying database schemas
- Changing production stored procedures
- Major architectural changes

## Success Criteria

The first government session is successful when:
1. ✅ All stored procedures have been categorized
2. ✅ Audit report has been generated
3. ✅ Oracle has reviewed and approved findings
4. ✅ Cleanup recommendations are documented
5. ✅ Next steps are clear and actionable

## Ready to Begin?

Based on the current state, we have two paths forward:

### Path A: Quick Manual Audit (Recommended)
Since the directory structure is already clean with staging/production separation complete, we can do a quick manual verification that all current files are CANONICAL and document any remaining bloat.

### Path B: Full Agent-Led Audit
Spawn an exploration agent to comprehensively review the entire codebase, generating a detailed report with findings beyond just stored procedures (could include Lambda code, Python scripts, documentation, etc.).

**Oracle Decision Required**: Which path should we take?
