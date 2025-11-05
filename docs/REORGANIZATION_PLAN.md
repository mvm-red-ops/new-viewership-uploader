# Codebase Reorganization Plan

## Current Problems
1. UDF definitions mixed with procedures
2. Permissions scattered across multiple files
3. No clear deployment order/dependencies
4. Column additions done manually
5. Hard to understand what needs to be deployed where

## New Structure

```
sql/
├── migrations/                    # Modular, ordered SQL files
│   ├── 001_schema_tables.sql     # Table definitions & alterations
│   ├── 002_udfs.sql              # User-defined functions only
│   ├── 003_procedures_shared.sql # Shared procedures (phase-agnostic)
│   ├── 004_procedures_phase2.sql # Phase 2 procedures (asset matching)
│   ├── 005_procedures_phase3.sql # Phase 3 procedures (final insert)
│   └── 006_permissions.sql       # All GRANT statements
│
├── deploy/                        # Orchestration scripts
│   ├── deploy.py                 # Main deployment orchestrator
│   ├── config.yaml               # Environment configurations
│   └── verify.py                 # Post-deployment verification
│
├── templates/                     # Keep for reference/legacy
│   └── (existing files)
│
└── utils/                         # Helper scripts
    ├── cleanup.py                # Data cleanup utilities
    └── diagnostics.py            # Diagnostic queries
```

## Principles

### 1. Separation of Concerns
- **Schema**: Only DDL (CREATE/ALTER TABLE, ADD COLUMN)
- **UDFs**: Only function definitions
- **Procedures**: Only stored procedure logic
- **Permissions**: Only GRANT statements

### 2. Idempotent
- All SQL uses `CREATE OR REPLACE`
- Column additions use `ADD COLUMN IF NOT EXISTS` pattern
- Safe to run multiple times

### 3. Environment Agnostic
- SQL files use `{{PLACEHOLDERS}}`
- Single source of truth for environment config
- Deployment script handles replacements

### 4. Clear Dependencies
- Numbered migrations enforce order
- Each file can be deployed independently if deps met
- Explicit dependency documentation

### 5. Orchestration
- Single command deploys everything: `python sql/deploy/deploy.py --env staging`
- Supports partial deployment: `python sql/deploy/deploy.py --env staging --only permissions`
- Automatic verification after deployment

## Migration Strategy

### Phase 1: Extract and Modularize (No Breaking Changes)
1. Create new modular structure
2. Extract components from existing templates
3. Keep templates as-is for safety

### Phase 2: Test New Structure
1. Deploy to staging using new scripts
2. Verify everything works
3. Compare with existing approach

### Phase 3: Migrate Fully
1. Update CI/CD to use new structure
2. Deprecate old deployment scripts
3. Archive templates

## Deployment Flow

```
deploy.py --env staging
  ↓
1. Load config.yaml (staging section)
  ↓
2. Connect to Snowflake
  ↓
3. Execute migrations in order:
   - 001_schema_tables.sql
   - 002_udfs.sql
   - 003_procedures_shared.sql
   - 004_procedures_phase2.sql
   - 005_procedures_phase3.sql
   - 006_permissions.sql
  ↓
4. Run verification queries
  ↓
5. Report success/failure
```

## Benefits

1. **Easier to understand**: Each file has one clear purpose
2. **Easier to maintain**: Change UDF? Only touch 002_udfs.sql
3. **Easier to debug**: Know exactly what was deployed when
4. **Easier to onboard**: Clear structure and documentation
5. **Safer deployments**: Verification and idempotency built-in
6. **Flexible**: Can deploy just permissions without redeploying procedures
