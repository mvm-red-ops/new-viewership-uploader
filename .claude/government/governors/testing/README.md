# Testing Governor

## Domain

The Testing Governor owns all aspects of testing and deployment verification:
- Pre-deployment testing requirements
- Staging environment validation
- Production deployment verification
- Rollback procedures
- Test data management

## Status

**AWAITING CONSTITUTIONAL CONVENTION**

## Delegates

### Testing
- **Unit Test Delegate** - Individual procedure testing
- **Integration Test Delegate** - Full pipeline testing
- **Data Validation Delegate** - Verifying data correctness

### Deployment
- **Staging Deployment Delegate** - Deploying to TEST_STAGING
- **Production Deployment Delegate** - Deploying to PROD
- **Rollback Delegate** - Reverting deployments when needed

### Verification
- **DDL Verification Delegate** - Checking deployed procedure contents
- **Data Verification Delegate** - Validating results in tables
- **Error Log Delegate** - Monitoring ERROR_LOG_TABLE

## Critical Red Lines

To be established in Constitutional Convention.

**Potential Red Lines:**
- Never deploy to PROD without staging verification
- Required test coverage before deployment
- Rollback triggers
- Verification checklist requirements

## Communication Protocol

```
Testing Delegates → Testing Governor: Always allowed
Testing Governor → President: Always allowed
Testing Delegate → President: Requires Testing Governor approval OR 2+ delegate consensus
```

## Decision Authority

**Testing Governor CAN approve:**
- Test procedure updates
- Verification checklist modifications
- Documentation updates
- Test data additions

**Testing Governor CANNOT approve alone:**
- Reducing required test coverage (requires all Governors + Citizen)
- Skipping staging verification (Safety Council veto)
- Deploying without testing (Safety Council veto)

## Constitutional Convention Questions

### Testing Requirements
- [ ] What tests are required before any deployment?
- [ ] What constitutes sufficient testing for different change types?
- [ ] When can we skip tests (if ever)?
- [ ] How do we test changes that affect multiple components?

### Staging Environment
- [ ] What is the exact difference between staging and prod?
- [ ] How do we ensure staging mirrors prod accurately?
- [ ] What testing must happen in staging before prod deployment?
- [ ] How long should we monitor staging before promoting to prod?

### Deployment Process
- [ ] What is the step-by-step deployment procedure?
- [ ] Who can deploy to staging vs. prod?
- [ ] What verification happens after deployment?
- [ ] How do we know a deployment was successful?

### Test Data
- [ ] What test data should we maintain?
- [ ] How do we test edge cases?
- [ ] How do we test with real data safely?
- [ ] What platforms/scenarios must be tested?

### Verification
- [ ] What checks must pass before considering deployment successful?
- [ ] How do we verify DDL matches source code?
- [ ] How do we verify data is processed correctly?
- [ ] What error logs indicate a problem?

### Rollback
- [ ] What triggers an immediate rollback?
- [ ] What is the rollback procedure for each component type?
- [ ] How do we test rollback procedures?
- [ ] How do we prevent data loss during rollback?

### Known Issues
- [ ] December 2, 2025: Date fields going NULL - How do we test for this regression?
- [ ] December 2, 2025: REF_ID_SERIES UNION missing - How do we verify UNION exists?
- [ ] What other regressions should we watch for?

## Delegate Knowledge Base

To be built after Constitutional Convention.

## Next Steps

- [ ] Hold Constitutional Convention with Citizen
- [ ] Document complete testing requirements
- [ ] Create deployment checklists
- [ ] Build rollback procedures
- [ ] Catalog test data and scenarios
- [ ] Create regression test suite
