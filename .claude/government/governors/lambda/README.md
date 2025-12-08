# Lambda Governor

## Domain

The Lambda Governor owns all aspects of AWS Lambda orchestration:
- Lambda function deployment and configuration
- Procedure execution orchestration
- Error handling and retry logic
- Logging and monitoring
- Integration between Streamlit and Snowflake

## Status

**AWAITING CONSTITUTIONAL CONVENTION**

## Delegates

### Orchestration
- **Main Orchestrator Delegate** - Primary Lambda handler that coordinates procedure execution
- **Error Handler Delegate** - Retry logic and failure handling
- **Logging Delegate** - CloudWatch logging and error tracking

### Integration
- **Streamlit Integration Delegate** - Receives triggers from Streamlit app
- **Snowflake Integration Delegate** - Executes stored procedures in Snowflake

## Critical Red Lines

To be established in Constitutional Convention.

**Potential Red Lines:**
- Procedure execution order
- Retry logic patterns
- Error propagation to Streamlit
- Timeout configurations

## Communication Protocol

```
Lambda Delegates → Lambda Governor: Always allowed
Lambda Governor → President: Always allowed
Lambda Delegate → President: Requires Lambda Governor approval OR 2+ delegate consensus
```

## Decision Authority

**Lambda Governor CAN approve:**
- Changes to logging levels
- Retry configuration adjustments
- Performance optimizations
- Documentation updates

**Lambda Governor CANNOT approve alone:**
- Changes to procedure execution order (requires Snowflake Governor)
- Breaking changes to Streamlit integration (requires Streamlit Governor)
- Removal of error handling (requires Testing Governor verification)

## Constitutional Convention Questions

### Orchestration
- [ ] What is the exact sequence of procedure calls?
- [ ] What happens if a procedure fails mid-sequence?
- [ ] When should Lambda retry vs. fail immediately?
- [ ] How are partial successes handled?

### Error Handling
- [ ] What errors should trigger automatic retry?
- [ ] What errors should fail immediately?
- [ ] How are errors communicated back to Streamlit?
- [ ] What logging is required for troubleshooting?

### Integration
- [ ] How does Streamlit trigger Lambda?
- [ ] What parameters are passed from Streamlit to Lambda?
- [ ] How does Lambda pass results back to Streamlit?
- [ ] What timeout values are appropriate?

### Deployment
- [ ] How are Lambda functions deployed?
- [ ] What testing is required before deployment?
- [ ] How do we rollback a Lambda deployment?
- [ ] What environment variables are critical?

## Delegate Knowledge Base

To be built after Constitutional Convention.

## Next Steps

- [ ] Hold Constitutional Convention with Citizen
- [ ] Document Lambda execution flow
- [ ] Map dependencies with Snowflake procedures
- [ ] Establish error handling patterns
- [ ] Create deployment procedures
