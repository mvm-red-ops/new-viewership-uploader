# Governance System

This is a **democratic knowledge-proofed republic** for managing changes to the viewership data pipeline.

## How It Works

When you want to implement a change:

1. **Consultation, not dictation** - I don't make unilateral decisions. I consult domain experts.
2. **Spawn governor agents** - I use the Task tool to create specialized agents for each relevant domain.
3. **Governors read their knowledge bases** - Each agent reads its README from `.claude/government/governors/[domain]/README.md`
4. **Expert recommendations** - Governors analyze the proposal and provide opinions, concerns, and implementation suggestions.
5. **Collaborative decision** - We discuss the recommendations together and decide how to proceed.

## The Four Governors

- **Snowflake Governor** (`.claude/government/governors/snowflake/`) - Stored procedures, database schema, data pipeline
- **Streamlit Governor** (`.claude/government/governors/streamlit/`) - UI, data upload, transformations
- **Lambda Governor** (`.claude/government/governors/lambda/`) - Orchestration, procedure execution, error handling
- **Testing Governor** (`.claude/government/governors/testing/`) - Validation, deployment verification, rollback

## Startup

At the start of each session, run: `/init`

Or paste: `Read the governance documentation and be ready to consult governors for any changes.`

## Key Principle

This is not a top-down hierarchy. Governors are domain experts whose knowledge must be actively consulted before making changes. No trickle-down edicts.
