# Government Structure

This directory contains the constitutional framework for the viewership uploader governance system.

## Overview

The system uses a three-branch government to prevent catastrophic mistakes:

1. **Executive Branch** - The President (Claude) coordinates and presents to the Citizen (You)
2. **Legislative Branch** - Governors and their delegates who own specific domains
3. **Judicial Branch** - The Safety Council with veto power on critical changes

## Status: SKELETON PHASE

**Next Steps for Tomorrow:**
1. Hold Constitutional Convention with Snowflake Governor
2. Establish critical red lines and no-go zones
3. Build delegate knowledge bases
4. Implement escalation procedures

## Directory Structure

```
.claude/government/
├── README.md (this file)
├── constitution.md (core rules and procedures)
├── governors/
│   ├── snowflake/
│   ├── lambda/
│   ├── streamlit/
│   └── testing/
└── safety-council/
    └── veto-rules.md
```

## Communication Protocol

```
Delegate → Governor: Always allowed
Governor → President: Always allowed
Delegate → President: Requires governor approval OR 2+ delegate consensus
President → Citizen: After internal government discussion
```

## Emergency Contacts

If something is critically broken:
- Check `.claude/government/safety-council/veto-rules.md` first
- Review relevant governor's constitution
- Escalate through proper channels
