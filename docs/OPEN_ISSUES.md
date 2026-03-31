# Open Issues — Canonical Dataset

This file tracks **known gaps, ambiguities, and deferred improvements**.

These are NOT errors unless they violate QC or canonical contract.

---

## High Priority

### 2016 World Championships (Trnava)

- Event appears sparse relative to adjacent years
- Cause: manually-entered results block not parsed
- Missing:
  - women's routines
  - intermediate net divisions
  - circle contests
  - additional freestyle disciplines

Status: NOT INGESTED  
Action: create RESULTS_FILE_OVERRIDE after verification

---

### 2015 / 2018 Worlds (Potential Similar Pattern)

- Likely also contain manually-entered blocks
- Require audit

Status: NOT INVESTIGATED

---

## Medium Priority

### 1984 / 1985 Event Duplication

- Possible duplicate events:
  - 1984_worlds vs 1984_wfa_nationals
  - 1985_worlds vs 1985_wfa_nationals

Status: REVIEW REQUIRED  
Risk: incorrect merge or double counting

---

### Division Completeness (Modern Era)

Some events may:
- omit minor divisions
- include only published categories

This is acceptable if:
- no mirror data is lost
- omissions are source-driven

---

## Low Priority

### Identity Edge Cases

Examples:
- Jodi Sebastian vs Jodi Chandler
- Doug Little placements
- isolated name variants

Status: SAFE TO DEFER

---

## Principle

Open issues are:

- tracked
- documented
- intentionally unresolved

They must NOT be silently fixed or guessed.
