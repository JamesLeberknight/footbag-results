# Phase 0 Results

## Goal
Determine whether canonicalization is causing substantial real data loss.

## Starting baseline
- PRESENT: 25,999
- ABSENT: 1,981
- UNKNOWN: 1,721
- ANC_DIV_ABSENT: 186

## Audit refinements added
- PP2_TRANSFORM matching
- PERSON_ID matching
- apostrophe normalization
- case normalization

## Final scorecard
- PRESENT: 26,592
- ABSENT: 1,388
- UNKNOWN: 1,131
- ANC_DIV_ABSENT: 18

## Key findings
- Most apparent loss was not real loss
- Major causes were audit blind spots, canonical naming normalization, and expected identity limitations
- Remaining genuine issues are very small and bounded

## Remaining genuine ANC_DIV_ABSENT rows
- 9 word-order variants
- 4 typo variants
- 2 PP5 pool-slot rows
- 2 rows from event 9921901 (1982 event absent from pipeline)
- 1 abbreviation variant

## Conclusion
Current stable pipeline is largely correct.
Further work should be narrow, explicit, and low-risk.
