# Member ID Extraction Summary

**Run date:** 2026-03-11
**Source:** www.footbag.org/members/list (live search)

## Results

| Category | Count |
|---|---|
| Assigned (live match) | 1388 |
| Assigned (pre-existing in PT) | 367 |
| **Total assigned** | **1755** |
| Ambiguous (manual review needed) | 43 |
| No match found | 1645 |
| Search errors | 3 |
| **Total processed** | **3446** |

## Match methods used
- **EXACT_NAME_MATCH**: normalized display name identical to search query
- **NORMALIZED_NAME_MATCH**: display name tokens fully contained in result
- **ALIAS_MATCH**: footbag.org handle/alias matches query (e.g. DLeberknight ↔ Dave Leberknight)
- **MANUAL_RULE**: pre-existing legacyid from Persons_Truth (human-verified)

## Output files
- `member_id_assignments.csv` — high-confidence assignments
- `member_id_ambiguous.csv` — multiple candidates, needs manual review
- `member_id_no_match.csv` — no footbag.org profile found

## Notes
- Not all persons are registered footbag.org members
- Many historical persons (pre-2000) predate the member system
- South American and Finnish persons often not registered
- Unresolved persons and __NON_PERSON__ excluded from search
