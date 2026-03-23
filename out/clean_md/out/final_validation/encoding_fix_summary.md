# Encoding Corruption Fix Summary

Total raw findings: 322  
Unique (file, field, value, type) entries: 127  
Publication blockers (visible corruption): 127

## By file

| Source file | Raw issues |
|---|---|
| Placements_Flat | 214 |
| Placements_ByPerson | 104 |
| Persons_Truth | 1 |
| stage2_canonical_events | 1 |
| canonical/events | 1 |
| canonical/event_results | 0 |
| canonical/persons | 1 |

## By corruption type

| Type | Count | Description |
|---|---|---|
| FFFD | 237 | U+FFFD replacement char — encoding fallback in source HTML |
| QS_APOS | 76 | ?S apostrophe — Women?S, Master?S (encoding artifact) |
| ISO88592 | 5 | ISO-8859-2 byte misread as Latin-1 (¹→š, ¿→ż, etc.) |
| MOJI_QUOTE | 4 | ÏxyzÓ mojibake — corrupted smart quotes around nickname |

## Where fixes are applied

All corruption listed here originates in the HTML mirror or legacy data files,
was propagated through stages 1-2 into the identity lock artifacts, and appears
in canonical outputs. Canonical CSVs are **not altered** — fixes are applied
in the presentation layer (`pipeline/04B_create_community_excel.py`) only.

The functions `_clean_div()`, `_fix_name_encoding()`, and `_fix_display_str()`
in 04B handle the presentation-layer corrections.

## Publication rule

Any visible encoding corruption in public-facing workbook cells is a **BLOCKER**.

After applying presentation-layer fixes, no visible corruption should remain.

---

**ENCODING_FAIL** — 127 unique corrupted values require fixes.
See `encoding_corruption_report.csv` for full details.
