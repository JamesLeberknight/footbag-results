# Final Publication Validation Report

**Date:** 2026-03-12
**Dataset:** Footbag Historical Results v2.10.1
**PBP:** Placements_ByPerson_v62.csv (27,154 rows)
**Persons Truth:** Persons_Truth_Final_v42.csv (3,441 persons)

---

## 🟢 PUBLICATION STATUS: READY

---

## Summary

| Metric | Value |
|---|---|
| Events in stage2 | 774 |
| Events skipped — quarantined | 20 |
| Events skipped — no results | 31 |
| Source-partial events (documented) | 9 |
| Known-issue events | 54 |

| Check | Result | Blockers |
|---|---|---|
| Gate 1 — Division Preservation | ✓ PASS | 0 |
| Gate 2 — Placement Preservation | ✓ PASS | 0 |
| Gate 3 — Encoding Cleanliness | ✓ PASS | 0 |
| Gate 4 — Schema Integrity | ✓ PASS | 0 |

---

## Gate 1 — Division Preservation

Source divisions checked: 3797
Exact matches: 3797
Missing (total, incl. known-issue events): 0
Missing (blockers — not in known_issues): 0

---

## Gate 2 — Placement Preservation

Compares stage2 participant counts against canonical event_result_participants.csv.
Stage2 participants (non-quarantined): 34,596
Canonical participants (all events):   35,861
  (difference = quarantined events excluded from Gate 2 check but present in canonical)
Events checked (non-quarantined):      724
Events with participant delta:         0
Blockers (non-known-issue deltas):     0

_All participant counts match. Canonical export is complete._

---

## Gate 3 — Encoding Cleanliness

Files scanned: events.csv, event_disciplines.csv, event_result_participants.csv, Placements_Flat.csv

Issues are classified into three tiers:
- **FIXABLE** — pipeline bugs; block publication until resolved
- **SOURCE_LOSS** — U+FFFD from HTML mirror encoding loss; unrecoverable without original source
- **SOFT** — quoted nicknames stored as `?Name?`; low severity, non-blocking

| Tier | Count | Blocking? |
|---|---|---|
| FIXABLE pipeline corruption | 0 | No — 0 found |
| SOURCE_LOSS (U+FFFD from HTML mirror) | 99 | No — documented limitation |
| SOFT (?Nickname? patterns) | 5 | No — non-blocking |

_No fixable encoding corruption detected. Zero pipeline encoding bugs._

### ℹ SOURCE_LOSS — 99 U+FFFD Characters (Non-blocking)

These characters represent encoding loss in the HTML mirror source. The original footbag.org mirror had UTF-8 characters that were corrupted during archival. They cannot be recovered without the original source pages. Affected fields: player display names, person_canon for unresolved players.

Affected names include: accented characters in French, Finnish, German, Polish, and Czech names (e.g., François, Geneviève, Toni Pääkkönen, Václav Klouda, Robin Péchel).

This is a known source limitation documented in the dataset release notes.

### ℹ SOFT — 5 Quoted Nickname Patterns (Non-blocking)

Names like `?Dexter?`, `?Hollywood?`, `?Crazy?` represent quoted nicknames where the quotation marks were lost in source encoding. The names remain readable and searchable.

---

## Gate 4 — Schema Integrity

### 32_post_release_qc
Exit code: 0 (PASS)
  ✓  No spurious event_ids in Index
  ✓  Index total rows: 777 = 774 stage2 + 3 stubs
  ✓  No Coverage entries with placements_present=0
  SUMMARY
  ✓  All 6 checks passed — no issues found

### 33_schema_logic_qc
Exit code: 0 (PASS)
  ✓  Data_Integrity Placements total matches PF row count (27,154)
  ✓  Data_Integrity Persons Gate3 matches PT non-excluded count (3,441)
  ✓  Data_Integrity.csv is current (no staleness detected)
  SUMMARY
  ✓  All 7 checks passed — no errors found


---

## Quarantined Events (excluded from Gates 1–2)

Total quarantined: 20
These events have documented structural issues and are excluded from the canonical dataset.

| Event ID | Year | Event Name | Reason |
|---|---|---|---|
| 981560223 | 2001 | Swiss Footbag Championships (FootJam) | COMPLEX_COMPETITION_FORMAT |
| 1179679872 | 2007 | German Footbag Championships 2007 | COMPLEX_COMPETITION_FORMAT |
| 1269111845 | 2010 | The King of the Hill 2010 (presented by Aki Québec | COMPLEX_COMPETITION_FORMAT |
| 857880054 | 1997 | Bedford (Australia) Indoor Footbag Championships | SOURCE_PARTIAL |
| 910551956 | 1999 | 1999 Western Regional Footbag Championships | COMPLEX_COMPETITION_FORMAT |
| 947026077 | 2000 | The 2nd Annual Philly Open Footbag Championships | COMPLEX_COMPETITION_FORMAT |
| 1265745512 | 2010 | 31st IFPA WORLD FOOTBAG CHAMPIONSHIPS | SOURCE_PARTIAL |
| 1568692191 | 2019 | MedeJam de Footbag 2019 | SOURCE_PARTIAL |
| 1114277529 | 2005 | Czech Footbag Championships | COMPLEX_COMPETITION_FORMAT |
| 1127155729 | 2005 | Zocha Jam 2005 | SOURCE_PARTIAL |
| 1158263300 | 2006 | Zocha Jam 2006 | COMPLEX_COMPETITION_FORMAT |
| 1200725314 | 2008 | SoCali Jam 08 | OVERRIDE_CONFLICT |
| 1294262550 | 2011 | 12e Open de France de Footbag | COMPLEX_COMPETITION_FORMAT |
| 1301837216 | 2011 | The King of the Hill 2011 (presented by Aki Québec | COMPLEX_COMPETITION_FORMAT |
| 1331667371 | 2012 | Paris Net Battle 2 | COMPLEX_COMPETITION_FORMAT |
| 1537994275 | 2018 | 2o Torneo Nacional de Footbag | SOURCE_PARTIAL |
| 1160579826 | 2006 | CommALaMaison Contest 3 | SOURCE_PARTIAL |
| 959353403 | 2000 | 2000 Southeast Idaho Footbag Championships | SOURCE_PARTIAL |
| 984694623 | 2001 | 1st Montreal Summer Freestyle Challenge | SOURCE_PARTIAL |
| 1366240051 | 2013 | 4ta. Copa Ciencias | SOURCE_PARTIAL |

---

## Artifact Inventory

| Artifact | Version | Rows |
|---|---|---|
| Placements_ByPerson | v62 | 27,154 |
| Persons_Truth_Final | v42 | 3,441 |
| Persons_Unresolved_Organized | v28 | 82 |
| Stage2 events | — | 774 |
| Known-issue events | — | 54 |
| Quarantined events | — | 20 |

---

_Generated by tools/58_final_publication_validation.py_