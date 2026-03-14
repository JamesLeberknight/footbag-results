# CLAUDE.md
## Footbag Results Pipeline — Canonical Contract (v1.2)

This document defines the architectural contract and philosophical constraints
for the Footbag historical results pipeline.

It is authoritative for pipeline behavior.

---

# 0. Project Output Contract

The pipeline produces two deliverable artifacts.

## Deliverable A — Community Workbook

A public spreadsheet intended for the footbag community containing:

- yearly event results
- summary statistics
- player summaries
- historical consecutive records
- a technical appendix with freestyle analytics

Freestyle analytics exist only in the FREESTYLE INSIGHTS sheet and must not alter historical results.

## Deliverable B — Canonical Relational Dataset

A normalized set of CSV tables representing the historical archive in relational form.
These tables are intended for database creation and future applications.

## Non-Deliverables

The following artifacts exist only for validation and development support:

- diagnostic spreadsheets
- QC audit reports
- intermediate pipeline outputs
- experimental analysis surfaces

These must never be treated as final project outputs.

---

# 0.1 Freestyle Analytics Policy

Freestyle analytics provide technical insight into the evolution of freestyle play but are strictly supplementary.

Rules:

- They appear only in the FREESTYLE INSIGHTS sheet.
- They must not modify historical results.
- They must not alter player rankings or placement statistics.
- They exist purely as analytical commentary on the dataset.

The historical competition record always remains the authoritative dataset.

---

# 1. Core Principles (Non-Negotiable)

1. Deterministic builds.
2. No silent merges.
3. No guessing without explicit tagging.
4. One real person = one canonical person record.
5. Garbage / non-person entities are preserved, not erased.
6. Human-verified identity truth overrides all heuristics.
7. Analytics depend ONLY on canonical identity outputs.
8. **Results fidelity.** Competition results are primary data. Identity resolution
   is enrichment. Failure to resolve a player identity must NOT remove placements.
   Unresolved players are assigned `__NON_PERSON__` and retained in the dataset.

This is an archival system, not an experiment.

---

# 2. Identity Model

## 2.1 player_id vs person_id

- `player_id`
  - Raw competitor token derived from source data.
  - May represent real person, garbage, club, trick, or corrupted name.
  - Must never be deleted silently.

- `person_id`
  - Represents a real human being.
  - Assigned ONLY through human-verified identity artifacts.
  - Never inferred heuristically in release mode.
  - Unresolvable tokens receive `person_id = __NON_PERSON__` and are retained.

---

## 2.2 Canonical Identity Artifacts (Authoritative Inputs)

Release builds rely on:

```
inputs/identity_lock/
  Persons_Truth_Final_v42.csv       (3,441 persons)
  Persons_Unresolved_Organized_v28.csv  (82 rows)
  Placements_ByPerson_v64.csv       (28,511 rows)
```

These files are treated as ground truth.

They must satisfy:

- Exactly one row per real human in Persons_Truth.
- No collisions in person_canon.
- All unresolved real humans appear in Persons_Unresolved.
- All garbage explicitly classified (`__NON_PERSON__`).
- **No competitor dropped.** Every stage2 placement must appear in PBP as either
  a resolved person, an unresolved person, or `__NON_PERSON__`.

Identity truth is not recomputed in release mode.

### PBP version history

- v63 → v64 (2026-03-13): Targeted data quality fixes for 5 events.
  979816633: stripped leading ") " from 4 doubles entries.
  979089216: stripped "between " prefix from Logan Dethman team row; fixed caps.
  1195677906: restored Jakob/Matthias/André (Circle Competition) from __NON_PERSON__.
  1369141018: removed 2 "()" phantom partner rows; cleaned team_display_name.
  1727756195: Open Golf group2 renumbered 1-7 → 4-10; division_raw set to "Open Golf".
  Net: 28,513 → 28,511 rows (2 phantom rows removed).
- v62 → v63 (2026-03-12): Added 1,359 rows restoring 887 dropped placements across
  213 events. Previous versions silently omitted unresolved single-name tokens.
  v63 enforces the Results Fidelity principle: all stage2 placements present in PBP.

---

# 3. Pipeline Modes

## 3.1 Release Mode (Canonical)

Purpose:
Produce the canonical, archival dataset and Excel workbook.

Characteristics:
- Consumes identity-lock artifacts.
- Does not perform identity merges.
- Does not generate alias suggestions.
- Deterministic from a clean clone.
- Produces:
  - `out/Placements_Flat.csv`
  - `out/Placements_ByPerson.csv`
  - `out/Persons_Truth.csv`
  - `out/Persons_Unresolved.csv`
  - `out/persons_truth.lock`
  - `out/canonical/events_normalized.csv`
  - `Footbag_Results_Community_FINAL_v12.xlsx` (community workbook)

Release mode is the only mode required for archival reproduction.

---

## 3.2 Rebuild Mode (Research / Reconstruction)

Purpose:
Reconstruct placements from raw mirror data.

Characteristics:
- Parses HTML mirror.
- Produces canonicalized events.
- Generates candidate identities.
- May produce alias suggestions.
- Does NOT establish canonical identity.

Rebuild mode is exploratory.
Release mode is authoritative.

---

# 4. Stage Responsibilities

## Stage 01–02 (Rebuild Mode Only)
- Parse mirror.
- Normalize events.
- Preserve raw tokens.
- No identity merges.

## Stage 02p5
- Structural token cleanup.
- In Release Mode:
  - Generates `Placements_Flat.csv` and `Placements_ByPerson.csv` from the
    authoritative PBP lock file. Pure pass-through — no filtering.

## Stage 03
- Build canonical workbook structure.
- No identity mutations.

## Stage 04
- Apply identity lock artifacts.
- Enforce coverage guarantees.
- Generate analytics.
- Write `persons_truth.lock`.
- Finalize sheet ordering.

Stage 04 must never alter canonical identity rows.

## Stage 04B / Community Workbook Builder
- Produces the community-facing Excel workbook.
- Script: `tools/build_final_workbook_v12.py` (current canonical builder).
- Reads: `out/Placements_ByPerson.csv`, `out/stage2_canonical_events.csv`,
  `out/Persons_Truth.csv`, `out/canonical/events_normalized.csv`.
  Freestyle analytics inputs (from `out/noise_aggregates/`) are used for the
  FREESTYLE INSIGHTS sheet only.
- **Year sheets display ALL placements including unresolved/non-person entries.**
  Unresolved names are shown as-is using the `person_canon` token (cleaned).
  This was changed from earlier behavior which filtered these rows.
- Front sheets are rebuilt from canonical data, not copied from earlier workbooks.
- No identity mutations. Read-only with respect to canonical identity.

### Community workbook sheet order

```
README
DATA NOTES
STATISTICS
EVENT INDEX
PLAYER SUMMARY
CONSECUTIVE RECORDS
FREESTYLE INSIGHTS
1980, 1981, 1982, ...
2025
2026   (present, retained)
```

### FREESTYLE INSIGHTS sheet

Supplementary sheet summarising trick-sequence analytics from the freestyle
corpus. Contains six compact tables:

1. **Difficulty by Year** — chains, avg_sequence_add, max_sequence_add per year
2. **Hardest Documented Sequence** — single-row highlight (Greg Solis 2008, 22 ADD)
3. **Backbone Tricks of Freestyle** — top tricks by mentions with ADD, players, events
4. **Top Tricks by Mentions** — broader trick frequency table
5. **Most Common Trick Transitions** — top trick-pair transitions (A → B)
6. **Trick Innovation Timeline** — first/last year seen per trick

This sheet is a technical appendix. Freestyle analytics must not appear in
PLAYER SUMMARY, STATISTICS, or any year sheet. The primary purpose of the
workbook is historical results archival.

---

# 5. Coverage Guarantees

Every placement competitor must map to exactly one of:

- `Persons_Truth`
- `Persons_Unresolved`
- `__NON_PERSON__`

No row may be dropped silently.
All exclusions must be auditable.

Enforcement: `tools/final_dataset_verification.py` compares stage2 source
placements against PBP and reports any genuine gaps (BLOCKER_GENUINE).

---

# 6. Location Normalization

Canonical location data is stored in `out/canonical/events_normalized.csv`.
Original pipeline `events.csv` is preserved; normalized version is the display source.

## 6.1 Canonical storage format

```
city, region, country
```

Country is always written in full (United States, Canada, Spain, etc. — never USA, US, UK).

## 6.2 Public workbook display rules

**United States and Canada:**
```
City / State or Province column:  "City, State"     e.g. Rochester, New York
Country column:                   "United States"
```

**All other countries:**
```
City / State or Province column:  "City"            e.g. Bilbao
Country column:                   "Country"         e.g. Spain
```

Region is not displayed for non-US/CA events in the public workbook.

## 6.3 Normalization rules applied

- Country abbreviations expanded: USA → United States, B.C. → British Columbia, etc.
- Basque events: region = Bizkaia (for Bilbao/Larrabetzu area)
- Duplicate city = region: region field cleared (e.g. Stara Zagora, Stara Zagora → Stara Zagora)
- Venue names stripped from city field (e.g. RIT Rochester → Rochester)
- Street addresses never appear in location display fields

---

# 7. Identity Lock Sentinel

Release builds generate:

```
out/persons_truth.lock
```

This file contains:
- sha256 hashes of authoritative inputs
- row counts
- filenames
- release timestamp

This sentinel proves identity immutability for the release.

**Note:** When upgrading PBP only (adding `__NON_PERSON__` rows, no PT/PU changes),
the sentinel remains valid for PT and PU. Update the lock after any PBP patch.

---

# 8. Versioning Rules

- Patch (v1.0.x):
  - Documentation updates
  - Refactors
  - No data changes

- Minor (v1.x.0):
  - Additive analytics
  - Location normalization updates
  - PBP gap-fill patches (`__NON_PERSON__` additions only)

- Major (v2.0.0):
  - Any change to:
    - Persons_Truth
    - Persons_Unresolved
    - Identity classification logic

Identity changes require a new release and full regeneration.

---

# 9. What This System Is Not

- Not a dynamic alias resolution engine.
- Not a speculative merge system.
- Not an automated identity inference pipeline.
- Not tolerant of silent data loss.

It is a controlled archival reconstruction of historical data.

---

# 10. Mental Model

Human truth > heuristics.

Results fidelity > identity completeness.

Identity is locked.
Ambiguity is preserved.
Noise is explicit.
Reproducibility is mandatory.

---

# 11. Current Canonical State (as of 2026-03-12)

| Artifact | Version | Count |
|---|---|---|
| Persons_Truth_Final | v42 | 3,441 persons |
| Persons_Unresolved_Organized | v28 | 82 rows |
| Placements_ByPerson | v64 | 28,511 rows |
| Placements_Flat | — | 28,513 rows |
| Stage2 events | — | 774 events |
| Quarantined events | — | 20 |
| Community workbook | v12 | Footbag_Results_Community_FINAL_v12.xlsx |

---

End of contract.
