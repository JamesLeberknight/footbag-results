# Project Rules — Canonical Dataset (Non-Negotiable)

These rules define the system. All pipeline changes must respect them.

---

## 1. Canonical Source of Truth

Canonical CSVs are authoritative:

- events.csv
- event_disciplines.csv
- event_results.csv
- event_result_participants.csv
- persons.csv

The workbook is derived and must not introduce changes.

---

## 2. Identity Integrity

- All stats must use person_id
- persons.csv contains canonical names only
- aliases must not appear as separate persons
- no duplicate identities allowed

---

## 3. Results Integrity

- No fabricated results
- No inferred placements
- No dropping of valid competitors
- Each placement must be preserved exactly

---

## 4. Mirror Fidelity

Mirror data is authoritative.

Allowed:
- missing data (if not present in mirror)

Not allowed:
- loss of mirror placements
- truncated divisions
- incorrect parsing

---

## 5. Structural Rules

- one real-world event = one event_id
- no duplicate placements
- no duplicate disciplines
- correct participant cardinality:
  - singles = 1
  - doubles = 2

---

## 6. Division Canonicalization

- division_canon must be fully normalized
- no abbreviations (Sgls, Dbls, Dobles)
- consistent naming across dataset

---

## 7. QC Contract

A dataset is valid ONLY if:

QC_STATUS = PASS

No exceptions for release.

---

## 8. Unknown Handling

- unknown participants allowed
- must not create fake identities
- must not affect statistics

---

## 9. Workbook Rules

- no hidden rows, columns, or sheets
- Column A must always be visible
- no data mutation in workbook

---

## 10. Change Discipline

All changes must follow:

analyze → classify (safe/high/review) → apply → QC → document

No direct edits without classification.

---

## 11. Documentation Requirement

All non-trivial issues must be:

- documented in OPEN_ISSUES.md
- or resolved and logged in CHANGELOG.md

---

## 12. No Regression Policy

Any change that introduces:

- duplicate persons
- alias leakage
- missing mirror data
- QC failure

is invalid and must be reverted.
