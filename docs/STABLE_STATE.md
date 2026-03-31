# Stable State — v17 (Pre-Manual-Results Fix)

Date: 2026-03-31

This file defines the current **locked, trusted state** of the canonical dataset prior to further ingestion work.

---

## Summary

The dataset is structurally sound, identity-consistent, and QC-passing.

Major improvements in this state:

- Identity resolution hardened (v3.2.0)
- ~400 previously unresolved participants resolved
- Canonical names enforced (no alias leakage)
- 1980–1985 early data validated against Andy’s authoritative results
- Structural cleanup applied:
  - duplicate placements removed
  - duplicate disciplines removed
  - cross-event contamination (1983 NHSA vs WFA) resolved
- Workbook rebuilt and aligned with canonical CSVs

QC Gate: **PASS (0 hard failures)**

---

## Canonical Guarantees

- persons.csv contains canonical identities only
- all stats derived from person_id (no name-based aggregation)
- no duplicate placements per (event_id, discipline, place)
- mirror-derived results preserved without loss
- no fabricated or inferred results

---

## Known Accepted Limitations

- Pre-1997 data remains incomplete by nature
- ~500 participants unresolved (valid unknowns, not artifacts)
- Some events contain partial coverage (top-N only)

---

## Important Note — 2016 Worlds

The 2016 World Championships (`2016_worlds_trnava`) appear sparse relative to adjacent years.

Root cause:
- Mirror page contains a **"Manually Entered Results" block**
- Current parser does NOT ingest this section
- Missing ~7–8 disciplines (not a data loss, but ingestion gap)

This is a **known limitation of the pipeline**, not a canonical data error.

Resolution is deferred.

---

## Deferred Work (NOT applied in this state)

- 2016 manual-results ingestion
- Similar audit for 2015 and 2018 Worlds
- 1984 / 1985 duplicate-event reconciliation
- Specific identity edge cases (Jodi, Doug Little, etc.)

---

## Definition of This Checkpoint

This state is considered:

- safe to publish (with documented limitations)
- safe to branch from
- protected against regression

Any future changes must:
- preserve QC PASS
- not violate canonical guarantees
