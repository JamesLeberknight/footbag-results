cat > CLAUDE.md <<'EOF'
# CLAUDE.md
## Footbag Results Pipeline — Canonical Contract (v1.0)

This document defines the architectural contract and philosophical constraints
for the Footbag historical results pipeline.

It is authoritative for pipeline behavior.

---

# 1. Core Principles (Non-Negotiable)

1. Deterministic builds.
2. No silent merges.
3. No guessing without explicit tagging.
4. One real person = one canonical person record.
5. Garbage / non-person entities are preserved, not erased.
6. Human-verified identity truth overrides all heuristics.
7. Analytics depend ONLY on canonical identity outputs.

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

---

## 2.2 Canonical Identity Artifacts (Authoritative Inputs)

Release builds rely on:

inputs/identity_lock/
- Persons_Truth_Final_v16.csv
- Persons_Unresolved_Organized_v14.csv
- Placements_ByPerson_v16.csv

These files are treated as ground truth.

They must satisfy:

- Exactly one row per real human in Persons_Truth.
- No collisions in person_canon.
- All unresolved real humans appear in Persons_Unresolved.
- All garbage explicitly classified (__NON_PERSON__).
- No competitor dropped.

Identity truth is not recomputed in release mode.

---

# 3. Pipeline Modes

## 3.1 Release Mode (Canonical v1.0)

Purpose:
Produce the canonical, archival dataset and Excel workbook.

Characteristics:
- Consumes identity-lock artifacts.
- Does not perform identity merges.
- Does not generate alias suggestions.
- Deterministic from a clean clone.
- Produces:
  - Placements_Flat.csv
  - Persons_Truth.csv
  - Persons_Unresolved.csv
  - persons_truth.lock
  - Canonical Excel workbook

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
  - Generates Placements_Flat from authoritative placements.
  - No heuristic identity logic executed.

## Stage 03
- Build canonical workbook structure.
- No identity mutations.

## Stage 04
- Apply identity lock artifacts.
- Enforce coverage guarantees.
- Generate analytics.
- Write persons_truth.lock.
- Finalize sheet ordering.

Stage 04 must never alter canonical identity rows.

---

# 5. Coverage Guarantees

Every placement competitor must map to exactly one of:

- Persons_Truth
- Persons_Unresolved
- __NON_PERSON__

No row may be dropped silently.
All exclusions must be auditable.

---

# 6. Identity Lock Sentinel

Release builds generate:

out/persons_truth.lock

This file contains:
- sha256 hashes of authoritative inputs
- row counts
- filenames
- release timestamp

This sentinel proves identity immutability for the release.

---

# 7. Versioning Rules

- Patch (v1.0.x):
  - Documentation updates
  - Refactors
  - No data changes

- Minor (v1.x.0):
  - Additive analytics
  - No identity changes

- Major (v2.0.0):
  - Any change to:
    - Persons_Truth
    - Persons_Unresolved
    - Identity classification logic

Identity changes require a new release and full regeneration.

---

# 8. What This System Is Not

- Not a dynamic alias resolution engine.
- Not a speculative merge system.
- Not an automated identity inference pipeline.
- Not tolerant of silent data loss.

It is a controlled archival reconstruction of historical data.

---

# 9. Mental Model

Human truth > heuristics.

Identity is locked.
Ambiguity is preserved.
Noise is explicit.
Reproducibility is mandatory.

---

End of contract.
EOF
