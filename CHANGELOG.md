# Changelog

All notable changes to this project are documented in this file.

This project follows **semantic versioning**, with an additional rule:
**Any change to human identity truth requires a new version.**

---

## [v1.0.0] — Canonical Identity-Locked Release  
**Release date:** 2026-02-25

### 🚀 Overview
First **archive-quality canonical release** of the Footbag historical results dataset.

This release formalizes **human-verified identity resolution** as authoritative input
and introduces **Identity Lock Mode**, making the pipeline fully deterministic,
reproducible, and safe for public distribution and long-term preservation.

---

### 🔐 Identity Model (Breaking Change)
- Introduced **Identity Lock Mode**.
- Identity is no longer derived heuristically in release builds.
- Canonical identity is sourced exclusively from human-verified artifacts:
  - `Persons_Truth_Final_v13.csv`
  - `Persons_Unresolved_Organized_v11.csv`
  - `Placements_ByPerson_v13.csv`
- Exactly **one row per real person** in `Persons_Truth`.
- All unresolved humans preserved in `Persons_Unresolved`.
- All non-person / garbage entities explicitly classified (`__NON_PERSON__`).
- No speculative merges.
- No silent drops.

**This is a breaking conceptual change** relative to pre-v1.0 versions.

---

### 🧠 Pipeline Architecture
- Added explicit **Release Mode vs Rebuild Mode** distinction.
- Rebuild Mode:
  - Parses mirror data.
  - Produces candidates and audits only.
  - Does *not* reproduce canonical identity.
- Release Mode:
  - Consumes identity-lock inputs.
  - Produces final canonical dataset and workbook.
  - Deterministic from a clean clone.

---

### 🧩 Script-Level Changes
- **02p5_player_token_cleanup.py**
  - Added `--identity_lock_*` options.
  - Can now generate `Placements_Flat.csv` directly from authoritative placements.
  - Heuristic identity logic bypassed in release mode.

- **03_build_excel.py**
  - Uses identity-locked `Placements_Flat.csv`.
  - Produces presentation-safe canonical workbook.

- **04_build_analytics.py**
  - Accepts identity-lock inputs directly.
  - Enforces identity immutability.
  - Writes `persons_truth.lock` sentinel with hashes and row counts.
  - Analytics depend only on canonical identity outputs.

---

### 📊 Outputs
- Canonical CSVs (generated, not committed):
  - `Placements_Flat.csv`
  - `Persons_Truth.csv`
  - `Persons_Unresolved.csv`
- Final Excel workbook:
  - `Footbag_Results_Canonical.xlsx`
- Identity lock sentinel:
  - `out/persons_truth.lock`

---

### 🧪 Quality & Coverage Guarantees
- Every placement competitor maps to exactly one of:
  - `Persons_Truth`
  - `Persons_Unresolved`
  - `__NON_PERSON__`
- No row loss without audit.
- Deterministic ordering and reproducible builds.
- Identity coverage is enforced, not assumed.

---

### 📁 Repository Cleanup
- Introduced `inputs/identity_lock/` for authoritative human truth artifacts.
- Non-core helpers moved to:
  - `qc/`
  - `tools/`
  - `legacy/`
- Generated outputs and large static inputs excluded from git.
- README rewritten to reflect v1.0 contract and usage.

---

### 📜 Contract Status
- Original design contract preserved in `CLAUDE.md`.
- v1.0 release is a **faithful execution** of the contract’s core principles:
  - human truth is authoritative
  - no guessing
  - deterministic pipeline
  - full auditability

---

## Pre-v1.0 Versions (Historical)
Earlier versions (≤ v0.x / v1.0.x-persons-clean) represent **development and stabilization phases**:
- Identity partially heuristic
- Manual post-hoc corrections required
- Not suitable for archival or public canonical use

They are preserved for provenance only.

---

## Versioning Policy (Going Forward)

- **Patch** (v1.0.x):  
  Documentation, code cleanup, performance, no data changes.

- **Minor** (v1.x.0):  
  Additive data, new analytics, new sheets — **no identity changes**.

- **Major** (v2.0.0):  
  Any change to:
  - Persons_Truth
  - Persons_Unresolved
  - Identity classification rules

---

*End of changelog.*
