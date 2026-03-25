# Changelog

All notable changes to the Footbag Results Dataset & Reconstruction Project are documented in this file.

This project follows a structured, versioned release approach for dataset outputs.

---

## [v3.0.0] — 2026-03-25 — Merged Canonical Dataset Published

### Summary

First release combining PRE1997 historical reconstruction and POST1997 mirror data into a single unified canonical dataset covering **1980–present**.

### Dataset

- **Events:** 810 (29 PRE1997 + 781 POST1997)
- **Placements:** 28,079 (731 PRE1997 + 27,348 POST1997)
- **Persons:** 3,482
- **Worlds events:** 49 (classified with `event_type = "worlds"`)
- **Identity lock:** PT v47 / PBP v85 (post-1997, unchanged)

### New: Merged Canonical Dataset — `out/canonical_all/`

- Combines PRE1997 reconstruction (early_data/) with POST1997 mirror pipeline
- Overlap-filtered: POST1997 mirror stubs for years with PRE1997 coverage are suppressed
- Unified slug-based `event_id` system across all 810 events
- Zero orphan references across all four relational tables

### New: Merged Workbook — `Footbag_Results_Merged_FINAL.xlsx`

- Year sheets 1980–2026 (47 sheets)
- Worlds events: bold banner with `🌐 WORLD CHAMPIONSHIPS —` prefix, amber highlight
- Discipline category rows (NET / FREESTYLE / GOLF) styled as subordinate headers
- Incomplete events (≤2 placements): red row, status message
- STATISTICS sheet: dataset overview, worlds by year, placements by discipline category
- EVENT INDEX: slug-based event_id, worlds classified, quarantined/incomplete flagged
- PLAYER SUMMARY: alphabetical sort, BAP nickname column

### New: Merged Event Comparison Viewer — `out/merged_event_viewer.html`

- 810 events, exactly matching canonical_all
- Single slug-based event_id (no dual numeric/slug system)
- Scan pane deferred cleanly (layout simplified to 2-column)
- Default input paths point to merged feeds

### Event ID System

- All event IDs follow `YYYY_event_city` or `YYYY_event` (city-unknown fallback)
- No legacy numeric IDs in any user-facing output

### Worlds Event Classification

- All 22 PRE1997 worlds events updated: `event_type = "worlds"` (was NHSA_NATIONALS, WORLD_CHAMPIONSHIPS, WFA_WORLD_CHAMPIONSHIPS, IFAB_WORLD_CHAMPIONSHIPS)
- `_is_worlds()` in workbook builder simplified: checks `event_type == "worlds"` only
- Applied across: canonical_all/, canonical_all_union/ (PRE1997 rows), early_stage2_feed.csv

### Historical Worlds Slug Convention

Applied via `rename_worlds_event_ids.py`:

| Old slug | New slug | Rule |
|----------|----------|------|
| `1980_nhsa` | `1980_worlds` | NHSA = authoritative worlds |
| `1980_worlds` | `1980_worlds_clackamas` | Displaced generic → city qualifier |
| `1981_nhsa` | `1981_worlds` | NHSA = authoritative worlds |
| `1982_nhsa` | `1982_worlds` | NHSA = authoritative worlds |
| `1983_nhsa` | `1983_worlds_nhsa` | Dual worlds year |
| `1983_wfa` | `1983_worlds_wfa` | Dual worlds year |
| `1986_worlds_wfa` | `1986_worlds_golden` | WFA worlds with city |
| `1990_worlds_wfa` | `1990_worlds` | WFA worlds, no city |
| `1993_worlds_ifab` | `1993_worlds` | IFAB worlds, no city |
| `1994_worlds_ifab` | `1994_worlds_palo_alto` | IFAB worlds with city |

### Bug Fixes

- `build_appsafe_merged.py`: suppression filter was slug-only — PRE1997 events sharing a slug with a suppressed POST1997 event were incorrectly dropped (5 events: 1981_worlds_portland, 1982_worlds_portland, 1986/87/89_worlds_golden). Fixed to suppress POST1997-source rows only.
- `build_merged_feeds.py`: stage2 dedup — POST1997 suppressed events were passing through the canonical_all filter when a PRE1997 event with the same slug existed. Fixed with direct `suppressed_canon_ids` check.
- `event_comparison_viewerV10.py`: removed dual numeric/slug ID system; viewer now uses slug as sole event identifier.

### New Tools

- `tools/build_appsafe_merged.py` — merge PRE+POST1997, overlap suppression, canonical_all output
- `tools/build_merged_feeds.py` — generate merged_events_normalized, merged_placements_flat, merged_stage2
- `tools/build_merged_workbook_v14.py` — merged Excel workbook builder
- `tools/cleanup_event_ids.py` — surgical event_id rename pass
- `tools/rename_worlds_event_ids.py` — worlds slug standardization
- `tools/assign_pre1997_event_slugs.py` — PRE1997 slug assignment
- `tools/apply_pre1997_fixes.py` — PRE1997 data corrections

---

## [v2.15.0] — Post-1997 Dataset Finalized

### State

- **Identity lock:** Persons_Truth v47 (3,468 persons), Placements_ByPerson v85 (27,980 rows)
- **Events:** 814 published events (1997–present)
- **Canonical CSVs:** events.csv (814), event_disciplines.csv (4,117), event_results.csv (28,034), event_result_participants.csv (35,933), persons.csv (3,468)
- **QC gate:** PASS — 0 hard fails

### Included

- Post-1997 mirror-era dataset finalized and published
- Community spreadsheet (`Footbag_Results_Community_FINAL_v13.xlsx`)
- Canonical CSV dataset (`out/canonical/`)
- HTML event comparison viewer for event-level validation
- Full identity resolution via immutable lock (PT v47 / PBP v85)
- 9 quarantined events excluded from canonical outputs (documented)

### Notes

- Pre-1997 historical recovery is a separate ongoing effort, not included in this release
- Mirror source (`mirror.tar.gz`) distributed as a GitHub Release asset

---

## [post1997-v1.0] — Initial Stable Release

### Added

- **Post-1997 Mirror-Era Dataset (1997–present)**
  - High-confidence dataset derived from Footbag.org mirror data
  - Fully reproducible pipeline output
  - Clean separation from legacy pre-1997 data

- **Community Spreadsheet (Primary Deliverable)**
  - Year-by-year event sheets (1997–present)
  - Player Summary with Member ID and BAP Nickname
  - Statistics sheet (subset-derived)
  - Freestyle Insights sheet (subset-derived)
  - Event Index sheet
  - Worlds events identified and highlighted

- **Canonical CSV Dataset**
  - `events.csv`
  - `event_results.csv`
  - `event_result_participants.csv`
  - `persons.csv`
  - `event_disciplines.csv`
  - Deterministic, normalized schema

- **HTML Event Comparison Viewer**
  - Side-by-side comparison of source vs canonical data
  - Mismatch detection and QC support
  - Used for event-level validation and debugging

---

### Changed

- **Repository Structure Reorganized**
  - Clear separation between:
    - post-1997 published dataset
    - pre-1997 recovery project
  - Introduction of `early_data/` for historical reconstruction work

- **README Updated**
  - Post-1997 dataset defined as primary release
  - Pre-1997 work explicitly marked as incomplete and ongoing
  - Event comparison viewer recognized as a formal artifact

- **Event Type Normalization**
  - Removed misuse of discipline labels (e.g., NET, FREESTYLE) as event types
  - Introduced deterministic identification of "Worlds" events

- **Player Summary Schema**
  - Removed:
    - First Year
    - Last Year
    - Data Confidence
  - Added:
    - Member ID
    - BAP Nickname

- **Year Sheets Improved**
  - Added Event ID for traceability and QC alignment
  - Standardized structure and ordering
  - Worlds events visually highlighted

---

### Removed

- All pre-1997 data from the published dataset
- All Footbag World (FBW)-derived results from release outputs
- Legacy assumptions of full historical completeness

---

### Notes

- This release represents a **high-confidence subset**, not a complete historical archive
- Coverage is intentionally limited to 1997–present
- Pre-1997 data is being reconstructed separately and is not included in this release

---

## [Unreleased]

### Planned

- Pre-1997 historical dataset (partial, provenance-driven)
- FBW image extraction pipeline improvements
- Enhanced identity resolution (alias handling)
- Additional QC automation
- Potential event tier classification (Worlds / Major / Regional)

---
