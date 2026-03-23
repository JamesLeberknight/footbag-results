# Changelog

All notable changes to the Footbag Results Dataset & Reconstruction Project are documented in this file.

This project follows a structured, versioned release approach for dataset outputs.

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
