# Pre-1997 Recovery — Output File Reference

**Last updated:** 2026-03-23

All paths are relative to `early_data/`. The pipeline preserves full provenance
and does not perform automatic deduplication or normalization.

---

## Key Counts at a Glance

```
Sources:          3 (FBW Magazine scans, IFAB History page, OLD_RESULTS.txt)
Source events:   45 → 37 canonical groups
Years:           17 (1980–1996)
Placements:     755 (all sources combined)
Participants:  1141 (teams expanded to individuals)
Persons:         97 (83 established + 14 new early players)

Validation: 8 CONFIRMED_MULTI_SOURCE, 29 SINGLE_SOURCE, 0 CONFLICT
Identity:   83 EXACT, 5 AUTOACCEPTED, 14 NEW_PLAYER, 12 REVIEW_NEEDED, 1 NOISE
```

---

## Canonical Outputs (FROZEN — do not edit manually)

### `canonical/events_pre1997.csv`
One row per canonical event group (year < 1997). **37 rows.**
Fields: canonical_event_id, event_name, year, location, normalized_event_type,
source_types, num_sources, validation_status, confidence, num_placements.

### `canonical/event_disciplines_pre1997.csv`
One row per unique (canonical_event_id, division_raw) pair. **308 rows.**
Preserves raw division names exactly as found in sources.

### `canonical/event_results_pre1997.csv`
All placement rows with canonical event IDs. One row per source placement
(multiple rows possible for same logical placement from different sources).
**755 rows.** Fields: result_id, canonical_event_id, division_raw, place,
player_raw, team_raw, source_event_id, source_type.

### `canonical/event_result_participants_pre1997.csv`
Teams expanded to individual participants. One row per individual per placement.
**1,141 rows.** Key field: resolution_status
(MATCHED / AUTOACCEPTED / NEW_PLAYER / REVIEW_NEEDED / NOISE / UNRESOLVED).

### `canonical/persons_pre1997.csv`
**97 rows** = 83 established PT persons referenced in pre-1997 data
+ 14 new early players (source_scope = PRE1997_ONLY).

### `canonical/person_aliases_pre1997.csv`
Full name→person_id mapping for all resolved pre-1997 names.

### `canonical/canonical_events.csv`
All 37 canonical event groups (pre-1997 only; no post-1997 in this file).

### `canonical/event_id_mapping.csv`
Maps each source event_id → canonical_event_id. **45 rows.**

### `canonical/event_groups.csv`
All 45 source events with group membership and confidence scores.

### `canonical/event_source_comparison.csv`
One row per canonical event group with validation_status and source list.

---

## Source-Level Data

### `event_blocks/event_blocks.csv`
35 in-scope FBW/IFAB events from Gemini extraction.
Schema: event_id, event_name_raw, year, date_raw, location_raw,
source_file, source_type, normalized_event_type, exclude_pre1997.

### `event_blocks/fbw_event_blocks.csv`
Original 04_json_to_csv.py output (before normalized_event_type was added).
Legacy file — prefer event_blocks.csv for new work.

### `event_blocks/fbw_event_blocks_out_of_scope.csv`
13 events with year ≥ 1997 found in Gemini extraction (preserved, not used).

### `placements/placements_flat.csv`
**496 rows** from Gemini AI extraction (FBW + IFAB).

### `placements/fbw_placements_flat.csv`
Legacy file — prefer placements_flat.csv.

### `old_results/old_results_event_blocks.csv`
**10 in-scope events** parsed from OLD_RESULTS.txt.

### `old_results/old_results_placements_flat.csv`
**259 placement rows** parsed from OLD_RESULTS.txt.

---

## Identity Resolution Files

### `identity/person_match_candidates.csv`
All **115 unique raw player names** with match results against Persons_Truth.
Match types: EXACT (83), NONE (32).

### `identity/unresolved_names.csv`
32 names with match_type = NONE, with near-miss suggestions for review.

### `identity/person_aliases_autoaccepted.csv`
5 safe aliases auto-accepted: Billy Hayne, Fred Kipley, Misty Helme,
Max Smith Jr., Gary Laut.

### `identity/person_aliases_needs_review.csv`
12 near-miss aliases requiring human decision. Source for review files.

### `identity/new_early_players.csv`
14 player names added as PRE1997_ONLY persons with stable generated IDs.

### `identity/unresolved_noise.csv`
1 entry: `unknown` (literal noise — not added to any person table).

---

## Review Files (Human Input Required)

### `review/person_alias_resolution.csv`
12 REVIEW_NEEDED aliases with context columns and blank DECISION field.
**Fill in DECISION:** ACCEPT | CREATE_NEW | REJECT | DEFER

### `review/review_aliases.xlsx`
Excel version of the alias review file with formatted layout.

### `review/event_group_resolution.csv`
37 canonical event groups with review questions and blank DECISION field.
**Fill in DECISION:** CORRECT | MERGE_WITH | SPLIT | NEEDS_DATA | DEFER

### `review/review_event_groups.xlsx`
Excel version of the event group review file.

---

## Spreadsheet Deliverables

### `out/footbag_results_pre1997_recovery.xlsx`
V1 standalone Excel workbook. **22 sheets:**
README · DATA NOTES · EVENT INDEX · PLAYER SUMMARY ·
1980–1996 (17 year sheets) · VALIDATION SUMMARY

### `out/footbag_results_pre1997_recovery_PRE1997_V2.xlsx`
V2 workbook with accepted review decisions applied.
12 person aliases resolved · 5 same-year event pairs merged.

---

## Comparison Viewer Feed Files

These files plug directly into the existing event comparison viewer
(`tools/event_comparison_viewerV10.py`) without touching the post-1997 release.

### `out/early_stage2_feed.csv`
**32 rows.** One row per canonical event. Schema mirrors `out/stage2_canonical_events.csv`.
Key field: `results_raw` — synthesized from raw source extractions (FBW/IFAB/OLD_RESULTS),
one block per source per division. Used as the viewer's left (source) column.

### `out/early_placements_feed.csv`
**752 rows.** One row per canonical placement. Schema mirrors `out/Placements_Flat.csv`.
Identity-resolved using V2 decisions. Singles: one row per player. Doubles: one team row
with `person_canon=__NON_PERSON__` and `team_display_name="P1 / P2"`.

### `out/event_comparison_viewer_pre1997.html`
Rendered comparison viewer for all 32 canonical pre-1997 events.
Generated by: `./run_pipeline.sh pre1997`
Viewer not generated by default — run explicitly when needed.

---

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/01_build_page_inventory.py` | Extract PPTX slide inventory |
| `scripts/02_image_qc_report.py` | Image QC for scanned pages |
| `scripts/03_rename_pages.py` | Rename/extract page images from PPTX |
| `scripts/04_json_to_csv.py` | Gemini batch JSON → event_blocks + placements |
| `scripts/05_build_historical_dataset.py` | Gemini + OLD_RESULTS → canonical grouping layer |
| `scripts/06_identity_resolution.py` | Match raw names against Persons_Truth |
| `scripts/07_build_early_release.py` | Apply identity policy, build canonical CSVs + Excel |
| `scripts/08_build_review_package.py` | Build human review CSV + Excel files |
| `scripts/09_apply_decisions.py` | Apply accepted review decisions → V2 canonical CSVs |
| `scripts/10_build_early_comparison_feed.py` | Build viewer-compatible comparison feed |

---

## Key Design Principles

- No guessing or inference on identities
- No automatic deduplication of placement evidence
- All raw names, division names, and source files preserved
- Conflicts are surfaced as review items, never silently resolved
- Pre-1997 outputs are isolated from post-1997 published dataset
