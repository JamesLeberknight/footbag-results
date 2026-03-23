# Early Data Pipeline Outputs (Pre-1997 Reconstruction)

This directory contains the structured outputs from the historical reconstruction pipeline using:

- FBW (Footbag World) scanned images
- IFAB Worlds history pages
- OLD_RESULTS.txt (text-based historical records)

The pipeline preserves full provenance and does not perform automatic deduplication or normalization.

---

## 1. Raw Structured Outputs

### `event_blocks/event_blocks.csv`

One row per extracted event.

Fields:
- event_id
- event_name_raw
- date_raw
- year
- location_raw
- source_file
- source_type (FBW / IFAB)
- normalized_event_type
- exclude_pre1997

---

### `placements/placements_flat.csv`

One row per placement.

Fields:
- event_id
- division_raw
- placement_raw
- placement_num
- player_raw
- team_raw
- score_raw
- notes
- source_file
- source_type

---

## 2. OLD_RESULTS Source Layer

### `old_results/old_results_event_blocks.csv`

Structured events parsed from OLD_RESULTS.txt.

### `old_results/old_results_placements_flat.csv`

Structured placements parsed from OLD_RESULTS.txt.

---

## 3. Canonical Event Identity Layer

### `canonical/event_groups.csv`

Candidate event groupings based on:
- normalized_event_type
- year

Fields:
- group_id
- normalized_event_type
- year
- candidate_event_ids
- source_types
- confidence

---

### `canonical/canonical_events.csv`

Canonical event identities.

Fields:
- canonical_event_id
- normalized_event_type
- year
- source_count
- source_types

---

### `canonical/event_id_mapping.csv`

Mapping from raw events to canonical events.

Fields:
- event_id
- canonical_event_id
- source_file
- source_type

---

## 4. Validation Layer

### `canonical/event_source_comparison.csv`

Cross-source comparison of placements.

Fields:
- canonical_event_id
- division_raw
- placement_raw
- placement_num
- player_raw
- team_raw
- score_raw
- source_type
- source_file
- validation_status

Validation statuses:
- CONFIRMED_MULTI_SOURCE → appears identically in ≥2 sources
- SINGLE_SOURCE → appears in only one source
- CONFLICT → disagreement between sources

---

## Key Design Principles

- No guessing or inference
- No automatic deduplication
- All sources preserved independently
- Canonical layer is reversible
- Conflicts are surfaced, not resolved

---

## Known Limitations

- Some events are partial or fragmented across sources
- Naming inconsistencies exist (WFA / IFAB / World Championships)
- Some player names may include location text
- Post-1997 data may appear in FBW and is flagged but not removed at raw level

---

## Next Step

Person Identity Resolution:
- Map `player_raw` / `team_raw` to Persons_Truth
- Generate alias candidates
- Preserve unresolved identities
