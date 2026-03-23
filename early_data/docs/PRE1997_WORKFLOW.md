# Pre-1997 Historical Recovery — Workflow Guide

**Last updated:** 2026-03-23

---

## Overview

The pre-1997 pipeline is a **separate, isolated data recovery track**. It produces
its own canonical outputs and viewer-compatible comparison feeds without touching the
post-1997 published release (`out/canonical/`, `out/Placements_Flat.csv`, etc.).

---

## Data Flow

```
Sources                         Scripts                     Outputs
─────────────────────────────────────────────────────────────────────
FBW magazine scans (Gemini)  ─┐
IFAB history page (Gemini)   ─┼─ 04_json_to_csv.py ──────▶ event_blocks/placements_flat.csv
OLD_RESULTS.txt              ─┘  05_build_historical…py ──▶ canonical/events_pre1997.csv
                                                            canonical/event_id_mapping.csv
                                                            …

                               06_identity_resolution.py ──▶ identity/ (match candidates)

                               07_build_early_release.py ──▶ canonical/*.csv (V1 frozen)
                                                             out/footbag_results_pre1997_recovery.xlsx

                               09_apply_decisions.py ──────▶ canonical/*.csv (V2 with accepted aliases)
                                                             out/footbag_results_pre1997_recovery_PRE1997_V2.xlsx

                               10_build_early_comparison  ──▶ out/early_stage2_feed.csv
                                  _feed.py                    out/early_placements_feed.csv

tools/event_comparison         (unchanged viewer) ──────────▶ out/event_comparison_viewer_pre1997.html
viewerV10.py --stage2 … --pf …
```

---

## Running the Pre-1997 Comparison Viewer

```bash
./run_pipeline.sh pre1997
```

This runs two steps:
1. `early_data/scripts/10_build_early_comparison_feed.py` — builds viewer-compatible feed files
2. `tools/event_comparison_viewerV10.py --stage2 … --pf … --output …` — renders HTML

Output: `out/event_comparison_viewer_pre1997.html`

To run steps individually:

```bash
# Step 1: build feed files
python early_data/scripts/10_build_early_comparison_feed.py

# Step 2: render viewer
python tools/event_comparison_viewerV10.py \
    --stage2 early_data/out/early_stage2_feed.csv \
    --pf     early_data/out/early_placements_feed.csv \
    --output out/event_comparison_viewer_pre1997.html
```

---

## What the Comparison Viewer Shows

| Left column (source) | Right column (canonical) |
|---|---|
| Raw placements from FBW/IFAB/OLD_RESULTS | Identity-resolved canonical placements |
| Prefixed by source: `[FBW p012.jpg]` | Resolved names from Persons_Truth |
| Raw division names as extracted | Raw division names (no forced normalization) |
| Multi-source events show all sources | Same canonical entry covers all sources |

For multi-source events (e.g., 1983 NHSA confirmed by both FBW and OLD_RESULTS), the
`results_raw` text contains sections for each source. The viewer matches each division
block to the canonical side independently.

### Status colors

- **Green** — all placements align between source and canonical
- **Yellow** — minor gaps (missing rows on one side only)
- **Red** — suspicious matches, surname mismatches, or unmatched divisions

Suspicious matches are expected for pre-1997 data where:
- Raw names differ from resolved canonical names (e.g., "Kenny Shults" vs "Kenneth Shults")
- Division names are abbreviated (e.g., "Open Sgls Net" vs "Open Singles Net")

---

## Viewer Feed File Design

### `early_data/out/early_stage2_feed.csv`

Schema matches `out/stage2_canonical_events.csv` exactly:

| Field | Source |
|---|---|
| `event_id` | `canonical_event_id` from events_pre1997.csv |
| `year` | year from events_pre1997.csv |
| `event_name` | event_name from events_pre1997.csv |
| `results_raw` | Synthesized from raw source placements (FBW + IFAB + OLD_RESULTS) |
| `placements_json` | Stub with validation_status metadata |

The `results_raw` text is formatted as numbered placement lines under division headers,
prefixed with source type (`[FBW p012.jpg]`, `[OLD_RESULTS]`, etc.).

### `early_data/out/early_placements_feed.csv`

Schema matches `out/Placements_Flat.csv` exactly:

| Field | Source |
|---|---|
| `event_id` | `canonical_event_id` |
| `division_canon` | `division_raw` (raw — viewer fuzzy-matches) |
| `place` | resolved place number |
| `person_canon` | Identity-resolved name, or `__NON_PERSON__` for teams |
| `team_display_name` | "Player1 / Player2" for doubles placements |
| `person_id` | UUID from identity resolution (or blank if unresolved) |
| `coverage_flag` | `complete` if CONFIRMED_MULTI_SOURCE, else `partial` |
| `person_unresolved` | `"1"` if not MATCHED / ACCEPTED / NEW_PLAYER |

---

## Key Isolation Properties

The pre-1997 pipeline **never touches**:
- `out/canonical/*.csv` (post-1997 published canonical CSVs)
- `out/Placements_Flat.csv` (post-1997 identity-locked placements)
- `inputs/Persons_Truth.csv` (post-1997 identity master)
- `out/event_comparison_viewer.html` (post-1997 comparison viewer)

The `--stage2` and `--pf` flags added to the viewer are purely additive. Running the
viewer without flags still works exactly as before on post-1997 data.

---

## Script Sequence Reference

| Script | Purpose | Inputs | Outputs |
|---|---|---|---|
| `04_json_to_csv.py` | Gemini batch JSON → event_blocks + placements | Gemini JSON | event_blocks/, placements/ |
| `05_build_historical_dataset.py` | Merge + canonical grouping | event_blocks/, old_results/ | canonical/ (V1 frozen) |
| `06_identity_resolution.py` | Match raw names against PT | canonical/, Persons_Truth.csv | identity/ |
| `07_build_early_release.py` | Apply policy, build V1 outputs | canonical/, identity/ | canonical/*.csv, out/*.xlsx |
| `08_build_review_package.py` | Build human review files | canonical/, identity/ | review/*.csv, review/*.xlsx |
| `09_apply_decisions.py` | Apply accepted review decisions | canonical/, review/ | canonical/*.csv (V2), out/*.xlsx |
| `10_build_early_comparison_feed.py` | Build viewer-compatible feeds | canonical/, placements/, old_results/ | out/early_*.csv |

---

## Promoting to the Main Dataset (Not Yet)

Pre-1997 data is NOT yet integrated into the post-1997 pipeline. Integration requires:

1. All review decisions recorded (alias resolution + event grouping)
2. QC pass on early canonical CSVs
3. Explicit decision to merge pre/post-1997 into one public dataset
4. New PT entries for PRE1997_ONLY persons

See `NEXT_STEPS.md` for current status.
