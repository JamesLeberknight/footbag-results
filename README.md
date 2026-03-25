# Footbag Results Dataset & Reconstruction Project

**Current release: v3.0.0**

## Overview

This repository contains a canonical, reproducible dataset of footbag competition results spanning **1980–present**, produced from two source tracks merged into a single authoritative output.

---

## Primary Deliverables

### 1. Merged Canonical Dataset — `out/canonical_all/`

The authoritative relational dataset combining PRE1997 reconstruction and POST1997 mirror data:

| File | Rows | Description |
|------|------|-------------|
| `events.csv` | 810 | All official events (1980–2025) |
| `event_disciplines.csv` | 4,136 | All disciplines |
| `event_results.csv` | 27,040 | All result rows |
| `event_result_participants.csv` | 35,189 | All participants |
| `persons.csv` | 3,482 | All persons |

- Unified slug-based `event_id` system (`YYYY_event_city`)
- `event_type = "worlds"` standardized across all 49 world championship events
- Cross-table referential integrity validated (0 orphans)
- QC gate: PASS

### 2. Merged Spreadsheet — `Footbag_Results_Merged_FINAL.xlsx`

Human-readable Excel workbook covering 1980–present:

- Year sheets for every year from 1980 to 2026
- Worlds events visually highlighted with banner row
- Incomplete events flagged in red
- Discipline hierarchy visually subordinated under event headers
- EVENT INDEX with hyperlinks to every year sheet
- PLAYER SUMMARY with BAP nickname
- STATISTICS section (dataset overview, worlds by year, placements by discipline)

### 3. Event Comparison Viewer — `out/merged_event_viewer.html`

Side-by-side QC tool comparing raw mirror source text against canonical placements:

- 810 events (exactly matching canonical_all)
- Single slug-based event_id throughout
- Search, filter by QC status, keyboard navigation
- Reason tags on suspicious rows (TRUNCATED, SURNAME_MISMATCH, etc.)

---

## Source Tracks

### Post-1997 Mirror-Era (Primary)

- **Coverage:** 1997–present
- **Source:** Footbag.org mirror
- **Status:** Complete, identity-locked (PT v47 / PBP v85)
- **Events:** 781 published

### Pre-1997 Historical Recovery

- **Coverage:** 1980–1996
- **Sources:** Footbag World magazine, `OLD_RESULTS.txt`, expert review
- **Status:** v1.0 finalized (29 events, expert-reviewed by Bruce Guettich)
- **Events:** 29 included in merged dataset

---

## Data Coverage

| Era | Events | Placements | Status |
|-----|--------|------------|--------|
| 1980–1996 | 29 | 731 | v1.0 finalized |
| 1997–2025 | 781 | 27,348 | Complete, locked |
| **Total** | **810** | **28,079** | **Published** |

---

## Data Philosophy

- **No guessing** — unknown data stays unknown
- **Provenance-first** — every record traceable to source
- **Deterministic outputs** — identical inputs produce identical outputs
- **Reproducibility over completeness** — partial accurate data beats fabricated completeness

---

## Event ID System

All events use a single slug-based identifier:

```
YYYY_event_city      # e.g. 2003_worlds_prague, 1986_worlds_golden
YYYY_event           # fallback when city unknown, e.g. 1993_worlds
```

Rules:
- Lowercase only
- Underscores only
- City derived from location data; omitted if unknown
- No legacy numeric IDs anywhere in outputs

### Worlds Mapping

| Era | Rule | Example |
|-----|------|---------|
| 1980–1982 | NHSA = authoritative worlds | `1981_worlds` |
| 1983 | Dual worlds (NHSA + WFA) | `1983_worlds_nhsa`, `1983_worlds_wfa` |
| 1984–1989 | WFA worlds (city known 1986–1989) | `1988_worlds_golden` |
| 1990–1996 | Single worlds per year | `1993_worlds` |
| 1997+ | Single worlds per year | `2003_worlds_prague` |

---

## Pipeline

```
pipeline/               # core pipeline (stages 01–05p5)
tools/
  build_appsafe_merged.py      # merge PRE1997 + POST1997 → canonical_all/
  build_merged_feeds.py        # produce merged CSV feeds
  build_merged_workbook_v14.py # build merged Excel workbook
  event_comparison_viewerV10.py# build QC HTML viewer
  cleanup_event_ids.py         # apply surgical event_id renames
  rename_worlds_event_ids.py   # standardize worlds slug conventions
  run_qc_gate.py               # authoritative QC gate
early_data/             # pre-1997 reconstruction pipeline
  scripts/              # 13 numbered recovery scripts
  canonical/            # pre-1997 canonical tables
  out/                  # pre-1997 feeds (early_stage2_feed, early_placements_feed)
out/
  canonical/            # post-1997 authoritative CSVs (committed)
  canonical_all/        # merged canonical CSVs (committed)
inputs/
  identity_lock/        # immutable identity snapshots (PT v47 / PBP v85)
overrides/              # person aliases, event metadata, known issues
legacy_data/            # parser override result files
```

### Rebuild Merged Outputs

After any canonical data change:

```bash
python3 tools/build_appsafe_merged.py        # merge PRE+POST1997 → canonical_all/
python3 tools/build_merged_feeds.py          # regenerate merged feeds
python3 tools/build_merged_workbook_v14.py   # rebuild spreadsheet
python3 tools/event_comparison_viewerV10.py  # rebuild viewer
```

---

## Repository Notes

- `mirror/` — not committed; distribute as GitHub Release asset
- `out/canonical_all_union/` — build artifact only; not committed
- `*.xlsx` — generated outputs; not committed
- `out/merged_*.csv` — generated feeds; not committed

---

## Identity Lock (Post-1997)

| File | Version | Rows |
|------|---------|------|
| `Persons_Truth_Final_v47.csv` | v47 | 3,468 |
| `Persons_Unresolved_Organized_v28.csv` | v28 | 82 |
| `Placements_ByPerson_v85.csv` | v85 | 27,980 |

---

## Status

- ✅ Merged canonical dataset published (`out/canonical_all/`) — 810 events, 1980–2025
- ✅ Merged spreadsheet built (`Footbag_Results_Merged_FINAL.xlsx`)
- ✅ Event comparison viewer updated (810 events, slug IDs)
- ✅ Unified slug event_id system — no legacy numeric IDs
- ✅ Worlds classification: `event_type = "worlds"` for all 49 world championship events
- ✅ Pre-1997 v1.0 finalized (29 events, expert-reviewed)
- ✅ Post-1997 dataset locked (PT v47 / PBP v85)

---

## License

Dataset: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
Pipeline code: MIT
