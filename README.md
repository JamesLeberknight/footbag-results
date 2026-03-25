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

## Pipeline Architecture

The pipeline has three distinct lanes. Each lane has its own runner and output directory.

### Lane 1 — Post-1997 Production

**Source: mirror only** (`mirror/www.footbag.org/`)

All authoritative post-1997 event data comes from the footbag.org HTML mirror. OLD_RESULTS, FBW magazine data, and magazine ingestion scripts are **not** part of this pipeline.

```
Stage 01   01_parse_mirror.py              parse HTML mirror → stage1_raw_events_mirror.csv
Stage 01c  01c_merge_stage1.py             merge stage-1 sources (mirror-only in production)
Stage 02   02_canonicalize_results.py      structured placements → stage2_canonical_events.csv
Stage 02p5 02p5_player_token_cleanup.py    apply identity lock (PT v47 / PBP v85)
Stage 02p6 02p6_structural_cleanup.py      artifact removal + structural fixes
Stage 03   03_build_excel.py               canonical Excel workbook
Stage 04   04_build_analytics.py           analytics + coverage flags + lock sentinel
Stage 01b1 01b1_merge_consecutives.py      [AUXILIARY] merge trick-record reference data
Stage 04B  tools/build_final_workbook_v13  community Excel workbook
Stage 05   05_export_canonical_csv.py      export out/canonical/*.csv  ← AUTHORITATIVE
Stage 05p5 05p5_remediate_canonical.py     final integrity pass
```

Runner: `./run_pipeline.sh [rebuild|release|qc|all]`

### Lane 2 — Pre-1997 Historical Recovery

**Sources: FBW magazine scans, OLD_RESULTS.txt, Gemini JSON extractions**

Completely isolated from the post-1997 pipeline. Outputs live in `early_data/`.

```
Stage 04  04_json_to_csv.py               Gemini JSON → event_blocks + placements
Stage 05  05_build_historical_dataset.py  cross-source grouping + OLD_RESULTS merge
Stage 06  06_identity_resolution.py       match raw names → Persons_Truth
Stage 07  07_build_early_release.py       V1 canonical CSVs + workbook
Stage 08  08_build_review_package.py      human review package
Stage 09  09_apply_decisions.py           apply review decisions → V2
Stage 10  10_build_early_comparison_feed  viewer feed files
Stage 11  11_finalize_pre1997.py          v1.0 release artifacts → early_data/final_pre1997/
Stage 12  12_build_enrichment_and_merged  person enrichment + canonical_all_union/
Stage 13  13_parse_passback_records.py    trick/record data (optional)
```

Runner: `./run_early_pipeline.sh [ingest|canonical|identity|release|review|apply|feed|finalize]`

### Lane 3 — Merged Build

**Requires: both post-1997 and pre-1997 pipelines complete**

Combines `out/canonical/` (post-1997) and `early_data/canonical/` (pre-1997) into a single 1980–present dataset.

```
tools/build_appsafe_merged.py      overlap suppression → out/canonical_all/
tools/build_merged_feeds.py        merged CSV feeds for workbook/viewer
tools/build_merged_workbook_v14.py merged Excel workbook (1980–present)
tools/event_comparison_viewerV10.py merged event comparison viewer
```

Runner: `./run_pipeline.sh merged`

Precondition: `out/canonical_all_union/` must exist (built by `early_data/scripts/12_build_enrichment_and_merged.py`).

### Pipeline Scripts — Lane Classification

| Script | Lane | Role |
|--------|------|------|
| `pipeline/01_parse_mirror.py` | Post-1997 | Parse mirror HTML |
| `pipeline/01c_merge_stage1.py` | Post-1997 | Merge stage-1 sources |
| `pipeline/02_canonicalize_results.py` | Post-1997 | Canonicalize placements |
| `pipeline/02p5_player_token_cleanup.py` | Post-1997 | Apply identity lock |
| `pipeline/02p6_structural_cleanup.py` | Post-1997 | Structural fixes |
| `pipeline/03_build_excel.py` | Post-1997 | Canonical workbook |
| `pipeline/04_build_analytics.py` | Post-1997 | Analytics + lock sentinel |
| `pipeline/05_export_canonical_csv.py` | Post-1997 | Export canonical CSVs |
| `pipeline/05p5_remediate_canonical.py` | Post-1997 | Final remediation |
| `pipeline/01b1_merge_consecutives.py` | **Auxiliary** | Trick-record reference data |
| `pipeline/01b_import_old_results.py` | **Pre-1997** | Convert OLD_RESULTS.txt |
| `pipeline/01b2_merge_FBW_Data.py` | **Pre-1997** | Convert FBW magazine CSV |
| `pipeline/01d_ingest_magazine_data.py` | **Pre-1997** | Magazine ingestion |

### Repository Layout

```
pipeline/               post-1997 production pipeline scripts
early_data/
  scripts/              pre-1997 reconstruction pipeline (stages 04–13)
  canonical/            pre-1997 canonical tables
  out/                  pre-1997 feeds + viewer outputs
tools/                  merged build + QC tools
out/
  canonical/            post-1997 authoritative CSVs  ← committed
  canonical_all/        merged canonical CSVs          ← committed
inputs/
  identity_lock/        immutable identity snapshots (PT v47 / PBP v85)
overrides/              person aliases, event metadata, known issues
legacy_data/            RESULTS_FILE_OVERRIDE source files (parser)
qc/                     QC module (qc_master.py + checks)
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
