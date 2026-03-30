# Footbag Results Dataset

**Current release: v3.2.0**

A canonical, reproducible dataset of footbag competition results spanning **1980–present**, combining post-1997 mirror data with pre-1997 historical reconstruction.

---

## What This Dataset Contains

### Published Results — `out/release_publication/`

The authoritative publication-ready relational dataset, fully resolved and QC-validated:

| File | Rows | Description |
|------|------|-------------|
| `events.csv` | 812 | Published events (1980–2026) |
| `event_disciplines.csv` | 4,112 | Qualifying disciplines (≥3 placements) |
| `event_results.csv` | 24,932 | Placement rows |
| `event_result_participants.csv` | 35,237 | Participant rows |
| `persons.csv` | 3,490 | Canonically identified persons |

QC gate: **PASS** (0 hard failures)

### Published vs Documented: What the Counts Mean

841 events are documented in the archive. Not all appear in the published results:

| Category | Count | Reason |
|----------|-------|--------|
| **Published** | **812** | Have qualifying results (≥3 placements in ≥1 discipline) |
| No results | 30 | Posted on Footbag.org but no competitive results recorded |
| All-sparse | 39 | Every discipline has fewer than 3 placements |
| Pre-1997 sparse | 9 | Historical events with only 1–2 surviving placements |
| **Quarantined** | **9** | Structural ambiguity prevents deterministic parsing |
| **Total documented** | **841** | |

Excluded and quarantined events are listed in the workbook's **EXCLUDED EVENTS** sheet with reasons. Quarantined events appear in the **EVENT INDEX** highlighted in red.

> **Absence of an event from the published results does not mean it did not happen** — it means qualifying result data is not available.

---

## Pre-1997 Data: Explicit Disclaimer

Pre-1997 coverage is **incomplete and reconstructed from partial sources**.

| Era | Published Events | Character |
|-----|-----------------|-----------|
| 1980–1986 | 13 | Major Worlds and championship events only; top finishers only, rarely complete fields |
| 1987–1991 | 4 | Worlds plus select European/major regional events |
| 1992–1996 | 2 | One event per year (Worlds or major championship) |

- Sources: Footbag World magazine scans, `OLD_RESULTS.txt`, Footbag.org archive
- Pre-1997 events have year-level precision only — specific dates are not available
- All pre-1997 results are reconstructed; some placements are approximate or incomplete
- Expert review by Bruce Guettich (v1.0)

**Absence of a pre-1997 result does not mean it did not happen.** The sources are fragmentary. Career statistics for players active before 1997 are lower bounds only.

---

## Data Quality Signals

Every event and discipline carries quality metadata that appears in the EVENT INDEX sheet:

### `coverage_flag`
| Value | Meaning |
|-------|---------|
| `complete` | Full standings recorded |
| `partial` | Only top finishers recorded (e.g. top 3 or top 5) |
| `sparse` | Fewer than 3 placements — discipline excluded from publication |

### `status`
| Value | Meaning |
|-------|---------|
| `completed` | Post-1997 event with results |
| `historical` | Pre-1997 event, reconstructed |
| `no_results` | Event posted but no competitive results recorded |

### `data_source`
| Value | Meaning |
|-------|---------|
| `POST1997` | Sourced from Footbag.org mirror (primary, 1997–present) |
| `PRE1997` | Reconstructed from magazine/archive (secondary, pre-1997) |

---

## What "Canonical" Means

A result is **canonical** when:

1. It is traceable to a documented primary source (mirror HTML or magazine scan)
2. The event identity is unambiguous (no structural parsing failure)
3. Player identity is either resolved to a `person_id` or explicitly left unresolved
4. It has passed the QC gate (`tools/run_qc_gate.py`)

Where sources conflict (e.g. Footbag.org vs magazine), the more specific and verifiable source is used and documented in `overrides/`. Pre-1997 verified corrections are documented in the workbook's **CORRECTIONS** sheet.

---

## Records Dataset

The dataset includes two distinct types of competition data:

### 1. Event Placement Results (primary)
Results from sanctioned footbag competitions — who placed where at which event. This is the main dataset (812 events, 24,932 placements).

### 2. Trick-Specific Consecutive Records (separate)
World records for individual footbag tricks (e.g. highest consecutive count for a given move). Sourced from [Passback Footbag](https://www.passbackfootbag.com/records), not from competition events.

These appear as separate sheets in the community workbook:
- **RECORDS SUMMARY** — one row per trick, top holder
- **RECORDS LEADERBOARD** — full ranked leaderboard per trick
- **RECORDS REVIEW QUEUE** — records flagged for manual review

Trick records are independent of event placements and use a separate QC process.

---

## Community Workbook — `out/Footbag_Results_Community_v14_Canonical.xlsx`

Human-readable Excel workbook covering 1980–2026:

| Sheet | Contents |
|-------|----------|
| README | Dataset overview and sheet guide |
| DATA NOTES | Source quality notes, known limitations, quarantined events |
| EXCLUDED EVENTS | All 78 events not in published results, with reasons |
| CORRECTIONS | Verified historical corrections (1987/1995/1996 Worlds) |
| STATISTICS | Career podiums, wins, event counts, career spans |
| EVENT INDEX | All 841 documented events — green=published, gray=excluded, red=quarantined |
| PLAYER SUMMARY | Per-player wins, podiums, placements, events competed |
| CONSECUTIVE RECORDS | Competition-based consecutives world records |
| RECORDS SUMMARY | Trick-specific world records (from Passback) |
| RECORDS LEADERBOARD | Full trick leaderboard |
| RECORDS REVIEW QUEUE | Records under review |
| FREESTYLE INSIGHTS | Trick-sequence analytics (difficulty, transitions, innovation) |
| 1980–2026 | One sheet per year with all placement results |

---

## Source Tracks

### Post-1997 Mirror-Era (Primary)

- **Coverage:** 1997–present
- **Source:** Footbag.org mirror archive
- **Status:** Complete, identity-locked (PT v47 / PBP v85)
- **Published events:** 781

### Pre-1997 Historical Recovery

- **Coverage:** 1980–1996
- **Sources:** Footbag World magazine, `OLD_RESULTS.txt`, Footbag.org archive, expert review
- **Status:** v1.0 finalized (expert-reviewed by Bruce Guettich)
- **Published events:** 31

---

## Data Philosophy

- **No guessing** — unknown data stays unknown; unresolved names preserved as-is
- **Absence ≠ non-existence** — missing results reflect source limitations, not historical fact
- **Provenance-first** — every record traceable to a primary source
- **Deterministic outputs** — identical inputs produce identical outputs
- **Reproducibility over completeness** — partial accurate data beats fabricated completeness

---

## Event ID System

All events use a single slug-based identifier:

```
YYYY_event_city      # e.g. 2003_worlds_prague, 1986_worlds_golden
YYYY_event           # fallback when city unknown, e.g. 1993_worlds
```

Rules: lowercase, underscores only, city derived from location data.

### Worlds Mapping

| Era | Rule | Example |
|-----|------|---------|
| 1980–1982 | NHSA = authoritative worlds | `1981_worlds` |
| 1983 | Dual worlds (NHSA + WFA) | `1983_worlds_nhsa`, `1983_worlds_wfa` |
| 1984–1989 | WFA worlds (city known 1986–1989) | `1986_worlds_golden` |
| 1990–1996 | Single worlds per year | `1993_worlds` |
| 1997+ | Single worlds per year | `2003_worlds_prague` |

Note: the 1988 WFA Worlds entry was removed — cross-referencing confirmed the data was actually from 1987 (mislabeled in the source archive). The corrected 1987 entry is included.

---

## Pipeline Architecture

### Lane 1 — Post-1997 Production

```
Stage 01   01_parse_mirror.py              parse HTML mirror → stage1_raw_events_mirror.csv
Stage 02   02_canonicalize_results.py      structured placements → stage2_canonical_events.csv
Stage 02p5 02p5_player_token_cleanup.py    apply identity lock (PT v47 / PBP v85)
Stage 02p6 02p6_structural_cleanup.py      artifact removal + structural fixes
Stage 05   05_export_canonical_csv.py      export out/canonical/*.csv  ← AUTHORITATIVE
Stage 05p5 05p5_remediate_canonical.py     final integrity pass
```

Runner: `./run_pipeline.sh [rebuild|release|qc|all]`

### Lane 2 — Pre-1997 Historical Recovery

```
Stage 11  11_finalize_pre1997.py          v1.0 release artifacts → early_data/final_pre1997/
Stage 11b 11b_apply_verified_corrections.py  apply verified corrections
Stage 12  12_build_enrichment_and_merged.py  person enrichment + merged union
```

Runner: `./run_early_pipeline.sh [finalize|merge]`

### Lane 3 — Publication Build

```
tools/build_canonical_pf.py              filter + export → out/release_publication/
tools/build_workbook_v14.py              community Excel workbook
tools/inject_pre1997_persons.py          add PRE1997_ONLY persons to canonical/persons.csv
tools/run_qc_gate.py                     validate out/canonical/ — must PASS
```

---

## Repository Layout

```
pipeline/               post-1997 production pipeline scripts
early_data/
  scripts/              pre-1997 reconstruction pipeline (stages 04–13)
  final_pre1997/        pre-1997 canonical tables (v1.0 finalized)
  out/records/          trick-specific world records CSVs
tools/                  publication build + QC tools
out/
  canonical/            post-1997 authoritative CSVs (816 events)
  release_publication/  filtered publication CSVs (761 events)  ← PRIMARY OUTPUT
  canonical_all/        merged union (839 events, pre-filter)
inputs/
  review_quarantine_events.csv   9 quarantined events
overrides/              person aliases, event metadata, known issues
legacy_data/            RESULTS_FILE_OVERRIDE source files
```

---

## Identity Lock (Post-1997)

| File | Version | Rows |
|------|---------|------|
| `Persons_Truth_Final_v47.csv` | v47 | 3,468 |
| `Persons_Unresolved_Organized_v28.csv` | v28 | 82 |
| `Placements_ByPerson_v85.csv` | v85 | 27,980 |

Current Persons Truth: **3,493 rows** (includes 25 PRE1997_ONLY persons added post-lock).

---

## Status

- ✅ Publication dataset: `out/release_publication/` — 761 events, 1980–2026, QC PASS
- ✅ Community workbook: `Footbag_Results_Community_v14_Canonical.xlsx`
- ✅ Records dataset integrated (166 tricks, Passback source)
- ✅ Platform export compatible: `tools/export_platform_*.py` → `fb-bw/canonical_input/`
- ✅ Pre-1997 v1.0 finalized (expert-reviewed, verified corrections applied)
- ✅ Post-1997 dataset locked (PT v47 / PBP v85)
- ✅ Persons: 4,861 canonical persons (sentinels excluded from publication)

---

## License

Dataset: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
Pipeline code: MIT
