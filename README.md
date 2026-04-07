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
- **Status:** Complete, identity-locked (PT v51 / PBP v96)
- **Canonical events:** ~776

### Pre-1997 Historical Recovery

- **Coverage:** 1980–1996
- **Sources:** Footbag World magazine (structured CSVs), `authoritative-results-1980-1985.txt`, Worlds TXT files (1985–1997), curated adapter
- **Status:** Migration complete (2026-04-06) — all digitized sources absorbed into canonical
- **Canonical events:** ~54

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
Stage 01   pipeline/adapters/mirror_results_adapter.py    parse HTML mirror → stage1_raw_events_mirror.csv
Stage 01c  pipeline/adapters/curated_events_adapter.py   curated structured CSVs → stage1_raw_events_curated.csv
Stage 02   pipeline/02_canonicalize_results.py           structured placements → stage2_canonical_events.csv
Stage 02p5 pipeline/02p5_player_token_cleanup.py         apply identity lock (PT v51 / PBP v96)
Stage 02p6 pipeline/02p6_structural_cleanup.py           artifact removal + structural fixes
Stage 05   pipeline/historical/export_historical_csvs.py export out/canonical/*.csv  ← AUTHORITATIVE
Stage 05p5 pipeline/05p5_remediate_canonical.py          final integrity + event merge pass
```

Runner: `./run_pipeline.sh [rebuild|release|qc|all]`

### Lane 2 — Pre-1997 Historical Recovery

Pre-1997 sources are now ingested through the curated adapter (Lane 1) and merged into `out/canonical/` alongside mirror data. The early pipeline (`run_early_pipeline.sh`) produces the merged `out/canonical_all/` dataset combining all eras.

```
inputs/curated/events/structured/   structured CSVs (FBW + magazine + worlds)
pipeline/adapters/curated_events_adapter.py  → stage1_raw_events_curated.csv
early_data/scripts/12_build_enrichment_and_merged.py  → out/canonical_all/
```

Runner: `./run_early_pipeline.sh [finalize|merge]`

### Lane 3 — Publication Build

```
tools/build_canonical_enrichment.py      enrich + filter → out/canonical_all/
tools/export_platform_canonical.py       platform export → out/platform_release/
pipeline/qc/run_qc.py                    validate out/canonical/ — must PASS
```

---

## Repository Layout

```
pipeline/               production pipeline (adapters, historical, qc, platform)
  adapters/             mirror + curated event ingestion
  historical/           canonicalization + export
  qc/                   QC gate
early_data/             pre-1997 reconstruction artifacts (gemini, review)
inputs/
  curated/events/structured/   35 structured CSVs (FBW + magazine + worlds)
  identity_lock/        Persons_Truth + Placements_ByPerson lock files
tools/                  enrichment, platform export, QC analysis
overrides/              person aliases, event metadata, source row exclusions
legacy_data/            RESULTS_FILE_OVERRIDE source files
out/
  canonical/            authoritative CSVs (830 events, 1980–2026)  ← PRIMARY OUTPUT
  canonical_all/        merged union including pre-1997 enrichment
```

---

## Identity Lock (Post-1997)

| File | Version | Rows |
|------|---------|------|
| `Persons_Truth_Final_v51.csv` | v51 | 3,396 |
| `Persons_Unresolved_Organized_v28.csv` | v28 | 82 |
| `Placements_ByPerson_v96.csv` | v96 | — |

---

## Status

- ✅ Canonical dataset: `out/canonical/` — 830 events, 1980–2026, QC PASS
- ✅ Identity locked: PT v51 / PBP v96
- ✅ Platform export: `tools/export_platform_canonical.py` → `out/platform_release/`
- ✅ Pre-1997 migration complete (2026-04-06): all digitized sources in canonical via curated adapter
  - 19 FBW structured CSVs + 15 magazine structured CSVs + worlds TXT files (1985–1997)
  - `magazine.csv` dependency retired (`2001980002` promoted to `magazine_1980_worlds_memphis.csv`)
  - No further FBW batch work possible without fresh transcription
- ✅ Persons: 3,396 canonical (PT v51)

---

## License

Dataset: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
Pipeline code: MIT

## Known Limitations

Some events may appear incomplete due to known parsing limitations in the source data.

In particular:
- European name formats ("Last, First") may be misinterpreted in some historical results
- Certain freestyle events (e.g., Circle Contest, Request Contest) may have partial placement coverage due to pool/finals deduplication logic
- Some divisions (e.g., "Big 1") may be omitted if not recognized by the parser

These cases are documented and prioritized for future improvement, but do not affect the structural integrity of the dataset.
