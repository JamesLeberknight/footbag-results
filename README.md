# Footbag Results Pipeline — Canonical Excel Builder (v1.0.21)

Deterministic pipeline for producing the **final, reviewer-ready Excel workbook** of historical Footbag results.

This repository reconstructs structured event and placement data from:

- An offline HTML mirror of Footbag.org
- Legacy historical result files
- A curated identity lock (`Persons_Truth`)

The primary deliverable is the final Excel spreadsheet.

---

## Primary Goal

The purpose of this repository is to build a clean, internally consistent, archival-quality Excel workbook of Footbag results suitable for:

- Historical preservation
- Statistical analysis
- Community sharing
- Reviewer audit

All parsing, normalization, analytics, and QC exist to support this final spreadsheet.

### Data Completeness

Results data is **incomplete for early years**. The primary source (Footbag.org mirror) provides dense coverage from 1997 onward. Pre-1997 data is sparse: only a handful of events are recorded for 1985–1986 and 1990–1991, and the years 1987–1989 and 1992–1996 have no coverage at all. Historical records recovery from print sources (e.g. Footbag World magazine) is ongoing.

---

## Identity Model (Authoritative Core)

Identity is the foundation of the dataset.

All person identity used in analytics and the workbook derives exclusively from:

```
inputs/identity_lock/Persons_Truth_Final_v31.csv
```

### Persons_Truth

`Persons_Truth_Final_v31.csv` enforces:

- One row per real person (3,444 persons)
- Globally unique `effective_person_id` (UUID)
- Unique canonical display name (`person_canon`)
- Human-verified identity resolution
- Collision-free canonicalization

Release mode **does not infer identity**.
It only applies the identity lock.

If `Persons_Truth` changes, the identity of the dataset changes.
This should be treated as a **major version event**.

---

## Repository Structure

```text
/
├─ Makefile
├─ inputs/
│  └─ identity_lock/
│     ├─ Persons_Truth_Final_v31.csv
│     ├─ Persons_Unresolved_Organized_v27.csv
│     └─ Placements_ByPerson_v33.csv
├─ scripts/
│  ├─ 01_parse_mirror.py
│  ├─ 01b_import_old_results.py
│  ├─ 01c_merge_stage1.py
│  ├─ 02_canonicalize_results.py
│  ├─ 02p5_player_token_cleanup.py
│  ├─ 03_build_excel.py
│  └─ 04_build_analytics.py
├─ qc/
│  └─ qc_master.py
├─ tools/
│  ├─ 32_post_release_qc.py
│  └─ 33_schema_logic_qc.py
├─ overrides/
└─ out/                          (generated outputs — not source of truth)
```

The only authoritative identity artifact is `Persons_Truth_Final_v31.csv`.

---

## Pipeline Overview

### Stage 01 — Parse Mirror

```
scripts/01_parse_mirror.py
```

Extracts raw structured placement data from the HTML mirror.

### Stage 01b — Import Legacy Results

```
scripts/01b_import_old_results.py
```

Imports historical results not present in the mirror.

### Stage 01c — Merge Stage 1 Sources

```
scripts/01c_merge_stage1.py
```

Unifies mirror and legacy inputs into a single structured dataset.

### Stage 02 — Canonicalize Results

```
scripts/02_canonicalize_results.py
```

Normalizes event metadata, division names, and placement structure.
Produces canonical stage-2 tables.

### Stage 02p5 — Player Token Cleanup

```
scripts/02p5_player_token_cleanup.py
```

Release-mode bridge between stage-2 canonical events and the identity lock.

- Applies `Placements_ByPerson_v33.csv` (identity-locked placements) to produce `Placements_Flat.csv`
- Enforces coverage guarantees: every placement maps to Persons_Truth, Persons_Unresolved, or `__NON_PERSON__`
- Does **not** perform identity merges or heuristic resolution

Requires `out/stage2_canonical_events.csv` (produced by stage 02).

### Stage 03 — Build Excel Workbook (Primary Output)

```
scripts/03_build_excel.py
```

Builds the final Excel spreadsheet from canonical stage-2 data.
Applies canonical location strings from `inputs/location_canon_full_final.csv`.

### Stage 04 — Build Analytics

```
scripts/04_build_analytics.py
```

Generates analytics surfaces written into the workbook:
summary sheets, person stats, coverage analysis, and data integrity.

Identity used here is strictly derived from `Persons_Truth`.
Writes `out/persons_truth.lock` on Gate 3 PASS.

---

## Running the Pipeline

### Prerequisites

1. **Python 3.12+** and `make`

2. **HTML mirror** — the Footbag.org mirror is not stored in this repo (it's large).
   Obtain it separately and extract it so the repo root contains a `mirror/` directory:
   ```
   mirror/
   └─ ...html files...
   ```

3. **`inputs/OLD_RESULTS.txt`** is included in this repo. It contains three historical
   events that predate the mirror and are required for a complete dataset. The rebuild
   pipeline processes it automatically via `scripts/01b_import_old_results.py`.

### Commands

```bash
# First-time setup: create venv, install dependencies
make setup

# Full pipeline (rebuild → release → qc)
make all

# Rebuild only: mirror + legacy results → canonical stage-2 events
make rebuild

# Release only: identity lock → workbook (requires out/stage2_canonical_events.csv)
make release

# QC only
make qc
```

**Rebuild mode** parses the HTML mirror and `inputs/OLD_RESULTS.txt` into canonical stage-2 tables.

**Release mode** applies the identity lock and produces the final workbook. Requires `out/stage2_canonical_events.csv` — run `make rebuild` first if it doesn't exist.

---

## Quality Control (QC)

```bash
make qc
```

QC includes:

- Pipeline integrity checks (`qc/qc_master.py`)
- Post-release reconciliation checks (`tools/32_post_release_qc.py`)
- Schema and logical consistency validation (`tools/33_schema_logic_qc.py`)

QC ensures:

- Referential integrity (placements ↔ Persons_Truth)
- No duplicate canonical persons
- No duplicate placement collisions
- Consistent event/index coverage
- Division taxonomy sanity
- Place-sequence integrity (where full results are claimed)

Warnings may remain for known historical limitations (ties, pool play, partial top-N publishing).

**Current baseline (v1.0.21):** Gate 3 PASS = 3,444 · Stage 2: 0 errors, 15 warnings · Stage 3: 0 errors

---

## Generated Outputs

Typical outputs under `out/`:

| File | Description |
|---|---|
| `Placements_Flat.csv` | All placements, one row per competitor slot |
| `Placements_ByPerson.csv` | Placements joined to identity lock |
| `Placements_Unresolved.csv` | Placements with no identity assignment |
| `Analytics_Safe_Surface.csv` | Coverage-filtered, identity-locked analytics rows |
| `Coverage_ByEventDivision.csv` | Coverage completeness per event/division |
| `Coverage_GapPriority.csv` | Prioritized list of coverage gaps |
| `Persons_Truth.csv` | Active copy of identity lock (from v31 source) |
| `Persons_Unresolved.csv` | Persons with unresolved identity |
| `persons_truth.lock` | SHA-256 sentinel proving identity immutability |
| `out/qc_reports/` | QC detail reports |
| `Footbag_Results_Canonical.xlsx` | **Final workbook (primary deliverable)** |

All generated artifacts are reproducible from: mirror data, legacy results, `Persons_Truth_Final_v31.csv`, and code version.

### Workbook Sentinel Closure

If the final workbook contains placement rows bucketed under the reserved sentinel identity `__NON_PERSON__`, run:

```bash
python tools/06_fixup_workbook_sentinels.py INPUT.xlsx OUTPUT.xlsx
```

This ensures referential closure between `Placements_ByPerson` and `Persons_Truth` within the workbook artifact.
This does **not** modify `Persons_Truth_Final_v31.csv`.

---

## Determinism Guarantee

From a clean clone with the same mirror, legacy inputs, `Persons_Truth_Final_v31.csv`, and code version, the pipeline produces:

- Identical row counts
- Identical UUID assignments
- Identical QC results
- Identical workbook sheet structure

No randomness. No implicit identity merges. No silent data mutation.

---

## Versioning Policy

| Version | Meaning |
|---|---|
| Patch (v1.0.x) | QC improvements, formatting, refactors — no identity change |
| Minor (v1.x.0) | Additive analytics sheets |
| Major (v2.0.0) | Any change to Persons_Truth or identity logic |

Current identity baseline: `Persons_Truth_Final_v31.csv` (3,444 persons · v1.0.21)

---

## Maintainer

James Leberknight
Footbag archival reconstruction project
