# Footbag Results Pipeline — v2.11.0

Deterministic pipeline for reconstructing and archiving historical Footbag results.

Produces canonical relational CSV exports, an analytics Excel workbook, and a
community-facing Excel workbook from an HTML mirror of Footbag.org and curated
identity-lock artifacts.

---

## Requirements

> **The HTML mirror is required to run the full pipeline.**
> Download `mirror.tar.gz` from the GitHub Release assets and extract it
> before running anything:
>
> ```bash
> tar -xzf mirror.tar.gz        # produces mirror/ in the current directory
> # — or, if you have mirror_full/ —
> ln -s mirror_full mirror
> ```
>
> Without `mirror/`, only **release mode** works (requires a pre-built
> `out/stage2_canonical_events.csv`).

---

## Quick Start

```bash
# 1. One-time setup (create venv, install deps, create out/)
./run_pipeline.sh setup

# 2. Extract the mirror (see Requirements above), then run the full pipeline
./run_pipeline.sh all
```

Or run stages individually:

```bash
./run_pipeline.sh rebuild   # Parse mirror → stage2_canonical_events.csv
./run_pipeline.sh release   # Apply identity lock → workbooks + canonical CSVs
./run_pipeline.sh qc        # Run all QC checks
```

---

## Installation

### 1. Python 3.12+

### 2. Clone and set up

```bash
git clone <repo-url>
cd FOOTBAG_DATA

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p out
```

Or use the setup shortcut:

```bash
./run_pipeline.sh setup
```

---

## Data Prerequisites

### 1. HTML mirror (required for rebuild)

Obtain `mirror.tar.gz` from the GitHub Release assets and extract it so
the repo root contains a `mirror/` directory:

```bash
tar -xzf mirror.tar.gz   # produces mirror/ in the current directory
```

If the archive extracted as `mirror_full/` instead:

```bash
ln -s mirror_full mirror
```

Stage 01 (`01_parse_mirror.py`) expects `mirror/www.footbag.org/events/show/*/index.html`.

### 2. Identity lock files (already in repo)

```
inputs/identity_lock/
    Persons_Truth_Final_v42.csv          # 3,441 canonical persons
    Persons_Unresolved_Organized_v28.csv # 82 unresolved entries
    Placements_ByPerson_v63.csv          # 28,513 identity-locked placements
```

These are human-verified and treated as immutable for this release.

### 3. Legacy results (already in repo)

```
inputs/OLD_RESULTS.txt    # Historical events not in the mirror
legacy_data/              # Per-event result overrides (1999, 2003 Worlds, etc.)
```

---

## Pipeline Stages

### Rebuild Mode (stages 01–02)

Parses raw data into canonical stage-2 events.
**Requires `mirror/`.** Run once, or whenever the mirror or parser changes.

```bash
./run_pipeline.sh rebuild
```

| Stage | Script | Description |
|---|---|---|
| 01 | `pipeline/01_parse_mirror.py` | Parse HTML mirror → raw stage-1 placements |
| 01b | `pipeline/01b_import_old_results.py` | Import legacy results from `inputs/OLD_RESULTS.txt` |
| 01b1 | `pipeline/01b1_merge_consecutives.py` | Merge consecutives reference data |
| 01c | `pipeline/01c_merge_stage1.py` | Merge mirror + legacy into unified stage-1 |
| 02 | `pipeline/02_canonicalize_results.py` | Normalize events, divisions, placements → `out/stage2_canonical_events.csv` |

**Output:** `out/stage2_canonical_events.csv` — 774 events.

### Release Mode (stages 02p5–05)

Applies the identity lock and produces all final outputs.
Requires `out/stage2_canonical_events.csv` (run rebuild first, or obtain from a prior release).

```bash
./run_pipeline.sh release
```

| Stage | Script | Description |
|---|---|---|
| 01b1 | `pipeline/01b1_merge_consecutives.py` | Merge consecutives reference data (also run in release) |
| 02p5 | `pipeline/02p5_player_token_cleanup.py` | Apply identity lock → `Placements_Flat.csv`, `Placements_ByPerson.csv` |
| 03 | `pipeline/03_build_excel.py` | Canonical Excel workbook (`Footbag_Results_Canonical.xlsx`) |
| 04 | `pipeline/04_build_analytics.py` | Analytics surfaces, person stats, coverage, lock sentinel |
| 04B | `pipeline/04B_create_community_excel.py` | Community Excel workbook (`Footbag_Results_Community.xlsx`) |
| 05 | `pipeline/05_export_canonical_csv.py` | Relational CSV export to `out/canonical/` |

### QC

```bash
./run_pipeline.sh qc
```

| Script | Checks |
|---|---|
| `qc/qc_master.py` | Stage-2 and stage-3 integrity |
| `tools/32_post_release_qc.py` | 6 post-release data integrity checks |
| `tools/33_schema_logic_qc.py` | 7 schema and logic consistency checks |

---

## Outputs

All outputs go into `out/` (gitignored — never committed).

### Core identity outputs

| File | Rows | Description |
|---|---|---|
| `out/Placements_Flat.csv` | 28,513 | All placements, identity-locked |
| `out/Placements_ByPerson.csv` | 28,513 | Placements joined to person identity |
| `out/Persons_Truth.csv` | 3,441 | Active identity truth (copy of v42 source) |

| `out/Persons_Unresolved.csv` | ~402 | Persons without resolved identity |
| `out/Placements_Unresolved.csv` | ~376 | Placements for unresolved persons |
| `out/persons_truth.lock` | — | SHA-256 sentinel proving identity immutability |

### Analytics outputs

| File | Rows | Description |
|---|---|---|
| `out/Analytics_Safe_Surface.csv` | 16,487 | Coverage-filtered, identity-locked analytics rows |
| `out/Coverage_ByEventDivision.csv` | 3,710 | Coverage flag per (event, division) |
| `out/Coverage_GapPriority.csv` | 441 | Prioritized list of coverage gaps |

### Canonical relational export (`out/canonical/`)

Ground-truth normalized CSVs produced by stage 05 (`pipeline/05_export_canonical_csv.py`),
intended for database import or external consumption:

| File | Rows | Description |
|---|---|---|
| `events.csv` | 774 | One row per event; includes `event_type` (net/mixed/freestyle/worlds/golf/social) |
| `event_disciplines.csv` | 3,918 | One row per discipline within an event |
| `event_results.csv` | 24,933 | One row per placement slot |
| `event_result_participants.csv` | 36,073 | One row per participant |
| `persons.csv` | 3,441 | Canonical persons with aliases and legacy IDs |

Natural keys:
- `events`: `event_key`
- `event_disciplines`: `(event_key, discipline_key)`
- `event_results`: `(event_key, discipline_key, placement)`
- `event_result_participants`: `(event_key, discipline_key, placement, participant_order)`

### Workbooks

| File | Description |
|---|---|
| `Footbag_Results_Canonical.xlsx` | Full analytics workbook (internal/archival) |
| `Footbag_Results_Community.xlsx` | Community-facing workbook with honours, records, leaderboards |

---

## Identity Model

Every placement competitor maps to exactly one of:

- **`Persons_Truth`** — a verified real person with a stable UUID
- **`Persons_Unresolved`** — a person whose identity could not be confirmed
- **`__NON_PERSON__`** — noise, handles, club names, or parsing artifacts

Release mode **never infers identity**. It only applies the identity lock.

The identity lock files are in `inputs/identity_lock/`. Any change to
`Persons_Truth_Final` triggers a minor version bump (see Versioning below).

After every successful release, `out/persons_truth.lock` is written with
SHA-256 hashes of all three identity lock inputs, proving immutability.

---

## Data Coverage

| Metric | Value |
|---|---|
| Events | 774 |
| Year range | 1980–2026 |
| Placements (identity-locked) | 28,513 |
| Persons (canonical) | 3,441 |
| Gate3 PASS | 3,441 |
| Known-issue events | 54 |

Coverage is comprehensive from 1997 onward (the primary Footbag.org mirror)
but is not perfect. Pre-1997 data comes from historical records: 1980–1986
and 1990–1991 have partial results (top-3 only for most divisions). Years
1987–1989 and 1992–1996 have no coverage.

Some events contain merged divisions or incomplete standings due to how
results were originally published on Footbag.org (pool-plus-finals combined,
Open/Intermediate divisions merged under a single label, or only top
finishers listed). These limitations are documented in
`overrides/known_issues.csv` (54 events, severity: minor / moderate / severe)
and are not data errors — they reflect the fidelity of the original source.

21 events are quarantined in `inputs/review_quarantine_events.csv` because
their source structure makes deterministic parsing impossible without
authoritative external data. They are preserved in the dataset but excluded
from the active review queue.

---

## Versioning

| Version | Trigger |
|---|---|
| Patch (v2.x.y) | Docs, refactors, QC fixes — no data change |
| Minor (v2.x.0) | Any change to identity lock files or analytics |
| Major (v3.0.0) | Architectural change to identity model or pipeline contract |

Current identity baseline:

| Artifact | Version | Rows |
|---|---|---|
| `Persons_Truth_Final` | v42 | 3,441 |
| `Persons_Unresolved_Organized` | v28 | 82 |
| `Placements_ByPerson` | v63 | 28,513 |

---

## Repository Structure

```
/
├── Makefile                          # Pipeline commands
├── requirements.txt
├── run_pipeline.sh                   # Alternative to make
├── pipeline/
│   ├── 01_parse_mirror.py            # Rebuild: parse HTML mirror
│   ├── 01b_import_old_results.py     # Rebuild: import legacy results
│   ├── 01b1_merge_consecutives.py    # Rebuild+Release: merge consecutives data
│   ├── 01c_merge_stage1.py           # Rebuild: merge stage-1 sources
│   ├── 02_canonicalize_results.py    # Rebuild: produce stage-2 events
│   ├── 02p5_player_token_cleanup.py  # Release: apply identity lock
│   ├── 03_build_excel.py             # Release: canonical Excel workbook
│   ├── 04_build_analytics.py         # Release: analytics + lock sentinel
│   ├── 04B_create_community_excel.py # Release: community Excel workbook
│   └── 05_export_canonical_csv.py    # Release: relational CSV export
├── qc/
│   └── qc_master.py                  # Stage-2 and stage-3 QC
├── tools/
│   ├── 32_post_release_qc.py         # 6 post-release integrity checks
│   ├── 33_schema_logic_qc.py         # 7 schema and logic checks
│   └── ...                           # Identity curation and patch tools
├── inputs/
│   ├── identity_lock/                # Authoritative identity artifacts
│   │   ├── Persons_Truth_Final_v42.csv
│   │   ├── Persons_Unresolved_Organized_v28.csv
│   │   └── Placements_ByPerson_v62.csv
│   ├── location_canon_full_final.csv # Canonical location display strings
│   ├── consecutives_records.csv      # Consecutives world records reference data
│   ├── bap_data.csv                  # BAP honours data
│   ├── fbhof_data.csv                # FBHOF honours data
│   └── OLD_RESULTS.txt               # Legacy results (pre-mirror)
├── legacy_data/
│   └── event_results/                # Per-event result overrides
├── overrides/
│   └── person_aliases.csv            # Manual person alias assignments
├── data/
│   └── qc_baseline_*.json            # QC baseline snapshots
└── out/                              # Generated outputs (gitignored)
    ├── stage2_canonical_events.csv
    ├── Placements_Flat.csv
    ├── Persons_Truth.csv
    ├── persons_truth.lock
    ├── canonical/                    # Relational CSV export
    └── ...
```

---

## Maintainer

James Leberknight — Footbag archival reconstruction project
