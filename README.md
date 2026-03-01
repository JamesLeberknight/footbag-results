# Footbag Results Pipeline — v1.0.17

Archive-quality, deterministic pipeline for building a canonical Footbag historical
results dataset and Excel workbook from an offline HTML mirror.

---

## ⚠️ Prerequisite: Mirror Download

**The pipeline cannot run without the HTML mirror.** It is not stored in git — download
it from the [Releases page](https://github.com/JamesLeberknight/footbag-results/releases)
before doing anything else.

```bash
# Download mirror.tar.gz from Releases, then:
tar -xzf mirror.tar.gz      # extracts → mirror/  (required by scripts/rebuild/01_parse_mirror.py)
```

---

## Quick Start

```bash
# 1. Clone and set up
git clone https://github.com/JamesLeberknight/footbag-results.git
cd footbag-results
make setup                  # creates .venv, installs deps, creates out/

# 2. Download and extract mirror (see above — required first)

# 3. Rebuild Mode: parse mirror → canonical events
make rebuild
# Runs in order:
#   scripts/rebuild/01_parse_mirror.py      ← parses HTML mirror
#   scripts/rebuild/01b_import_old_results.py ← imports legacy/OLD_RESULTS.txt
#   scripts/rebuild/01c_merge_stage1.py     ← merges stage-1 outputs
#   scripts/rebuild/02_canonicalize_results.py ← produces out/stage2_canonical_events.csv

# 4. Release Mode: identity-locked outputs + workbook
make release
# Runs in order:
#   02p5_player_token_cleanup.py   ← applies identity lock → out/Placements_Flat.csv
#   03_build_excel.py              ← builds Footbag_Results_Canonical.xlsx
#   04_build_analytics.py          ← adds analytics, writes out/persons_truth.lock

# 5. QC
make qc
```

---

## Design Goals

- Deterministic builds — same inputs always produce the same outputs
- No silent merges — every identity decision is human-verified
- Explicit identity overrides — canonical truth lives in `inputs/identity_lock/`
- Reproducible from a clean clone + released mirror archive
- Clear separation: code / configuration / generated outputs / large static inputs

---

## Repository Structure

```
/
├─ Makefile                            ← Pipeline runner (setup/rebuild/release/qc/all)
│
├─ scripts/rebuild/                    ← REBUILD MODE — parse mirror → stage-2 events
│   ├─ 01_parse_mirror.py              ← Stage 1: extract raw facts from HTML mirror
│   ├─ 01b_import_old_results.py       ← Stage 1b: import pre-mirror results
│   ├─ 01c_merge_stage1.py             ← Stage 1c: merge all stage-1 outputs
│   └─ 02_canonicalize_results.py      ← Stage 2: normalize events + divisions
│
├─ 02p5_player_token_cleanup.py        ← RELEASE MODE stage 1: apply identity lock
├─ 03_build_excel.py                   ← RELEASE MODE stage 2: build workbook
├─ 04_build_analytics.py               ← RELEASE MODE stage 3: analytics + lock sentinel
│
├─ legacy/
│   ├─ OLD_RESULTS.txt                 ← Pre-mirror historical results (required by 01b)
│   └─ *.py                            ← Pre-identity-lock scripts (not for release builds)
│
├─ inputs/
│   └─ identity_lock/                  ← Authoritative human-verified truth (immutable)
│       ├─ Persons_Truth_Final_v31.csv
│       ├─ Persons_Unresolved_Organized_v27.csv
│       └─ Placements_ByPerson_v33.csv
│
├─ qc/                                 ← QC package (pipeline health + presentability)
│   ├─ qc_master.py
│   ├─ qc_pipeline_status.py
│   └─ qc*.py
│
├─ tools/                              ← Standalone audit and identity tools
│   ├─ 32_post_release_qc.py           ← 6 post-release data-integrity checks
│   ├─ 33_schema_logic_qc.py           ← 7 schema + logical coherence checks
│   └─ (alias generation, review, recovery helpers)
│
├─ overrides/
│   ├─ person_aliases.csv              ← Human-curated alias map
│   └─ events_overrides.jsonl          ← Event-level parsing overrides
│
├─ data/                               ← QC baselines
├─ requirements.txt
├─ CLAUDE.md                           ← Pipeline contract (authoritative)
├─ CHANGELOG.md
├─ RELEASE_CHECKLIST.md
└─ README.md

Not tracked in git (too large / generated):
  mirror/                              ← Download from Releases as mirror.tar.gz
  out/                                 ← All generated outputs (recreated by pipeline)
  *.xlsx                               ← Generated workbook
```

---

## Requirements

Python 3.9+

```bash
make setup
# equivalent to:
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir -p out
```

---

## Pipeline Walkthrough

### Step 0 — Mirror (required first)

Download `mirror.tar.gz` from the [Releases page](https://github.com/JamesLeberknight/footbag-results/releases)
and extract it:

```bash
tar -xzf mirror.tar.gz   # creates ./mirror/
```

`mirror/` is required by `scripts/rebuild/01_parse_mirror.py`. Without it nothing runs.

### Step 1 — Rebuild Mode

Parses the HTML mirror and reconstructs events. Produces `out/stage2_canonical_events.csv`,
which is required input for Release Mode.

```bash
make rebuild
```

What it runs:

| Script | What it does |
|--------|-------------|
| `scripts/rebuild/01_parse_mirror.py` | Walks `mirror/`, extracts raw placement facts |
| `scripts/rebuild/01b_import_old_results.py` | Imports `legacy/OLD_RESULTS.txt` (pre-mirror events) |
| `scripts/rebuild/01c_merge_stage1.py` | Merges 01 + 01b into one validated stage-1 dataset |
| `scripts/rebuild/02_canonicalize_results.py` | Normalizes events, divisions, persons → `out/stage2_canonical_events.csv` |

### Step 2 — Release Mode

Consumes identity-lock artifacts. Produces the deterministic canonical dataset and workbook.

```bash
make release
```

What it runs:

| Script | What it does |
|--------|-------------|
| `02p5_player_token_cleanup.py` | Applies `inputs/identity_lock/` → `out/Placements_Flat.csv` |
| `03_build_excel.py` | Builds `Footbag_Results_Canonical.xlsx` from stage-2 events |
| `04_build_analytics.py` | Adds analytics sheets, enforces Gate 3, writes `out/persons_truth.lock` |

> **Stage ordering:** never re-run Stage 03 after Stage 04 — it resets workbook sheet ordering.

### Step 3 — QC

```bash
make qc
```

Runs `qc/qc_master.py`, `tools/32_post_release_qc.py`, and `tools/33_schema_logic_qc.py`.

---

## Pipeline Outputs (`out/`)

| File | Description |
|------|-------------|
| `stage2_canonical_events.csv` | Parsed + normalized events (Rebuild Mode output) |
| `Placements_Flat.csv` | 25,679 identity-locked placements |
| `Persons_Truth.csv` | 3,451 canonical persons (Gate 3) |
| `Persons_Unresolved.csv` | 76 genuinely ambiguous persons |
| `Coverage_ByEventDivision.csv` | Coverage flags per event/division |
| `Analytics_Safe_Surface.csv` | 22,958 analytics-safe placements |
| `persons_truth.lock` | SHA-256 sentinel proving identity immutability |
| `Footbag_Results_Canonical.xlsx` | Canonical Excel workbook (repo root) |

---

## Identity Model (v1.0.17)

Identity is locked in `inputs/identity_lock/` and never recomputed during Release Mode.

- `Persons_Truth_Final_v31.csv` — 3,451 canonical persons, collision-free
- `Persons_Unresolved_Organized_v27.csv` — 76 genuinely ambiguous entries
- `Placements_ByPerson_v33.csv` — 25,679 authoritative placements

Any identity change requires a new major version (v2.0.0) per `CLAUDE.md`.

---

## Reproducibility

From a clean clone:

```bash
git clone https://github.com/JamesLeberknight/footbag-results.git
cd footbag-results
git checkout v1.0.17
make setup
tar -xzf mirror.tar.gz    # downloaded from Releases
make all                   # rebuild → release → qc
```

Expected metrics after a clean run:
- Gate 3 PASS: 3,451 persons
- `out/Placements_Flat.csv`: 25,679 rows
- `out/Analytics_Safe_Surface.csv`: 22,958 rows
- `out/persons_truth.lock`: references v31 / v27 / v33

---

## Versioning

| Version | Meaning |
|---------|---------|
| v1.0.x | Docs, refactors, QC improvements — no data changes |
| v1.x.0 | Additive analytics — no identity changes |
| v2.0.0 | Any change to Persons_Truth, Persons_Unresolved, or identity logic |

Current stable release: **v1.0.17**

---

## Maintainer

James Leberknight — Footbag archival project
