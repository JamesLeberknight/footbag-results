# Footbag Results Dataset — FOOTBAG_DATA

**CSV-only pipeline derivative of the canonical footbag results archive.**

---

## Repo Boundary

This repo produces **canonical CSVs, QC validation, and release/export artifacts** from raw competition data. It does NOT load data into any database.

| In Scope | Out of Scope |
|----------|-------------|
| Source ingestion (mirror + curated) | SQLite / database schema |
| Stage / merge / canonicalization | DB load scripts |
| Canonical CSV generation | DB seed loaders |
| QC validation | Express / server / app runtime |
| Workbook / release export | Net enrichment (DB-dependent) |
| Platform seed CSV export | Club / membership enrichment |

### Relationship to footbag-platform

`footbag-platform/legacy_data` is the **authoritative source of truth**. This repo reproduces its canonical CSV pipeline without database interaction. Pipeline code is synchronized from footbag-platform; data differences exist only due to mirror content (see Parity Report).

---

## Pipeline Modes

Run from the repo root with an active Python venv:

```bash
./run_pipeline.sh canonical_only   # rebuild → canonical CSVs → QC
./run_pipeline.sh release          # canonical_only + workbook + seed CSVs
./run_pipeline.sh full_csv         # alias for release
```

### Pipeline Stages

| Stage | Script(s) | Output |
|-------|-----------|--------|
| 1. Rebuild | mirror_results_adapter, curated_events_adapter, 01c, 02, 02p5, 02p6 | stage1/stage2 intermediate CSVs |
| 2. Release | export_historical_csvs, 05p5_remediate, export_canonical_platform | `out/canonical/*.csv`, `event_results/canonical_input/*.csv` |
| 3. Supplement | 02p5b_supplement_class_b | Placements_Flat completeness |
| 4. QC Gate | pipeline/qc/run_qc.py | PASS/FAIL (hard failures block release) |
| 5. Workbook | build_workbook_release.py | `out/Footbag_Results_Release.xlsx` |
| 6. Seed CSV | 07_build_mvfp_seed_full.py | `event_results/seed/mvfp_full/*.csv` |

---

## Canonical Outputs

### `out/canonical/` — Historical relational dataset

| File | Description |
|------|-------------|
| `events.csv` | All published events (1980–present) |
| `event_disciplines.csv` | Qualifying disciplines per event |
| `event_results.csv` | Placement rows |
| `event_result_participants.csv` | Participant rows |
| `persons.csv` | Canonically identified persons |

### `event_results/canonical_input/` — Platform-filtered export

Same schema as canonical, filtered by the platform export gate (person-likeness, alias merge, coverage filter).

### `event_results/seed/mvfp_full/` — Platform seed CSVs

Seed format suitable for downstream database loading (in footbag-platform).

---

## Known Unknowns

`out/known_unknowns.csv` is a live backlog of documented data gaps and limitations:

- Unresolved player identities (~2%)
- Missing country data
- Sparse pre-1997 events
- FBW scan coverage gaps
- Quarantined events

This file is included in the KNOWN UNKNOWNS sheet of the release workbook.

---

## Key Directories

```
inputs/               Source data (identity lock, curated events, overrides)
inputs/identity_lock/ Frozen person identity mappings (PT v53, PBP v97)
inputs/curated/       Pre-1997 structured CSVs and records
overrides/            Pipeline overrides (aliases, event metadata, location fixes)
pipeline/             Pipeline scripts (adapters, canonicalization, export, QC)
event_results/        Platform export outputs and seed CSVs
out/                  Pipeline outputs (canonical CSVs, workbook, QC artifacts)
mirror_full/          Local footbag.org HTML mirror (not committed)
tools/                Legacy analysis/migration scripts (non-authoritative)
qc/                   Standalone QC validators
```

---

## QC

```bash
python pipeline/qc/run_qc.py
```

QC must pass with zero hard failures before any release.

---

## Parity

```bash
./check_parity.sh
```

Compares FOOTBAG_DATA outputs against footbag-platform outputs. See `parity_report.txt` for the current divergence analysis (mirror data differences only).
