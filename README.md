# Footbag Results Pipeline — Canonical Excel Builder (v1.0.x)

Deterministic pipeline for producing the **final, reviewer-ready Excel workbook** of historical Footbag results.

This repository reconstructs structured event and placement data from:

- An offline HTML mirror of Footbag.org
- Legacy historical result files
- A curated identity lock (`Persons_Truth`)

The primary deliverable is the final Excel spreadsheet.

---

# 🎯 Primary Goal

The purpose of this repository is to build a clean, internally consistent, archival-quality Excel workbook of Footbag results suitable for:

- Historical preservation
- Statistical analysis
- Community sharing
- Reviewer audit

All parsing, normalization, analytics, and QC exist to support this final spreadsheet.

---

# 🔐 Identity Model (Authoritative Core)

Identity is the foundation of the dataset.

All person identity used in analytics and the workbook derives exclusively from:


inputs/identity_lock/Persons_Truth_Final_v31.csv


Location in working copy:


~/projects/FOOTBAG_DATA/inputs/identity_lock/


## Persons_Truth

`Persons_Truth_Final_v31.csv` enforces:

- One row per real person
- Globally unique `effective_person_id` (UUID)
- Unique canonical display name (`person_canon`)
- Human-verified identity resolution
- Collision-free canonicalization

Release mode **does not infer identity**.  
It only applies the identity lock.

If `Persons_Truth` changes, the identity of the dataset changes.

This should be treated as a **major version event**.

---

# 📁 Repository Structure

```text
/
├─ Makefile
├─ inputs/
│  └─ identity_lock/
│     └─ Persons_Truth_Final_v31.csv
├─ scripts/
│  ├─ 01_parse_mirror.py
│  ├─ 01b_import_old_results.py
│  ├─ 01c_merge_stage1.py
│  ├─ 02_canonicalize_results.py
│  ├─ 03_build_excel.py
│  └─ 04_build_analytics.py
├─ qc/
│  └─ qc_master.py
├─ tools/
│  ├─ 32_post_release_qc.py
│  └─ 33_schema_logic_qc.py
├─ overrides/
└─ out/  (generated outputs)

Notes:

out/ is fully rebuildable and not a source of truth.

The only authoritative identity artifact is Persons_Truth_Final_v31.csv.

🔄 Pipeline Overview
Stage 01 — Parse Mirror

scripts/01_parse_mirror.py
Extracts raw structured placement data from the HTML mirror.

Stage 01b — Import Legacy Results

scripts/01b_import_old_results.py
Imports historical results not present in the mirror.

Stage 01c — Merge Stage1 Sources

scripts/01c_merge_stage1.py
Unifies mirror and legacy inputs into a single structured dataset.

Stage 02 — Canonicalize Results

scripts/02_canonicalize_results.py
Normalizes:

event metadata

division names

placement structure

Produces canonical stage2 tables.

Stage 03 — Build Excel Workbook (Primary Output)

scripts/03_build_excel.py
Builds the final Excel spreadsheet from canonical data.

Stage 04 — Build Analytics

scripts/04_build_analytics.py
Generates analytics surfaces used by the workbook (summary sheets, stats, etc.).

Identity used here is strictly derived from Persons_Truth.

🧪 Quality Control (QC)

Run:

make qc

QC includes:

Pipeline integrity checks

Post-release reconciliation checks

Schema + logical consistency validation

QC ensures:

Referential integrity (placements ↔ Persons_Truth)

No duplicate canonical persons

No duplicate placement collisions

Consistent event/index coverage

Division taxonomy sanity

Place-sequence integrity (where full results are claimed)

Warnings may remain for known historical limitations (ties, pool play, partial top-N publishing).

📊 Generated Outputs

Typical outputs under out/ include:

stage2_canonical_events.csv

Placements_Flat.csv

Coverage_ByEventDivision.csv

Coverage_GapPriority.csv

Analytics_Safe_Surface.csv

QC reports under out/qc_reports/

Final Excel workbook (*.xlsx)

### Workbook Sentinel Closure

If the final workbook contains placement rows bucketed under the
reserved sentinel identity `__NON_PERSON__`, run:

python tools/06_fixup_workbook_sentinels.py INPUT.xlsx OUTPUT.xlsx

This ensures referential closure between Placements_ByPerson and
Persons_Truth within the workbook artifact.

This does not modify `Persons_Truth_Final_v31.csv`.

All generated artifacts are reproducible from:

Mirror data

Legacy results

Persons_Truth_Final_v31.csv

Code version

🔁 Determinism Guarantee

From a clean clone with the same:

Mirror

Legacy inputs

Persons_Truth_Final_v31.csv

The pipeline should produce:

Identical row counts

Identical UUID assignments

Identical QC results

Identical workbook sheet structure

No randomness.
No implicit identity merges.
No silent data mutation.

🏷 Versioning Policy
Version Type	Meaning
Patch (v1.0.x)	QC improvements, formatting, refactors (no identity change)
Minor (v1.x.0)	Additive analytics sheets
Major (v2.0.0)	Any change to Persons_Truth or identity logic

Current identity baseline: Persons_Truth_Final_v31.csv

Maintainer

James Leberknight
Footbag archival reconstruction project
