📦 Footbag Results Pipeline — v1.0.3

Archive-quality, deterministic pipeline for building a canonical Footbag historical results dataset and Excel workbook.

This repository contains:

End-to-end parsing + normalization pipeline

Deterministic identity layer (collision-free)

Human-gated identity resolution workflow

QC gates and diagnostics

Release-based mirror input distribution

🎯 Design Goals

Deterministic builds

No silent merges

Explicit identity overrides

Reproducible from a clean clone

Clear separation between:

Code

Configuration

Generated outputs

Large static inputs (mirror)

📁 Repository Structure
/
├─ 01_parse_mirror.py
├─ 01b_import_old_results.py
├─ 01c_merge_stage1.py
├─ 02_canonicalize_results.py
├─ 02p5_player_token_cleanup.py
├─ 03_build_excel.py
├─ 04_build_analytics.py
│
├─ qc_*.py
├─ tools/
│
├─ overrides/
│   └─ person_aliases.csv
│
├─ data/
│   ├─ qc_baseline_stage1.json
│   ├─ qc_baseline_stage2.json
│   ├─ qc_baseline_stage3.json
│   └─ qc_baseline_gate3.json
│
├─ OLD_RESULTS.txt
├─ requirements.txt
├─ CLAUDE.md
└─ README.md

Not tracked in git:

mirror/
out/
*.xlsx
archive files
🔧 Requirements

Python 3.9+

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

Dependencies:

pandas

openpyxl

beautifulsoup4

lxml

📥 Required Mirror Input (Release Asset)

Stage 01 requires an offline HTML mirror.

The mirror is distributed via GitHub Release (not stored in git).

Download

Go to the Releases page and download:

mirror.tar.gz
Extract

From repository root:

tar -xzf mirror.tar.gz

This creates:

./mirror/

Required by:

01_parse_mirror.py
🚀 Canonical Build Order
01_parse_mirror.py
01b_import_old_results.py
01c_merge_stage1.py
02_canonicalize_results.py
02p5_player_token_cleanup.py
03_build_excel.py
04_build_analytics.py
⚠️ Important: Stage Ordering Nuance

Stage 03 builds the base Excel workbook.

Stage 04 must run last.

Stage 04:

Finalizes analytics tables

Enforces collision-free Persons_Truth

Quarantines ambiguous identities

Sets final workbook sheet ordering (README first)

Never run Stage 03 after Stage 04.

If you do, workbook ordering will be reset.

📤 Outputs

Generated artifacts (in out/):

stage1_raw_events.csv

stage2_canonical_events.csv

Placements_Flat.csv

Placements_ByPerson.csv

Persons_Truth.csv

Persons_Unresolved.csv

QC summaries

Analytics CSVs

Generated workbook (repo root):

Footbag_Results_Canonical.xlsx

⚠️ Generated outputs are not committed to git.

🔐 Identity Model (v1.0.3)
Persons_Truth is guaranteed to be:

Collision-free

Deterministic

Derived from overrides + validated data

Never manually edited

If multiple effective_person_id values map to the same person_canon,
those rows are automatically moved to:

Persons_Unresolved

No speculative merges are performed.

All human decisions are stored in:

overrides/person_aliases.csv
🔄 Optional Human Resolution Workflow (06 / 07)

These steps are only needed if you want to reduce unresolved identities.

06 — Resolve Unmapped Names
python 06_resolve_unmapped.py --generate
# edit out/unmapped_resolution.csv
python 06_resolve_unmapped.py --apply
07 — Resolve Quarantine Splits
python 07_resolve_quarantine.py --generate
# edit out/quarantine_resolution.csv
python 07_resolve_quarantine.py --apply
After Applying 06 or 07

Rebuild identity layer:

python 04_build_analytics.py --force-identity

(Do not rerun Stage 03.)

🔒 Identity Locking

If overrides are locked:

overrides/person_aliases.lock

Remove the lock to apply changes:

rm overrides/person_aliases.lock

Recreate after applying.

Lock files are not committed.

🧪 Quality Control

Quick health check:

python qc_pipeline_status.py

Full QC sweep:

python qc_master.py

Baselines are stored in:

data/qc_baseline_*.json
📦 Release Policy

Large static inputs are distributed via GitHub Releases:

mirror.tar.gz

(optional) Footbag_Results_Canonical.xlsx

Repository contains:

Code

Overrides

Baselines

Configuration

🔁 Reproducibility Test

From a clean clone:

git clone <repo-url>
cd footbag-results
git checkout v1.0.3-persons-clean
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir -p out
# extract mirror.tar.gz
# run pipeline

If build completes and QC passes, repository is consistent.

📜 Versioning

Current stable identity baseline:

v1.0.3-persons-clean

This tag represents a collision-free, archive-safe identity state.

👤 Maintainer

James Leberknight
Footbag archival project
