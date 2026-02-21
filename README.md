📦 Footbag Results Pipeline — v1.0

Archive-quality, deterministic pipeline for building a canonical Footbag historical results dataset and Excel workbook.

This repository contains:

End-to-end parsing + normalization pipeline

Identity resolution workflow

QC gates and diagnostics

Deterministic analytics build

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

Install dependencies:

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

Go to the Releases page.

Download:

mirror.tar.gz
Extract

From repository root:

tar -xzf mirror.tar.gz

This creates:

./mirror/

which is required by:

01_parse_mirror.py
🚀 Running the Full Pipeline

Create output directory:

mkdir -p out

Run in order:

python 01_parse_mirror.py
python 01b_import_old_results.py --in OLD_RESULTS.txt
python 01c_merge_stage1.py --out-dir out
python 02_canonicalize_results.py
python 02p5_player_token_cleanup.py
python 03_build_excel.py
python 04_build_analytics.py
📤 Outputs

Generated artifacts (in out/):

stage1_raw_events.csv

stage2_canonical_events.csv

Placements_Flat.csv

Placements_ByPerson.csv

Persons_Truth.csv

QC summaries

Analytics CSVs

Generated workbook (repo root):

Footbag_Results_Canonical.xlsx

⚠️ Generated outputs are not committed to git.

🧪 Quality Control

Quick health check:

python qc_pipeline_status.py

Full QC sweep:

python qc_master.py

Baselines are stored in:

data/qc_baseline_*.json
🔐 Identity Resolution Workflow (Human Gates)

When QC detects issues:

Multi-person canon collisions
python qc02_canon_multiple_person_ids.py
python 05_disambiguate_persons.py --generate

Edit generated CSV → apply:

python 05_disambiguate_persons.py --apply
python 04_build_analytics.py --force-identity
Unmapped persons
python qc_tier1_people.py
python 06_resolve_unmapped.py --generate

Edit → apply → rebuild 02p5 and 04.

📦 Release Policy

Large static inputs are distributed via GitHub Releases:

mirror.tar.gz

(optional) Footbag_Results_Canonical.xlsx

Git repository contains:

Code

Overrides

Baselines

Configuration only

🔁 Reproducibility Test (Recommended)

From a clean clone:

git clone <repo-url>
cd footbag-results
git checkout v1.0
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir -p out
# extract mirror.tar.gz
# then run pipeline

If build completes without errors and QC passes, repository is consistent.

📜 Versioning

Tag used for this release:

v1.0

This tag represents a stable handoff state.

🧠 Notes

No identity merges are performed automatically.

All person resolution decisions are recorded in:

overrides/person_aliases.csv

Lock files are not committed.

The repository is intentionally strict and deterministic.

👤 Maintainer

James Leberknight
Footbag archival project
