PYTHON = .venv/bin/python

.PHONY: setup rebuild release qc all run

## First-time setup: create venv, install deps, create out/ dir
setup:
	python3 -m venv .venv
	.venv/bin/pip install --quiet -r requirements.txt
	mkdir -p out

## Rebuild Mode: parse HTML mirror → canonical stage-2 events
## Requires: mirror/ directory extracted from mirror.tar.gz (see README)
rebuild:
	$(PYTHON) pipeline/01_parse_mirror.py
	$(PYTHON) pipeline/01b_import_old_results.py
	$(PYTHON) pipeline/01b1_merge_consecutives.py
	$(PYTHON) pipeline/01c_merge_stage1.py
	$(PYTHON) pipeline/02_canonicalize_results.py

## Release Mode: identity-locked canonical outputs + workbook
## Requires: out/stage2_canonical_events.csv (run 'make rebuild' first)
## Order matches run_pipeline.sh: 04 → 04B → 05 → 05p5
release:
	$(PYTHON) pipeline/01b1_merge_consecutives.py
	$(PYTHON) pipeline/02p5_player_token_cleanup.py \
	  --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v78.csv \
	  --persons_truth_csv            inputs/identity_lock/Persons_Truth_Final_v44.csv \
	  --out_dir                      out
	$(PYTHON) pipeline/02p6_structural_cleanup.py
	$(PYTHON) pipeline/03_build_excel.py
	$(PYTHON) pipeline/04_build_analytics.py
	$(PYTHON) tools/build_final_workbook_v13.py
	$(PYTHON) pipeline/05_export_canonical_csv.py
	$(PYTHON) pipeline/05p5_remediate_canonical.py

## QC: master checks + post-release integrity + schema/logic audit
qc:
	$(PYTHON) qc/qc_master.py
	$(PYTHON) tools/32_post_release_qc.py
	$(PYTHON) tools/33_schema_logic_qc.py

## Full pipeline: rebuild → release → qc
all: rebuild release qc

## Alias: run pipeline
run: all
