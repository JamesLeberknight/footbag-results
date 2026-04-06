PYTHON = .venv/bin/python

.PHONY: setup rebuild release qc merged all run

## First-time setup: create venv, install deps, create out/ dir
setup:
	python3 -m venv .venv
	.venv/bin/pip install --quiet -r requirements.txt
	mkdir -p out

## Rebuild Mode: parse HTML mirror → canonical stage-2 events
## POST-1997 mirror only. OLD_RESULTS / FBW / magazine ingestion
## belong to the pre-1997 pipeline (./run_early_pipeline.sh).
## Requires: mirror/ directory extracted from mirror.tar.gz (see README)
rebuild:
	$(PYTHON) pipeline/adapters/mirror_results_adapter.py
	$(PYTHON) pipeline/01c_merge_stage1.py
	$(PYTHON) pipeline/02_canonicalize_results.py

## Release Mode: identity-locked canonical outputs + workbook
## Requires: out/stage2_canonical_events.csv (run 'make rebuild' first)
## Order: 02p5 → 02p6 → 03 → 04 → [01b1 aux] → 04B → 05 → 05p5
release:
	$(PYTHON) pipeline/02p5_player_token_cleanup.py \
	  --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v85.csv \
	  --persons_truth_csv            inputs/identity_lock/Persons_Truth_Final_v47.csv \
	  --out_dir                      out
	$(PYTHON) pipeline/02p6_structural_cleanup.py
	$(PYTHON) pipeline/03_build_excel.py
	$(PYTHON) pipeline/04_build_analytics.py
	$(PYTHON) pipeline/01b1_merge_consecutives.py
	$(PYTHON) tools/build_final_workbook_v13.py
	$(PYTHON) pipeline/historical/export_historical_csvs.py
	$(PYTHON) pipeline/05p5_remediate_canonical.py

## QC: master checks + post-release integrity + schema/logic audit
qc:
	$(PYTHON) qc/qc_master.py
	$(PYTHON) tools/32_post_release_qc.py
	$(PYTHON) tools/33_schema_logic_qc.py

## Merged build: combined 1980-present dataset + workbook + viewer
## Requires: both post-1997 ('make all') and pre-1997 pipelines complete
## Requires: out/canonical_all_union/ (from early_data/scripts/12_*)
merged:
	$(PYTHON) tools/build_appsafe_merged.py
	$(PYTHON) tools/build_merged_feeds.py
	$(PYTHON) tools/build_merged_workbook_v14.py
	$(PYTHON) tools/event_comparison_viewerV10.py

## Full post-1997 pipeline: rebuild → release → qc
all: rebuild release qc

## Alias: run pipeline
run: all
