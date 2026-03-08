#!/usr/bin/env bash
set -e

echo "Running Footbag pipeline..."

echo "Stage 01: parse mirror"
python pipeline/01_parse_mirror.py

echo "Stage 01b: import old results"
python pipeline/01b_import_old_results.py --old-results inputs/OLD_RESULTS.txt

echo "Stage 01c: merge stage1"
python pipeline/01c_merge_stage1.py

echo "Stage 02: canonicalize results"
python pipeline/02_canonicalize_results.py

echo "Stage 02p5: player token cleanup"
python pipeline/02p5_player_token_cleanup.py --identity_lock_placements_csv \
          inputs/identity_lock/Placements_ByPerson_v37.csv \
          --persons_truth_csv inputs/identity_lock/Persons_Truth_Final_v33.csv \
          --out_dir out

echo "Stage 03: build Excel workbook"
python pipeline/03_build_excel.py

echo "Stage 04: build analytics"
python pipeline/04_build_analytics.py

echo "Stage 04B: create community Excel workbook"
python pipeline/04B_create_community_excel.py

echo "Pipeline complete."
