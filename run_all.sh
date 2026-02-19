set -euo pipefail
python 01_parse_mirror.py
python 01b_import_old_results.py --in OLD_RESULTS.txt --out out/stage1_raw_events_old.csv
python 01c_merge_stage1.py
python 02_canonicalize_results.py
python 02p5_player_token_cleanup.py
python 03_build_excel.py
python 04_build_analytics.py
python 04b_recover_placements.py
