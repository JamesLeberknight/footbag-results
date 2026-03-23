# Tools

This directory contains utility scripts supporting the pipeline. Scripts fall into three categories:

## Active Pipeline Tools

These are called by `run_pipeline.sh` or invoked as part of the standard release workflow:

| Script | Role |
|---|---|
| `build_final_workbook_v13.py` | Build community Excel workbook (Stage 04B) |
| `run_qc_gate.py` | Canonical CSV QC gate (hard-fail check) |
| `32_post_release_qc.py` | Post-release data integrity (6 checks) |
| `33_schema_logic_qc.py` | Schema and logic consistency (7 checks) |
| `36_worlds_coverage_qc.py` | Worlds event coverage audit |
| `event_comparison_viewerV10.py` | HTML event comparison viewer (latest version) |

## Identity & Curation Tools

Scripts used during active identity resolution and data curation (not part of the automated release pipeline):

- `18_migrate_identity_lock.py` — migrate identity lock to new version
- `19_consolidate_truth.py` — batch Truth-to-Truth merges
- `22_merge_truth_pairs.py` — ad-hoc person merge pairs
- `34_identity_suggestions.py`, `56_member_alias_suggestions.py` — alias suggestions

## Historical Migration Scripts

Patch scripts used to migrate Placements_ByPerson and Persons_Truth between versions. Retained for audit traceability. Not needed to reproduce the current release.

- `patch_pt_v46_to_v47.py` — most recent PT patch
- `patch_pbp_v82_to_v83.py` through `patch_pbp_v84_to_v85.py` — most recent PBP patches
- Earlier versions: `35_patch_placements_v33_to_v34.py` through `62_patch_pbp_v65_to_v66.py`

## Pre-1997 / Image Processing Tools

Scripts supporting the separate pre-1997 historical recovery effort (not part of the post-1997 release pipeline):

- `fbw_viewer*.py` — Footbag World magazine image viewer
- `prep_vlm_tiles.py`, `harvest_vlm_results.py` — VLM extraction pipeline
- `preprocess_crops.py`, `preprocess_scans.py`, `sharpen_images.py` — image preprocessing
