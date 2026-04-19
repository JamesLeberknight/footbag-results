# tools/ — Legacy Analysis & Migration Scripts

**Status:** These scripts are non-authoritative and not part of the current
canonical pipeline. The authoritative pipeline is in `pipeline/` and is
synchronized from `footbag-platform/legacy_data`.

These scripts may reference paths, schemas, or intermediate files that no
longer exist. They are preserved for historical reference and audit
traceability.

## Historical Pipeline Tools (superseded)

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
