# Data Migration Patch Archive

These scripts applied sequential patches to `Placements_ByPerson` (PBP) and
`Persons_Truth` (PT) during the identity-resolution and data-cleaning phases.

They are **not part of the active pipeline**. They are preserved here as an
audit trail of how the identity-locked dataset evolved from PBP v68 → v85.

## Sequence

| Script | What it did |
|--------|-------------|
| `patch_pbp_v68_to_v69.py` | Batch A QC fixes |
| `patch_pbp_v69_to_v70.py` | ... |
| `patch_pbp_v70_to_v71.py` | ... |
| `patch_pbp_v71_to_v72.py` | Quarantine resolved: 1999 Western Regional |
| `patch_pbp_v72_to_v73.py` | 2000 Arica Open Doubles Net cleanup |
| `patch_pbp_v73_to_v74.py` | 2001 Frankfurt Open missing placements |
| `patch_pbp_v74_to_v75.py` | 2001 Montreal Summer: city/province format fix |
| `patch_pbp_v75_to_v76.py` | Montreal 1991 Hivernal + Championships cleanup |
| `patch_pbp_v76_to_v77.py` | Convert old-format team rows to piped-UUID format |
| `patch_pbp_v77_to_v78.py` | Bucket A QC fixes (62 parsing artifacts removed) |
| `patch_pbp_v79_to_v80.py` | 1999 East Coast: division fix, typo fix |
| `patch_pbp_v80_to_v81.py` | PT v44→v45 renames (8 PT renames + Welch merge) |
| `patch_pbp_v82_to_v83.py` | PT v46→v47 ALL_CAPS→Title Case (615 rows) |
| `patch_pbp_v83_to_v84.py` | Zeil 2003: team rows + club suffix cleanup |
| `patch_pbp_v84_to_v85.py` | 2024 Basque: removed date-header misparse rows |
| `patch_pt_v44_to_v45.py`  | PT v44→v45: 8 renames + Jolene Welch merge |
| `patch_pt_v46_to_v47.py`  | PT v46→v47: 26 ALL_CAPS, 17 wrong-case, 14 merges |

## Current lock state

- **PT v47** (`inputs/identity_lock/Persons_Truth_Final_v47.csv`)
- **PBP v85** (`inputs/identity_lock/Placements_ByPerson_v85.csv`)
