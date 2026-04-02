"""
patch_pt_v49_to_v50_pbp_v93_to_v94.py

Two cleanup fixes:

  Fix 1 — PT: remove dead duplicate 'Thomas Jouan' entry
    effective_person_id = ffe10517-080f-5a67-b641-3548a44d6d9e
    player_names_seen   = 'Thomas tomtom Jouan'
    Reason: zero PBP rows for this UUID; the active entry is 383d34a4.
    This duplicate causes 33_schema_logic_qc.py to error on duplicate person_canon.

  Fix 2 — PBP: remove 'imagepunkt für stuff' artifact row
    event_id = 1260368810, division = 'Results Pro Image Cash Golf', place = 1
    Reason: company/sponsor name parsed as a participant; its UUID (f846b3b2)
    is a stage2 player-token not present in PT, causing 33_schema_logic_qc.py
    to error with 'PF person_id not in PT'.
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── PT v49 → v50 ──────────────────────────────────────────────────────────────

PT_IN  = ROOT / "inputs/identity_lock/Persons_Truth_Final_v49.csv"
PT_OUT = ROOT / "inputs/identity_lock/Persons_Truth_Final_v50.csv"

DEAD_UUID = "ffe10517-080f-5a67-b641-3548a44d6d9e"

pt = pd.read_csv(PT_IN)
before_pt = len(pt)
mask_dead = pt["effective_person_id"] == DEAD_UUID
assert mask_dead.sum() == 1, f"Expected 1 row, found {mask_dead.sum()}"
pt = pt[~mask_dead].reset_index(drop=True)
print(f"PT: {before_pt} → {len(pt)} rows  (removed dead Thomas Jouan ffe10517)")
pt.to_csv(PT_OUT, index=False)
print(f"  Wrote {PT_OUT}")

# ── PBP v93 → v94 ─────────────────────────────────────────────────────────────

PBP_IN  = ROOT / "inputs/identity_lock/Placements_ByPerson_v93.csv"
PBP_OUT = ROOT / "inputs/identity_lock/Placements_ByPerson_v94.csv"

ARTIFACT_EVENT = 1260368810
ARTIFACT_DIV   = "Results Pro Image Cash Golf"
ARTIFACT_PLACE = 1
ARTIFACT_NAME  = "imagepunkt für stuff"

pbp = pd.read_csv(PBP_IN)
before_pbp = len(pbp)
mask_art = (
    (pbp["event_id"]       == ARTIFACT_EVENT) &
    (pbp["division_canon"] == ARTIFACT_DIV) &
    (pbp["place"]          == ARTIFACT_PLACE) &
    (pbp["person_canon"]   == ARTIFACT_NAME)
)
assert mask_art.sum() == 1, f"Expected 1 row, found {mask_art.sum()}"
pbp = pbp[~mask_art].reset_index(drop=True)
print(f"\nPBP: {before_pbp} → {len(pbp)} rows  (removed 'imagepunkt für stuff')")
pbp.to_csv(PBP_OUT, index=False)
print(f"  Wrote {PBP_OUT}")

print("\nDone.")
