#!/usr/bin/env python3
"""
30_noise_cleanup.py — Reclassify clearly non-person entries remaining in Unresolved v26.

All 17 entries are __NON_PERSON__: initials/asterisks, gaming handles, offensive handles,
theatrical nicknames, parsing artifacts, or words with no plausible person-name
interpretation in any language.

Borderline single-word cases (Nino, Doro, Dorner, Dubuis, Boriskov, Weiss, Tyrpekl,
Rush, Lotus, Leuch, etc.) are left in Unresolved — they may be real persons using
a single name or nickname.

Inputs:
  inputs/identity_lock/Persons_Unresolved_Organized_v26.csv
  inputs/identity_lock/Placements_ByPerson_v30.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Unresolved_Organized_v27.csv
  inputs/identity_lock/Placements_ByPerson_v31.csv

Note: Persons_Truth is unchanged — no new persons added.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v26.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v30.csv"

UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v27.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v31.csv"

# ---------------------------------------------------------------------------
# NOISE → __NON_PERSON__ in PBP + remove from Unresolved
# ---------------------------------------------------------------------------
NOISE: set[str] = {
    # Clearly not person names (symbols / initialisms)
    "G*",          # letter + asterisk, not a name
    "*",           # asterisk only
    "FLT",         # bare initialism — no known person
    "MLS",         # bare initialism — no known person
    "Footcraft",   # portmanteau handle, not a person name
    "Winner",      # placeholder label, not a name
    "Jeremy Watlers-prizes",  # parsing artifact — "prizes" is a notes field, not surname
    # Unambiguous gaming / online handles
    "Reaper",      # gaming handle
    "Gobbish",     # handle / gibberish
    "Zerg",        # StarCraft race name, gaming handle
    "Fingerbang",  # offensive handle
    "LAbitch",     # offensive handle
    "Jester",      # theatrical / online handle
    "Herra X",     # Finnish for "Mr. X" — explicit pseudonym
    "Skaut",       # Czech for "scout" — online handle
    # Handles with no plausible person-name basis
    "Rake",        # tool name used as a handle
    "Dreuf",       # no linguistic basis as a real name in any known language
}


def run(apply: bool) -> None:
    unresolved = pd.read_csv(UNRESOLVED_IN, low_memory=False)
    pbp = pd.read_csv(PLACEMENTS_IN, low_memory=False)

    print(f"Unresolved in: {len(unresolved)} rows")
    print(f"PBP in:        {len(pbp)} rows")
    print(f"NOISE entries: {len(NOISE)}")
    print()

    # Verify every NOISE entry exists in Unresolved
    ur_canons = set(unresolved["person_canon"].dropna())
    for c in sorted(NOISE):
        in_ur = c in ur_canons
        n_pbp = (pbp["person_canon"] == c).sum()
        print(f"  {'found' if in_ur else 'MISSING'} Unresolved | {n_pbp:2d} PBP | {c!r}")

    print()
    if not apply:
        print("=== DRY RUN (pass --apply to execute) ===")
        total_pbp = sum((pbp["person_canon"] == c).sum() for c in NOISE)
        print(f"Would reclassify {total_pbp} PBP rows → __NON_PERSON__")
        print(f"Would remove {len(NOISE)} rows from Unresolved "
              f"({len(unresolved)} → {len(unresolved) - len(NOISE & ur_canons)})")
        return

    # Apply: set __NON_PERSON__ in PBP
    noise_mask = pbp["person_canon"].isin(NOISE)
    pbp.loc[noise_mask, "person_canon"] = "__NON_PERSON__"
    pbp.loc[noise_mask, "person_id"] = float("nan")
    pbp.loc[noise_mask, "person_unresolved"] = False
    print(f"Set {noise_mask.sum()} PBP rows → __NON_PERSON__")

    # Remove from Unresolved
    before = len(unresolved)
    unresolved = unresolved[~unresolved["person_canon"].isin(NOISE)]
    print(f"Unresolved: {before} → {len(unresolved)} (-{before - len(unresolved)} rows)")

    unresolved.to_csv(UNRESOLVED_OUT, index=False)
    pbp.to_csv(PLACEMENTS_OUT, index=False)
    print()
    print(f"Written: {UNRESOLVED_OUT.name} ({len(unresolved)} rows)")
    print(f"Written: {PLACEMENTS_OUT.name} ({len(pbp)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write output files (default: dry run)")
    args = parser.parse_args()
    run(apply=args.apply)


if __name__ == "__main__":
    main()
