#!/usr/bin/env python3
"""
26_merge_unresolved_variants.py — Merge Unresolved near-duplicate variants into Truth.

Track B: six Unresolved persons are trivially the same person as an existing Truth entry.
  - Élise Regreny  → Elise Regreny    (diacritic É→E, same Net player 2017-2022)
  - Krystof Maléø  → Krystof Maler    (encoding corruption ø→r, same Czech player 2008-2018)
  - Jorden Moirs   → Jorden Moir      (trailing s, same player 2003-2011)
  - Andy Ronalds   → Andy Ronald      (trailing s, same player 1997-2005)
  - James Deans    → James Dean       (trailing s, same 1997 beginner)
  - Christian Loewe → Christian Loew  (spelling variant, 0 PBP rows — Unresolved only)

Track A (stale cleanup): 10 entries in Unresolved_Organized now in Truth (promoted by tools
  23-25). Remove them from the curated file.

For each Track B merge:
  - PBP: remap person_canon → truth_canon and assign truth UUID (for rows with PBP presence).
  - Unresolved_Organized: remove the merged entry.

Track A entries are simply dropped from Unresolved_Organized (no PBP changes needed —
  those persons already have correct UUIDs in PBP).

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v25.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v22.csv
  inputs/identity_lock/Placements_ByPerson_v26.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v26.csv     (unchanged copy — Truth not modified)
  inputs/identity_lock/Persons_Unresolved_Organized_v23.csv
  inputs/identity_lock/Placements_ByPerson_v27.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v25.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v22.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v26.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v26.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v23.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v27.csv"

# ---------------------------------------------------------------------------
# B) UNRESOLVED_MERGE
#    {unresolved_canon: (truth_canon, truth_uuid)}
#    Remap PBP rows from unresolved_canon → truth_canon + truth_uuid.
#    Entry removed from Unresolved_Organized.
# ---------------------------------------------------------------------------
UNRESOLVED_MERGE: dict[str, tuple[str, str]] = {
    "Élise Regreny":  ("Elise Regreny", "181795ec-8320-577b-8183-4cc4798f019b"),
    "Krystof Maléø":  ("Krystof Maler",  "33021da9-34aa-56fa-95f2-f31a6b2b6bca"),
    "Jorden Moirs":   ("Jorden Moir",    "16aae952-d265-5e4d-81b8-5474f6c43802"),
    "Andy Ronalds":   ("Andy Ronald",    "39bc6c51-d2e0-5930-8677-51828c12de14"),
    "James Deans":    ("James Dean",     "6257b1c3-bbae-5937-b773-ffd5c2152715"),
    # Christian Loewe has 0 PBP rows — Unresolved entry removed, no PBP change
    "Christian Loewe": ("Christian Loew", "1805e38e-adb4-52ab-9d96-281024c565d9"),
}

# ---------------------------------------------------------------------------
# A) STALE_REMOVE
#    Persons promoted to Truth by tools 23-25 but still in Unresolved_Organized.
#    Drop from Unresolved_Organized only — PBP already correct.
# ---------------------------------------------------------------------------
STALE_REMOVE: set[str] = {
    "Brent Welch",
    "Ian Pfeiffer",
    "Toxic Tom B.",
    "Garikoitz Casquero",
    "Josu Royuela",
    "Jose Cocolan",
    "Olivier Fages",
    "Baptiste Supan",
    "James Geraci",   # appears twice in Unresolved — both instances removed
}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Merge Unresolved near-duplicate variants into Truth + stale cleanup."
    )
    ap.add_argument("--apply", action="store_true",
                    help="Write output files. Default is dry-run (print only).")
    args = ap.parse_args()
    dry_run = not args.apply

    if dry_run:
        print("DRY RUN — pass --apply to write output files.\n")

    truth      = pd.read_csv(TRUTH_IN,      dtype=str).fillna("")
    unresolved = pd.read_csv(UNRESOLVED_IN, dtype=str).fillna("")
    placements = pd.read_csv(PLACEMENTS_IN, dtype=str).fillna("")

    print(f"Loaded Truth:      {len(truth)} rows")
    print(f"Loaded Unresolved: {len(unresolved)} rows")
    print(f"Loaded Placements: {len(placements)} rows")
    print()

    pbp_changes = 0
    unresolved_removes = 0

    # -------------------------------------------------------------------------
    # B) UNRESOLVED_MERGE — remap PBP + remove from Unresolved
    # -------------------------------------------------------------------------
    print("=== B) UNRESOLVED_MERGE ===")
    for bad_canon, (good_canon, good_uuid) in UNRESOLVED_MERGE.items():
        mask = placements["person_canon"] == bad_canon
        n = mask.sum()
        ur_mask = unresolved["person_canon"] == bad_canon
        n_ur = ur_mask.sum()

        if n > 0:
            print(f"  {bad_canon!r} → {good_canon!r}  pid={good_uuid[:8]}  pbp={n}  unresolved={n_ur}")
            if not dry_run:
                placements.loc[mask, "person_canon"] = good_canon
                placements.loc[mask, "person_id"]    = good_uuid
                if "norm" in placements.columns:
                    placements.loc[mask, "norm"] = good_canon.lower().strip()
            pbp_changes += n
        else:
            print(f"  {bad_canon!r} → {good_canon!r}  (0 PBP rows)  unresolved={n_ur}")

        if n_ur > 0:
            if not dry_run:
                unresolved = unresolved[~ur_mask]
            unresolved_removes += n_ur
        else:
            print(f"    WARN: {bad_canon!r} not found in Unresolved_Organized")
    print()

    # -------------------------------------------------------------------------
    # A) STALE_REMOVE — drop from Unresolved only
    # -------------------------------------------------------------------------
    print("=== A) STALE_REMOVE ===")
    for canon in sorted(STALE_REMOVE):
        ur_mask = unresolved["person_canon"] == canon
        n_ur = ur_mask.sum()
        if n_ur == 0:
            print(f"  {canon!r}: not found in Unresolved (already removed?)")
            continue
        print(f"  {canon!r}: removing {n_ur} row(s) from Unresolved")
        if not dry_run:
            unresolved = unresolved[~ur_mask]
        unresolved_removes += n_ur
    print()

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    ur_final = len(unresolved) - unresolved_removes if dry_run else len(unresolved)
    if dry_run:
        ur_final = len(unresolved) - unresolved_removes

    print("=" * 60)
    print(f"Truth:      {len(truth)} → {len(truth)} (unchanged)")
    print(f"Unresolved: {len(unresolved)} → {ur_final} (-{unresolved_removes} rows)")
    print(f"Placements: {len(placements)} rows ({pbp_changes} values updated)")
    print("=" * 60)

    if dry_run:
        print("\nDRY RUN complete — pass --apply to write output files.")
        return 0

    truth.to_csv(TRUTH_OUT, index=False)
    unresolved.to_csv(UNRESOLVED_OUT, index=False)
    placements.to_csv(PLACEMENTS_OUT, index=False)

    print(f"\nWritten: {TRUTH_OUT.name} ({len(truth)} rows)")
    print(f"Written: {UNRESOLVED_OUT.name} ({len(unresolved)} rows)")
    print(f"Written: {PLACEMENTS_OUT.name} ({len(placements)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
