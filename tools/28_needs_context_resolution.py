#!/usr/bin/env python3
"""
28_needs_context_resolution.py — Resolve all 46 NEEDS_CONTEXT Unresolved entries.

Four groups:

  Group 1 — NOISE (28 entries): Doubles pairs stored as a single person_canon
    in PBP. Reclassify → __NON_PERSON__. Patterns:
      • Two names concatenated (no separator)
      • Language connectors: og (Danish/Norwegian), a (Czech), und (German), Y (Spanish)
      • Country code separator: CAN
      • Other multi-name forms

  Group 2 — UNRESOLVED_ONLY_REMOVE (3 entries): Pairs with 0 PBP rows.
    Remove from Unresolved_Organized only — no PBP change.

  Group 3 — CANON_CORRECT to existing Truth (2 entries): Corrupted/variant
    person_canon in PBP → correct canonical form already in Truth.

  Group 4 — NEW_PERSONS (13 entries): Real individuals. Create Truth row +
    assign UUID to PBP rows.

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v27.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v24.csv
  inputs/identity_lock/Placements_ByPerson_v28.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v28.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v25.csv
  inputs/identity_lock/Placements_ByPerson_v29.csv
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v27.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v24.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v28.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v28.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v25.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v29.csv"

_NS = uuid.uuid5(uuid.NAMESPACE_URL, "footbag-results-identity")

def make_uuid(canon: str) -> str:
    return str(uuid.uuid5(_NS, canon))


# ---------------------------------------------------------------------------
# Group 1 — NOISE → __NON_PERSON__ in PBP + remove from Unresolved
# ---------------------------------------------------------------------------
NOISE: set[str] = {
    # No-separator doubles pairs
    "Lonya Julin davID Butcher",           # 1997 Mixed Doubles Net p1
    "Rob Sorenson Brian Jones",            # 1997 Intermediate Doubles p8
    "Szymon Kalwak Radoslav Turek",        # 2017 Open Doubles Net p22
    "Wilfrid Vincendeau Valentin Gigaud",  # 2009 Open Doubles Net p9
    "Nicolas De Zeeuw Serge Kaldany",      # 2009 Open Doubles Net p5
    "Benjamin De Bastos Louis Marchadier", # 2017 Open Doubles Net p29
    "Boris Ollivier Marco Trentmann",      # 2024 Open Singles Freestyle p8 (Circle Contest)
    "Jacob Kunde Marc Gebauer",            # 2024 Open Singles Freestyle p9
    "Noah Haunhorst Anton Kunde",          # 2024 Open Singles Freestyle p10
    "Lena Mlakar Jereb Wlady Pachexo",     # 2024 Open Singles Freestyle p11
    "Andreas Beimel Frenzel Eduardo Martinez",  # 2024 Open Singles Freestyle p6
    # Language connectors
    "Mikkel Frederiksen og Thomas Mortensen",   # og = Danish; 2003 Open Double Net p4
    "Ole Snack og Ryan Mulroney",               # og = Danish; 2003 Open Double Net p3
    "Pavel Hejra a Petr Stejskal",             # a = Czech; 2010 Net Doubles p2
    "Andy Götze und Flo Wolff",                # und = German; 2005 Double Open Net p1
    "Christian Löwe und Hanneé Tiger",         # und = German; 2005 Net Jam p3
    "Martin a Honza Hulejovi",                 # a = Czech; 2004 Doubles Net p2
    "Bjarne Everberg og Benny Leich",          # og = Norwegian; 2003 Open Double Net p2
    "YEISON OCAMPO Y ANDRES GALLEGO",          # Y = Spanish; 2012 Open Doubles Net p7
    "SEBASTIAN CEBALLOS Y ANDRES ZAPATA",      # Y = Spanish; 2012 Open Doubles Net p2
    "EDISSON DUQUE Y GIANY",                   # Y = Spanish; 2012 Open Doubles Net p3
    "ALEX LOPEZ Y GABRIEL BOHORQUEZ",          # Y = Spanish; 2012 Open Doubles Net p5
    "ANDRES ARCE Y BERNARDO PALACIOS",         # Y = Spanish; 2012 Open Doubles Net p4
    "ANTONO LINERO Y ALBERTO PEREZ",           # Y = Spanish; 2012 Open Doubles Net p1
    # Country code separator
    "Maude Landreville CAN Lena Mlakar",  # Maude Laudreville (Truth) + Lena Mlakar; 2 PBP
    "Luke Legault CAN Lena Mlakar",       # both in Truth; 2022 Mixed Doubles Net p2
    # Other
    "Xavier Lancret Boris Julien Ollivier",  # 2017 Open Doubles Net p24; Xavier+Boris (both Truth)
    "Dexter a Pavel Èervený",                # a = Czech; Pavel Cerveny in Truth; 2010 Net Doubles p1
}

# ---------------------------------------------------------------------------
# Group 2 — UNRESOLVED_ONLY_REMOVE (0 PBP rows — remove from Unresolved only)
# ---------------------------------------------------------------------------
UNRESOLVED_ONLY_REMOVE: set[str] = {
    "Lisa Uebele Andreas Wolff",    # 0 PBP — Lisa Uebele + Andreas Wolff (both in Truth)
    "Christian Bock Christian Bruhn",  # 0 PBP — two persons, concatenated
    "Craig McNair Sage Woodmansee",    # 0 PBP — two persons, concatenated
}

# ---------------------------------------------------------------------------
# Group 3 — CANON_CORRECT to existing Truth (no new Truth entry)
#   bad_canon (PBP) → clean_canon (Truth)
# ---------------------------------------------------------------------------
CANON_CORRECT: dict[str, str] = {
    "Félix Antoine Guérard": "Félix-Antoine Guérard",  # hyphen missing; Truth has hyphenated form
    "CARLOS MEDINA":          "Carlos Medina",          # all-caps variant; Carlos Medina in Truth
}

# ---------------------------------------------------------------------------
# Group 4 — NEW_PERSONS (real individuals not yet in Truth)
# ---------------------------------------------------------------------------
NEW_PERSONS: list[str] = [
    "Paweł Rożek",                    # 1 app — 2021 Open Singles Net p5 (Polish)
    "Wiktor Dębski",                  # 1 app — 2021 Open Singles Net p1 (Polish, winner)
    "Eduardo Martinez",               # 1 app — 2021 Open Singles Net p20 (Venezuelan)
    "Cameron Dowie",                  # 2 apps — 2000 Beginners Freestyle p1 + Consecutives p3
    "Andrey Pomanov",                 # 1 app — 2009 Routine p1 (Russian event 1241031822)
    "Victor Burnham",                 # 1 app — 2009 Intermediate Routines p3
    "Andrey Egorov",                  # 1 app — 2009 Routine p2 (same Russian event)
    "Sébastien Verdy",                # 1 app — 2005 Open Doubles p1 (one partner in team)
    "Stéphane Comeau",                # 1 app — 2005 Open Doubles p3 (one partner in team)
    "Luke Anderson",                  # 1 app — 2000 Advanced Freestyle p4
    "Max Kerkoff",                    # 1 app — 2005 Open Singles Freestyle p5
    "Jamie Lepley",                   # 1 app — 2006 Open Singles Net p2
    "Edison Alejandro Rodriguez Betancur",  # 1 app — 2014 Intermedio Net p6 (4-name Latin American)
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_truth_row(cols: list[str], pid: str, canon: str, source: str) -> dict:
    row = {c: "" for c in cols}
    row["effective_person_id"] = pid
    row["person_canon"] = canon
    row["player_ids_seen"] = pid
    row["player_names_seen"] = canon
    row["source"] = source
    row["norm_key"] = canon.lower().strip()
    if "person_canon.1" in row:
        row["person_canon.1"] = canon
    return row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Resolve all 46 NEEDS_CONTEXT Unresolved entries (tool 28)."
    )
    ap.add_argument("--apply", action="store_true",
                    help="Write output files. Default is dry-run.")
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

    truth_cols   = list(truth.columns)
    truth_canons = set(truth["person_canon"].str.strip())
    new_truth_rows: list[dict] = []
    pbp_changes  = 0
    unresolved_removes = 0
    uuid_map: dict[str, str] = {}  # extended by Group 4

    # -------------------------------------------------------------------------
    # Group 1 — NOISE → __NON_PERSON__
    # -------------------------------------------------------------------------
    print("=== Group 1: NOISE → __NON_PERSON__ ===")
    for canon in sorted(NOISE):
        mask = placements["person_canon"] == canon
        n = mask.sum()
        if n == 0:
            print(f"  {canon!r}: no PBP rows")
        else:
            print(f"  {canon!r} → __NON_PERSON__  rows={n}")
            if not dry_run:
                placements.loc[mask, "person_canon"] = "__NON_PERSON__"
                placements.loc[mask, "person_id"]    = ""
                if "norm" in placements.columns:
                    placements.loc[mask, "norm"] = "__non_person__"
            pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # Group 2 — UNRESOLVED_ONLY_REMOVE (no PBP)
    # -------------------------------------------------------------------------
    print("=== Group 2: UNRESOLVED_ONLY_REMOVE ===")
    for canon in sorted(UNRESOLVED_ONLY_REMOVE):
        mask = placements["person_canon"] == canon
        n = mask.sum()
        print(f"  {canon!r}: PBP rows={n} (expected 0)")
    print()

    # -------------------------------------------------------------------------
    # Group 3 — CANON_CORRECT to existing Truth
    # -------------------------------------------------------------------------
    print("=== Group 3: CANON_CORRECT ===")
    for bad_canon, clean_canon in CANON_CORRECT.items():
        pid = ""
        truth_match = truth[truth["person_canon"] == clean_canon]
        if len(truth_match):
            pid = truth_match["effective_person_id"].iloc[0]
        if not pid:
            print(f"  ERROR: {clean_canon!r} not found in Truth — skipping {bad_canon!r}")
            continue
        mask = placements["person_canon"] == bad_canon
        n = mask.sum()
        if n == 0:
            print(f"  {bad_canon!r}: no PBP rows")
        else:
            print(f"  {bad_canon!r} → {clean_canon!r}  pid={pid[:8]}  rows={n}")
            if not dry_run:
                placements.loc[mask, "person_canon"] = clean_canon
                placements.loc[mask, "person_id"]    = pid
                if "norm" in placements.columns:
                    placements.loc[mask, "norm"] = clean_canon.lower().strip()
            pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # Group 4 — NEW_PERSONS
    # -------------------------------------------------------------------------
    print("=== Group 4: NEW_PERSONS ===")
    for canon in NEW_PERSONS:
        if canon in truth_canons:
            print(f"  SKIP {canon!r} — already in Truth")
            continue
        pid = make_uuid(canon)
        uuid_map[canon] = pid
        mask = placements["person_canon"] == canon
        n_pbp = mask.sum()
        if not dry_run:
            placements.loc[mask, "person_id"] = pid
        pbp_changes += n_pbp
        row = build_truth_row(truth_cols, pid, canon, "NEW_PERSON_v28")
        new_truth_rows.append(row)
        truth_canons.add(canon)
        print(f"  NEW: {canon!r}  pid={pid[:8]}  pbp_rows={n_pbp}")
    print()

    # -------------------------------------------------------------------------
    # Unresolved cleanup: remove all resolved canons
    # -------------------------------------------------------------------------
    print("=== UNRESOLVED CLEANUP ===")
    canons_to_remove = (
        NOISE
        | UNRESOLVED_ONLY_REMOVE
        | set(CANON_CORRECT.keys())
        | set(NEW_PERSONS)
    )
    for canon in sorted(canons_to_remove):
        ur_mask = unresolved["person_canon"] == canon
        n_ur = ur_mask.sum()
        if n_ur == 0:
            print(f"  {canon!r}: not in Unresolved (skip)")
            continue
        print(f"  removing {n_ur} row(s): {canon!r}")
        if not dry_run:
            unresolved = unresolved[~ur_mask]
        unresolved_removes += n_ur
    print()

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    n_new = len(new_truth_rows)
    truth_final_len = len(truth) + n_new
    ur_final = len(unresolved) if not dry_run else len(unresolved) - unresolved_removes

    print("=" * 60)
    print(f"Truth:      {len(truth)} → {truth_final_len} (+{n_new} new rows)")
    print(f"Unresolved: {len(unresolved)} → {ur_final} (-{unresolved_removes} rows)")
    print(f"Placements: {len(placements)} rows ({pbp_changes} values updated)")
    print("=" * 60)

    if dry_run:
        print("\nDRY RUN complete — pass --apply to write output files.")
        return 0

    if new_truth_rows:
        new_df = pd.DataFrame(new_truth_rows, columns=truth_cols)
        truth_out = pd.concat([truth, new_df], ignore_index=True)
    else:
        truth_out = truth.copy()

    truth_out.to_csv(TRUTH_OUT, index=False)
    unresolved.to_csv(UNRESOLVED_OUT, index=False)
    placements.to_csv(PLACEMENTS_OUT, index=False)

    print(f"\nWritten: {TRUTH_OUT.name} ({len(truth_out)} rows)")
    print(f"Written: {UNRESOLVED_OUT.name} ({len(unresolved)} rows)")
    print(f"Written: {PLACEMENTS_OUT.name} ({len(placements)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
