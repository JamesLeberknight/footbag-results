#!/usr/bin/env python3
"""
27_coverage_closure_and_noise.py — Track C (COVERAGE_CLOSURE) + Track D (NOISE).

Track C — COVERAGE_CLOSURE: real athletes promoted from Unresolved to Truth.
  C1) NEW_PERSONS (17): Create Truth row + assign UUID to PBP rows.
  C2) CANON_CORRECT (5): Fix corrupted/abbreviated person_canon in PBP,
      pointing to an existing or newly-added Truth entry.

Track D — NOISE (6): Non-person entries reclassified as __NON_PERSON__ in PBP.

Unresolved cleanup: all resolved/reclassified canons removed from
  Persons_Unresolved_Organized.

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v26.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v23.csv
  inputs/identity_lock/Placements_ByPerson_v27.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v27.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v24.csv
  inputs/identity_lock/Placements_ByPerson_v28.csv
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v26.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v23.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v27.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v27.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v24.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v28.csv"

# ---------------------------------------------------------------------------
# Deterministic UUID generation (project-specific namespace)
# ---------------------------------------------------------------------------
_NS = uuid.uuid5(uuid.NAMESPACE_URL, "footbag-results-identity")

def make_uuid(canon: str) -> str:
    return str(uuid.uuid5(_NS, canon))


# ---------------------------------------------------------------------------
# C1) NEW_PERSONS
#    Real athletes confirmed via event context. UUID generated deterministically.
#    PBP rows with matching person_canon get the new UUID assigned.
#    Entry removed from Unresolved_Organized.
# ---------------------------------------------------------------------------
NEW_PERSONS: list[str] = [
    "AJ Shultz",            # 5 apps — Sick 3/Shred 30, 2013–2014 (p2, p4)
    "EJ Gammage",           # 4 apps — Circle Contest 2018–2019 (p1–p4)
    "T.J. Boutorwick",      # 4 apps — Sick 3, 2007–2010
    "Maciej Długoszek",     # 4 apps — Polish events, 2014–2015
    "Aaron De Glanville",   # 4 apps — Australian events, 2013
    "Kamil Hucał",          # 3 apps — Polish events, 2013–2016
    "Natalia Fry",          # 3 apps — UK/European Net, 2011–2013
    "C.J. Zohrer",          # 2 apps — Sick 3 + Shred 30, 2010
    "JB Pinto",             # 2 apps — Portuguese events, 2018
    "JJ Jones",             # 2 apps — Net Open Singles, 2003 (p1 twice)
    "Jana Čačáková",        # 2 apps — Czech events, 2012–2013
    "Kamil Burzyński",      # 2 apps — Polish events, 2014
    "Lukáš Blažek",         # 2 apps — Czech events, 2010–2012
    "Matyáš Mach",          # 2 apps — Czech events, 2012–2013
    "Rafał Piórkowski",     # 2 apps — Polish events, 2012–2013
    "Tomasz Strzałkowski",  # 2 apps — Polish events, 2014–2015
    "Alex Lopez",           # 5 apps — Sick 3 p1, 2012 (normalised from ALEX LOPEZ)
]

# Pre-computed UUIDs for reference (make_uuid output):
# AJ Shultz           → 071ff943-...
# EJ Gammage          → 03e4d710-...
# T.J. Boutorwick     → 4120a537-...
# Maciej Długoszek    → ae5e7424-...
# Aaron De Glanville  → eea1063c-...
# Kamil Hucał         → 8def240f-...
# Natalia Fry         → d47ff105-...
# C.J. Zohrer         → 81e8e278-...
# JB Pinto            → 7c0b9eff-...
# JJ Jones            → 8fc6832d-...
# Jana Čačáková       → bd0010df-...
# Kamil Burzyński     → 3ac0b59f-...
# Lukáš Blažek        → dcd4f256-...
# Matyáš Mach         → ad38435b-...
# Rafał Piórkowski    → 23959e00-...
# Tomasz Strzałkowski → e599aabd-...
# Alex Lopez          → 0d90bb3c-...


# ---------------------------------------------------------------------------
# C2) CANON_CORRECT
#    Corrupted / abbreviated person_canon in PBP → correct Truth canon.
#    UUID resolved at runtime from Truth (pre-existing or added in C1).
#
#    bad_canon (PBP)            → clean_canon (Truth)
# ---------------------------------------------------------------------------
CANON_CORRECT: dict[str, str] = {
    "Luka W.-Lavallée":    "Luka Weyler",    # W.=Weyler double-surname; Luka Weyler in Truth (25 apps)
    "JAN CERMAK":          "Honza Cermak",   # Jan=Honza (Czech nickname); Honza Cermak in Truth
    "Jim Hankins ID":      "Jim Hankins",    # " ID" = Idaho location suffix; Jim Hankins in Truth (45 apps)
    "Robert McCloskey ID": "Rob McCloskey",  # " ID" = Idaho location suffix; Rob McCloskey in Truth (18 apps)
    "ALEX LOPEZ":          "Alex Lopez",     # all-caps variant; Alex Lopez added in C1 above
}

# ---------------------------------------------------------------------------
# D) NOISE
#    Not real persons. Reclassify person_canon → __NON_PERSON__ in PBP.
#    Entries removed from Unresolved_Organized.
# ---------------------------------------------------------------------------
NOISE: set[str] = {
    "TG Sux",              # team / joke name — 3 PBP rows (2003 Circle Contest)
    "Footbag Team Moscow", # club / team name — 3 PBP rows (2009 event)
    "de finales",          # Spanish "of finals" — bracket section label
    "places",              # English position label — noise
    "st",                  # ordinal-suffix noise (e.g. "1st" split incorrectly)
    "Fcky",                # abbreviated noise token
}


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
        description="Track C (COVERAGE_CLOSURE) + Track D (NOISE) identity resolution."
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

    truth_cols   = list(truth.columns)
    truth_canons = set(truth["person_canon"].str.strip())
    new_truth_rows: list[dict] = []
    pbp_changes  = 0
    unresolved_removes = 0

    # Runtime UUID map (extended by C1)
    uuid_map: dict[str, str] = {}

    # -------------------------------------------------------------------------
    # C1) NEW_PERSONS
    # -------------------------------------------------------------------------
    print("=== C1) NEW_PERSONS ===")
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
        row = build_truth_row(truth_cols, pid, canon, "NEW_PERSON_v27")
        new_truth_rows.append(row)
        truth_canons.add(canon)
        print(f"  NEW: {canon!r}  pid={pid[:8]}  pbp_rows={n_pbp}")
    print()

    # -------------------------------------------------------------------------
    # C2) CANON_CORRECT
    # -------------------------------------------------------------------------
    print("=== C2) CANON_CORRECT ===")
    for bad_canon, clean_canon in CANON_CORRECT.items():
        # Resolve UUID: prefer uuid_map (just added), then existing Truth
        pid = uuid_map.get(clean_canon, "")
        if not pid:
            truth_match = truth[truth["person_canon"] == clean_canon]
            if len(truth_match):
                pid = truth_match["effective_person_id"].iloc[0]
        if not pid:
            print(f"  ERROR: no UUID for {clean_canon!r} — skipping {bad_canon!r}")
            continue
        mask = placements["person_canon"] == bad_canon
        n = mask.sum()
        if n == 0:
            print(f"  {bad_canon!r}: no PBP rows found")
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
    # D) NOISE
    # -------------------------------------------------------------------------
    print("=== D) NOISE ===")
    for canon in sorted(NOISE):
        mask = placements["person_canon"] == canon
        n = mask.sum()
        if n == 0:
            print(f"  {canon!r}: no PBP rows found")
            continue
        print(f"  {canon!r} → __NON_PERSON__  rows={n}")
        if not dry_run:
            placements.loc[mask, "person_canon"] = "__NON_PERSON__"
            placements.loc[mask, "person_id"]    = ""
            if "norm" in placements.columns:
                placements.loc[mask, "norm"] = "__non_person__"
        pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # Unresolved cleanup
    #   Remove entries for:
    #   - newly promoted persons (NEW_PERSONS clean canons)
    #   - old/bad CANON_CORRECT canons (the corrupted names in Unresolved)
    #   - NOISE canons
    # -------------------------------------------------------------------------
    print("=== UNRESOLVED CLEANUP ===")
    canons_to_remove = (
        set(NEW_PERSONS)
        | set(CANON_CORRECT.keys())
        | NOISE
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

    # Apply Truth additions
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
