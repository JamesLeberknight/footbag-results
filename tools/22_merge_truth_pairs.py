#!/usr/bin/env python3
"""
22_merge_truth_pairs.py — Apply approved Truth-to-Truth merges for REC-E and REC-I.

REC-E: Jakob Wagner Revstein (5e59054a) → Jakob Wagner (c90a00eb)
  - Both are in Truth; Revstein is an alias used at one event (2008 Swiss Open, Sick 3)
  - 1 Placements row remapped to c90a00eb / 'Jakob Wagner'

REC-I: Noah Jay Bohn (3f3c94b7) → Noah Jay (b1ad926c)
  - Both are in Truth; "Bohn" last name dropped in most results
  - 1 Placements row remapped to b1ad926c / 'Noah Jay'

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v21.csv
  inputs/identity_lock/Placements_ByPerson_v22.csv

Outputs:
  inputs/identity_lock/Persons_Truth_Final_v22.csv
  inputs/identity_lock/Placements_ByPerson_v23.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN = IDENTITY_LOCK / "Persons_Truth_Final_v21.csv"
PLACEMENTS_IN = IDENTITY_LOCK / "Placements_ByPerson_v22.csv"

TRUTH_OUT = IDENTITY_LOCK / "Persons_Truth_Final_v22.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v23.csv"


def pipe_merge(base: str, addition: str) -> str:
    """Merge pipe-separated lists, dedup, preserve order."""
    parts = [x.strip() for x in base.split("|") if x.strip()]
    for x in addition.split("|"):
        x = x.strip()
        if x and x not in parts:
            parts.append(x)
    return " | ".join(parts)


MERGES = [
    # (MERGE_pid_prefix, KEEP_pid_prefix, label)
    ("5e59054a", "c90a00eb", "REC-E: Jakob Wagner Revstein → Jakob Wagner"),
    ("3f3c94b7", "b1ad926c", "REC-I: Noah Jay Bohn → Noah Jay"),
]


def main() -> int:
    truth = pd.read_csv(TRUTH_IN, dtype=str).fillna("")
    placements = pd.read_csv(PLACEMENTS_IN, dtype=str).fillna("")

    print(f"Loaded Truth: {len(truth)} rows ({TRUTH_IN.name})")
    print(f"Loaded Placements: {len(placements)} rows ({PLACEMENTS_IN.name})")
    print()

    merge_pids_to_remove: list[str] = []
    placements_remaps: list[tuple[str, str, str]] = []  # (old_pid, new_pid, new_canon)

    for merge_prefix, keep_prefix, label in MERGES:
        print(f"--- {label} ---")

        merge_rows = truth[truth["effective_person_id"].str.startswith(merge_prefix)]
        keep_rows = truth[truth["effective_person_id"].str.startswith(keep_prefix)]

        if merge_rows.empty:
            print(f"  ERROR: MERGE pid {merge_prefix} not found in Truth")
            return 1
        if keep_rows.empty:
            print(f"  ERROR: KEEP pid {keep_prefix} not found in Truth")
            return 1

        merge_row = merge_rows.iloc[0]
        keep_row = keep_rows.iloc[0]
        merge_pid = merge_row["effective_person_id"]
        keep_pid = keep_row["effective_person_id"]
        keep_canon = keep_row["person_canon"]

        print(f"  MERGE: {merge_pid[:8]} {merge_row['person_canon']!r}")
        print(f"  KEEP:  {keep_pid[:8]} {keep_canon!r}")

        # Count Placements rows affected
        pf_merge = placements[placements["person_id"] == merge_pid]
        print(f"  Placements rows to remap: {len(pf_merge)}")

        placements_remaps.append((merge_pid, keep_pid, keep_canon))
        merge_pids_to_remove.append(merge_pid)

        # Update Truth KEEP row: merge player_ids_seen, player_names_seen
        keep_idx = keep_rows.index[0]
        truth.at[keep_idx, "player_ids_seen"] = pipe_merge(
            keep_row["player_ids_seen"], merge_row["player_ids_seen"]
        )
        truth.at[keep_idx, "player_names_seen"] = pipe_merge(
            keep_row["player_names_seen"], merge_row["player_names_seen"]
        )
        # Absorb aliases
        if merge_row["aliases"]:
            truth.at[keep_idx, "aliases"] = pipe_merge(
                keep_row["aliases"], merge_row["aliases"]
            )

        print()

    # Remove MERGE rows from Truth
    truth_out = truth[~truth["effective_person_id"].isin(merge_pids_to_remove)].copy()
    print(f"Truth: {len(truth)} → {len(truth_out)} (-{len(merge_pids_to_remove)} merged)")

    # Remap Placements
    remap_count = 0
    for old_pid, new_pid, new_canon in placements_remaps:
        mask = placements["person_id"] == old_pid
        placements.loc[mask, "person_id"] = new_pid
        placements.loc[mask, "person_canon"] = new_canon
        placements.loc[mask, "norm"] = new_canon.lower().strip()
        remap_count += mask.sum()
    print(f"Placements remapped: {remap_count} rows")

    # Write outputs
    truth_out.to_csv(TRUTH_OUT, index=False)
    placements.to_csv(PLACEMENTS_OUT, index=False)
    print()
    print(f"Written: {TRUTH_OUT.name} ({len(truth_out)} rows)")
    print(f"Written: {PLACEMENTS_OUT.name} ({len(placements)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
