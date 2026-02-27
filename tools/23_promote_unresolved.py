#!/usr/bin/env python3
"""
23_promote_unresolved.py — Promote high-value Unresolved entries to Truth.

Operations:
  A) BACKFILL_MERGES — remap Placements from Unresolved pid → existing Truth pid,
     update person_canon in Placements, remove from Unresolved.

  B) CLEANUP_REMOVE — remove from Unresolved where Placements are already
     correctly attributed to a Truth pid (no Placements remapping needed).

  C) PROMOTE — create new Truth row from Unresolved entry (COVERAGE_CLOSURE),
     remove from Unresolved. For multi-pid entries: pick dominant pid, remap
     minor pids to dominant in Placements first.

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v22.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v19.csv
  inputs/identity_lock/Placements_ByPerson_v23.csv

Outputs:
  inputs/identity_lock/Persons_Truth_Final_v23.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v20.csv
  inputs/identity_lock/Placements_ByPerson_v24.csv
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN = IDENTITY_LOCK / "Persons_Truth_Final_v22.csv"
UNRESOLVED_IN = IDENTITY_LOCK / "Persons_Unresolved_Organized_v19.csv"
PLACEMENTS_IN = IDENTITY_LOCK / "Placements_ByPerson_v23.csv"

TRUTH_OUT = IDENTITY_LOCK / "Persons_Truth_Final_v23.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v20.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v24.csv"

# ---------------------------------------------------------------------------
# A) BACKFILL MERGES
#    Unresolved canon → (Truth canon, Truth full pid)
# ---------------------------------------------------------------------------
BACKFILL_MERGES: dict[str, tuple[str, str]] = {
    "Sunil Tsunami Jani": (
        "Sunil Jani",
        "8f15594c-f3f9-53d9-bfe5-409e746b7c29",
    ),
    "Alex Trenner": (
        "Alexander Trenner",
        "24ffb4bb-a58e-54d8-b0d9-06b989c794cd",
    ),
}

# ---------------------------------------------------------------------------
# B) CLEANUP REMOVE
#    Remove from Unresolved only — Placements already under correct Truth pid.
# ---------------------------------------------------------------------------
CLEANUP_REMOVE: set[str] = {
    "Ken Hamric",  # Placements already use 'Ken  Hamric' Truth pid=3991f932
}

# ---------------------------------------------------------------------------
# C) PROMOTE
#    Each entry: (unresolved_canon, truth_canon, dominant_pid_prefix,
#                 [minor_pids_to_remap], [secondary_unresolved_canons_to_remove],
#                 placements_canon_override)
#
#    dominant_pid_prefix: 8-char prefix of the pid to use as Truth effective_person_id
#    minor_pids: list of 8-char prefixes to remap → dominant_pid in Placements
#    secondary_unresolved: extra Unresolved canon rows to also remove (merged)
#    placements_canon_override: if Placements use a different canon spelling
# ---------------------------------------------------------------------------
PROMOTE: list[dict] = [
    dict(unresolved_canon="Ken Somolinos",    truth_canon="Ken Somolinos",     dominant="d1aace44"),
    dict(unresolved_canon="Jim Hankins",      truth_canon="Jim Hankins",        dominant="61656316"),
    dict(unresolved_canon="Yves Kreil",       truth_canon="Yves Kreil",         dominant="b274fbcc"),
    dict(unresolved_canon="Robin Puchel",     truth_canon="Robin Puchel",       dominant="3c50ab0f"),
    dict(unresolved_canon="Eric Chang",       truth_canon="Eric Chang",         dominant="c8aa0d72"),
    dict(unresolved_canon="Fabien Riffaud",   truth_canon="Fabien Riffaud",     dominant="9337337c"),
    dict(unresolved_canon="Richard Cook",     truth_canon="Richard Cook",       dominant="619a16fc"),
    dict(unresolved_canon="Łukasz Krysiewicz", truth_canon="Łukasz Krysiewicz", dominant="75235a9f"),
    # Walt Houston + Walter R. Houston → Walt Houston
    dict(
        unresolved_canon="Walt Houston",
        truth_canon="Walt Houston",
        dominant="c6704eb0",
        minor_pids=["e68be9dc"],
        secondary_unresolved=["Walter R. Houston"],
    ),
    # Nicolas De Zeeuw: dominant 22/23 rows, minor 1/23
    dict(
        unresolved_canon="Nicolas De Zeeuw",
        truth_canon="Nicolas De Zeeuw",
        dominant="24331263",
        minor_pids=["5410d7ab"],
    ),
    # Benjamin De Bastos: dominant 20/22 rows, minors 1+1
    dict(
        unresolved_canon="Benjamin De Bastos",
        truth_canon="Benjamin De Bastos",
        dominant="df248674",
        minor_pids=["004d9889", "bdd957c5"],
    ),
    # Jessica Cedeño: Unresolved has "Jessica Cedeńo" (encoding variant),
    # Placements also have "Jessica Cedeńo"; promote with clean canon.
    dict(
        unresolved_canon="Jessica Cedeńo",
        truth_canon="Jessica Cedeño",
        dominant="d05edd33",
        placements_canon_override="Jessica Cedeńo",  # match Placements rows
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pipe_merge(base: str, addition: str) -> str:
    parts = [x.strip() for x in base.split("|") if x.strip()]
    for x in addition.split("|"):
        x = x.strip()
        if x and x not in parts:
            parts.append(x)
    return " | ".join(parts)


def find_full_pid(pf: pd.DataFrame, prefix: str, canon: str = "") -> str:
    """Find a full UUID in Placements by 8-char prefix (and optionally by canon)."""
    mask = pf["person_id"].str.startswith(prefix)
    hits = pf[mask]
    if not hits.empty:
        return hits.iloc[0]["person_id"]
    return ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    truth = pd.read_csv(TRUTH_IN, dtype=str).fillna("")
    unresolved = pd.read_csv(UNRESOLVED_IN, dtype=str).fillna("")
    placements = pd.read_csv(PLACEMENTS_IN, dtype=str).fillna("")

    print(f"Loaded Truth:       {len(truth)} rows")
    print(f"Loaded Unresolved:  {len(unresolved)} rows")
    print(f"Loaded Placements:  {len(placements)} rows")
    print()

    truth_canons = set(truth["person_canon"].str.strip())
    unresolved_canons_to_remove: set[str] = set()
    placements_remap_count = 0
    new_truth_rows: list[dict] = []

    # -------------------------------------------------------------------------
    # A) BACKFILL MERGES
    # -------------------------------------------------------------------------
    print("=== A) BACKFILL MERGES ===")
    for unres_canon, (truth_canon, truth_pid) in BACKFILL_MERGES.items():
        # Find matching Placements rows
        mask = placements["person_canon"] == unres_canon
        n = mask.sum()
        print(f"  {unres_canon!r} → {truth_canon!r}: {n} Placements rows remapped")
        placements.loc[mask, "person_id"] = truth_pid
        placements.loc[mask, "person_canon"] = truth_canon
        placements.loc[mask, "norm"] = truth_canon.lower().strip()
        placements_remap_count += n
        unresolved_canons_to_remove.add(unres_canon)

    print()

    # -------------------------------------------------------------------------
    # B) CLEANUP REMOVE
    # -------------------------------------------------------------------------
    print("=== B) CLEANUP REMOVE ===")
    for canon in CLEANUP_REMOVE:
        print(f"  Removing {canon!r} from Unresolved (Placements already covered)")
        unresolved_canons_to_remove.add(canon)
    print()

    # -------------------------------------------------------------------------
    # C) PROMOTE
    # -------------------------------------------------------------------------
    print("=== C) PROMOTE ===")
    truth_cols = list(truth.columns)

    for entry in PROMOTE:
        unres_canon = entry["unresolved_canon"]
        truth_canon = entry["truth_canon"]
        dominant = entry["dominant"]
        minor_pids = entry.get("minor_pids", [])
        secondary_unresolved = entry.get("secondary_unresolved", [])
        pf_canon = entry.get("placements_canon_override", unres_canon)

        # Safety check
        if truth_canon in truth_canons:
            print(f"  SKIP {truth_canon!r} — already in Truth")
            continue

        # Find full dominant pid
        full_pid = find_full_pid(placements, dominant, pf_canon)
        if not full_pid:
            # Try matching by canon
            rows = placements[placements["person_canon"] == pf_canon]
            rows_dom = rows[rows["person_id"].str.startswith(dominant)]
            if not rows_dom.empty:
                full_pid = rows_dom.iloc[0]["person_id"]
            else:
                print(f"  ERROR: cannot find full pid for {dominant} ({unres_canon!r})")
                return 1

        # Remap minor pids → dominant
        for minor in minor_pids:
            minor_mask = (placements["person_canon"] == pf_canon) & \
                         (placements["person_id"].str.startswith(minor))
            n_minor = minor_mask.sum()
            placements.loc[minor_mask, "person_id"] = full_pid
            placements.loc[minor_mask, "person_canon"] = truth_canon
            placements.loc[minor_mask, "norm"] = truth_canon.lower().strip()
            placements_remap_count += n_minor
            if n_minor:
                print(f"  {unres_canon!r}: remapped {n_minor} rows from minor pid={minor}")

        # Update Placements person_canon to truth_canon (encoding fix, etc.)
        pf_match_mask = (placements["person_canon"] == pf_canon) & \
                        (placements["person_id"].str.startswith(dominant))
        if pf_canon != truth_canon:
            n_fix = pf_match_mask.sum()
            placements.loc[pf_match_mask, "person_canon"] = truth_canon
            placements.loc[pf_match_mask, "norm"] = truth_canon.lower().strip()
            placements_remap_count += n_fix
            print(f"  {unres_canon!r}: canon-fixed {n_fix} Placements rows")

        # Collect player_names_seen from Placements
        all_pf_rows = placements[placements["person_id"] == full_pid]
        n_app = len(all_pf_rows)
        player_names = unres_canon  # at minimum

        # Build new Truth row
        new_row = {col: "" for col in truth_cols}
        new_row["effective_person_id"] = full_pid
        new_row["person_canon"] = truth_canon
        new_row["player_ids_seen"] = full_pid
        new_row["player_names_seen"] = player_names
        new_row["source"] = "COVERAGE_CLOSURE_v23"
        new_row["norm_key"] = truth_canon.lower().strip()
        if "person_canon.1" in new_row:
            new_row["person_canon.1"] = truth_canon
        new_truth_rows.append(new_row)
        truth_canons.add(truth_canon)

        # Mark Unresolved canons to remove
        unresolved_canons_to_remove.add(unres_canon)
        for sec in secondary_unresolved:
            unresolved_canons_to_remove.add(sec)

        print(f"  PROMOTED: {truth_canon!r}  pid={full_pid[:8]}  app={n_app}")

    print()

    # -------------------------------------------------------------------------
    # Apply changes
    # -------------------------------------------------------------------------

    # Remove from Unresolved
    unresolved_out = unresolved[
        ~unresolved["person_canon"].isin(unresolved_canons_to_remove)
    ].copy()

    # Add new Truth rows
    if new_truth_rows:
        new_df = pd.DataFrame(new_truth_rows, columns=truth_cols)
        truth_out = pd.concat([truth, new_df], ignore_index=True)
    else:
        truth_out = truth.copy()

    print("============================================================")
    print(f"Truth:       {len(truth)} → {len(truth_out)} (+{len(new_truth_rows)} new)")
    print(f"Unresolved:  {len(unresolved)} → {len(unresolved_out)} (-{len(unresolved_canons_to_remove)})")
    print(f"Placements:  {len(placements)} rows ({placements_remap_count} values updated)")
    print("============================================================")

    truth_out.to_csv(TRUTH_OUT, index=False)
    unresolved_out.to_csv(UNRESOLVED_OUT, index=False)
    placements.to_csv(PLACEMENTS_OUT, index=False)

    print(f"\nWritten: {TRUTH_OUT.name} ({len(truth_out)} rows)")
    print(f"Written: {UNRESOLVED_OUT.name} ({len(unresolved_out)} rows)")
    print(f"Written: {PLACEMENTS_OUT.name} ({len(placements)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
