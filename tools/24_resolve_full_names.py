#!/usr/bin/env python3
"""
24_resolve_full_names.py — Resolve full-name unresolved placements.

Investigated 28 unresolved placements that carry full person names.
Grouped into five work items:

  A) UUID_BACKFILL   — person_canon has a UUID in most PBP rows but one row
                       is missing the link. Fill in the known UUID.

  B) TRUTH_PROMOTE   — UUID exists in PBP but person is absent from Truth
                       (orphans). Create Truth rows.

  C) NEW_PERSONS     — No UUID anywhere. Generate deterministic UUID,
                       create Truth row, assign UUID to all PBP rows.

  D) CANON_CORRECT   — Encoding-corrupted person_canon in PBP. Rename to
                       clean canon and assign correct UUID (existing or new).

  E) NOISE           — Entries that are not real people.
                       Reclassify person_canon → __NON_PERSON__ in PBP.

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v23.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v20.csv
  inputs/identity_lock/Placements_ByPerson_v24.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v24.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v21.csv  (unchanged copy)
  inputs/identity_lock/Placements_ByPerson_v25.csv
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v23.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v20.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v24.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v24.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v21.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v25.csv"

# ---------------------------------------------------------------------------
# Deterministic UUID generation (project-specific namespace)
# ---------------------------------------------------------------------------
_NS = uuid.uuid5(uuid.NAMESPACE_URL, "footbag-results-identity")

def make_uuid(canon: str) -> str:
    return str(uuid.uuid5(_NS, canon))


# ---------------------------------------------------------------------------
# A) UUID_BACKFILL
#    PBP rows where person_canon has a UUID in other rows but one row is empty.
#    {person_canon: full_uuid}
# ---------------------------------------------------------------------------
UUID_BACKFILL: dict[str, str] = {
    # Group A: in Truth, UUID in most rows, 1 PBP row missing
    "Tina Lewis":       "54e16a85-0204-5ef1-aa92-a09c9af8ae1c",
    "James Roberts":    "12144bd1-1a64-5e79-ba8c-2e64c2de69f9",
    "Derric Scalf":     "b692f69a-35c0-5cdf-b786-a3ce86d7d9d6",
    "Tim Vozar":        "aea29c23-9cfe-5025-8ff4-ad3341304468",
    "Brendan Erskine":  "2b72150a-23a0-5c22-864d-86e32a01f185",
    "Windsen Pan":      "c035902d-8658-5b8d-8de2-4079452de49d",
    "Ander López":      "ee50c606-ce87-50bb-bfd4-2e3c53e14ede",
    "Monica Sandoval":  "64ce4eb8-0db7-5f91-9c06-6c504e9cec79",
    "Josh Bast":        "c383fcb7-76ce-5f22-874c-c9aaacfe0ccc",
    "Mike Lopez":       "778326dc-8646-52dc-82c4-f0c274a2b447",
    # Group B: UUID in PBP, absent from Truth (Truth rows added in section B)
    "Brent Welch":      "95643c20-ec23-57a5-a742-5b44534ae959",
    # Curtis Taylor: already in Truth as f3a1b132 (source=data_only); handled in F) UUID_REMAP
    "Curtis Taylor":    "f3a1b132-58a5-5743-9590-e2ad379146f3",  # Truth UUID, not deefac3b
    "James Geraci":     "e979f25e-5e69-5516-8bd9-15e44cef6e19",
}

# ---------------------------------------------------------------------------
# B) TRUTH_PROMOTE
#    Persons with a UUID in PBP but no Truth row (orphans).
#    {person_canon: full_uuid}
# ---------------------------------------------------------------------------
TRUTH_PROMOTE: dict[str, str] = {
    "Brent Welch":   "95643c20-ec23-57a5-a742-5b44534ae959",
    # Curtis Taylor already in Truth (f3a1b132, source=data_only) — no promote needed
    "James Geraci":  "e979f25e-5e69-5516-8bd9-15e44cef6e19",
}

# ---------------------------------------------------------------------------
# C) NEW_PERSONS
#    No UUID anywhere. UUID will be generated deterministically.
# ---------------------------------------------------------------------------
NEW_PERSONS: list[str] = [
    "Jocelyn Sandoval",  # 2003
    "Steven Sevilla",    # 2003
    "Jose Cocolan",      # 2003
    "Baptiste Supan",    # 2004
    "Olivier Fages",     # 2004
    "Garikoitz Casquero", # 2025 Basque
    "Josu Royuela",      # 2025 Basque
    "Toxic Tom B.",      # 1997 (nickname, 2 appearances)
    "Mikuláš Čáp",      # 2015 Czech (clean form of corrupted Mikulá¹ Èáp)
    "Tomáš Mirovský",   # 2015 Czech (clean form of corrupted Tomá¹ Mirovský)
]

# ---------------------------------------------------------------------------
# D) CANON_CORRECT
#    Encoding-corrupted person_canon in PBP → (clean_canon, uuid).
#    UUID must be in Truth (pre-existing or added in section B/C).
# ---------------------------------------------------------------------------
# Note: Mikuláš Čáp and Tomáš Mirovský UUIDs are computed in section C
# and stored in _new_uuid_map before section D runs.
CANON_CORRECT: dict[str, str] = {
    # corrupted_in_pbp → clean_canon (UUID resolved at runtime)
    "Robin P¸chel":    "Robin Puchel",    # already in Truth (3c50ab0f)
    "Mikulá¹ Èáp":    "Mikuláš Čáp",    # new Truth row from section C
    "Tomá¹ Mirovský": "Tomáš Mirovský",  # new Truth row from section C
}

# Pre-known UUID for Robin Puchel (already in Truth_v23)
KNOWN_UUIDS: dict[str, str] = {
    "Robin Puchel": "3c50ab0f-2056-50ce-a5f2-c7c95e96702f",
}

# ---------------------------------------------------------------------------
# E) NOISE
#    Not real people. Reclassify to __NON_PERSON__ in PBP.
# ---------------------------------------------------------------------------
NOISE: set[str] = {
    "thru 8th",              # range notation parsed as placement
    "Marc Weber* Bob Silva", # two people concatenated (1997 Novice Doubles)
    "Gregor Morel Ale¹ Pelko",  # two people concatenated (2017 Doubles Net)
}

# ---------------------------------------------------------------------------
# F) UUID_REMAP
#    PBP rows carry a UUID that doesn't match the Truth entry for that canon.
#    Remap all matching (person_canon, wrong_uuid) rows to the correct Truth UUID.
#    {person_canon: (wrong_uuid, correct_uuid)}
# ---------------------------------------------------------------------------
UUID_REMAP: dict[str, tuple[str, str]] = {
    # Curtis Taylor: PBP used deefac3b (orphan, not in Truth).
    # Truth already has Curtis Taylor under f3a1b132 (source=data_only).
    "Curtis Taylor": (
        "deefac3b-8a4f-5924-b17c-aeb4f2c5e86e",  # wrong (PBP orphan)
        "f3a1b132-58a5-5743-9590-e2ad379146f3",   # correct (Truth canonical)
    ),
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
    ap = argparse.ArgumentParser(description="Resolve full-name unresolved placements.")
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

    truth_cols    = list(truth.columns)
    truth_canons  = set(truth["person_canon"].str.strip())
    new_truth_rows: list[dict] = []
    pbp_changes   = 0

    # Build runtime UUID map (pre-known + will be extended by section C)
    uuid_map: dict[str, str] = dict(KNOWN_UUIDS)

    # -------------------------------------------------------------------------
    # A) UUID_BACKFILL
    # -------------------------------------------------------------------------
    print("=== A) UUID_BACKFILL ===")
    for canon, pid in UUID_BACKFILL.items():
        mask = (placements["person_canon"] == canon) & (placements["person_id"].str.strip() == "")
        n = mask.sum()
        if n == 0:
            print(f"  {canon!r}: no empty rows (already filled)")
            continue
        print(f"  {canon!r}: filling {n} row(s) with {pid[:8]}...")
        if not dry_run:
            placements.loc[mask, "person_id"] = pid
        pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # B) TRUTH_PROMOTE
    # -------------------------------------------------------------------------
    print("=== B) TRUTH_PROMOTE ===")
    for canon, pid in TRUTH_PROMOTE.items():
        if canon in truth_canons:
            print(f"  SKIP {canon!r} — already in Truth")
            continue
        # Count appearances
        n_app = (placements["person_id"] == pid).sum()
        row = build_truth_row(truth_cols, pid, canon, "BACKFILL_ORPHAN_v24")
        new_truth_rows.append(row)
        truth_canons.add(canon)
        uuid_map[canon] = pid
        print(f"  PROMOTED: {canon!r}  pid={pid[:8]}  appearances={n_app}")
    print()

    # -------------------------------------------------------------------------
    # C) NEW_PERSONS
    # -------------------------------------------------------------------------
    print("=== C) NEW_PERSONS ===")
    for canon in NEW_PERSONS:
        if canon in truth_canons:
            print(f"  SKIP {canon!r} — already in Truth")
            continue
        pid = make_uuid(canon)
        uuid_map[canon] = pid
        # Count existing PBP rows by canon (before UUID assigned)
        n_pbp = (placements["person_canon"] == canon).sum()
        # Assign UUID in PBP
        mask = placements["person_canon"] == canon
        if not dry_run:
            placements.loc[mask, "person_id"] = pid
        pbp_changes += n_pbp
        # Add Truth row
        n_app_truth = (placements["person_id"] == pid).sum() if not dry_run else n_pbp
        row = build_truth_row(truth_cols, pid, canon, "NEW_PERSON_v24")
        new_truth_rows.append(row)
        truth_canons.add(canon)
        print(f"  NEW: {canon!r}  pid={pid[:8]}  pbp_rows={n_pbp}")
    print()

    # -------------------------------------------------------------------------
    # D) CANON_CORRECT
    # -------------------------------------------------------------------------
    print("=== D) CANON_CORRECT ===")
    for bad_canon, clean_canon in CANON_CORRECT.items():
        pid = uuid_map.get(clean_canon, "")
        if not pid:
            print(f"  ERROR: no UUID found for clean canon {clean_canon!r} — skipping {bad_canon!r}")
            continue
        mask = placements["person_canon"] == bad_canon
        n = mask.sum()
        if n == 0:
            print(f"  {bad_canon!r}: no PBP rows found")
            continue
        print(f"  {bad_canon!r} → {clean_canon!r}  pid={pid[:8]}  rows={n}")
        if not dry_run:
            placements.loc[mask, "person_canon"] = clean_canon
            placements.loc[mask, "person_id"]    = pid
            if "norm" in placements.columns:
                placements.loc[mask, "norm"] = clean_canon.lower().strip()
        pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # E) NOISE
    # -------------------------------------------------------------------------
    print("=== E) NOISE ===")
    for canon in NOISE:
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
    # F) UUID_REMAP
    # -------------------------------------------------------------------------
    print("=== F) UUID_REMAP ===")
    for canon, (wrong_pid, correct_pid) in UUID_REMAP.items():
        mask = (placements["person_canon"] == canon) & (placements["person_id"] == wrong_pid)
        n = mask.sum()
        if n == 0:
            print(f"  {canon!r}: no rows with wrong UUID {wrong_pid[:8]}")
            continue
        print(f"  {canon!r}: remapping {n} row(s) {wrong_pid[:8]}... → {correct_pid[:8]}...")
        if not dry_run:
            placements.loc[mask, "person_id"] = correct_pid
        pbp_changes += n
    print()

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    n_new = len(new_truth_rows)
    truth_final_len = len(truth) + n_new

    print("=" * 60)
    print(f"Truth:      {len(truth)} → {truth_final_len} (+{n_new} new rows)")
    print(f"Unresolved: {len(unresolved)} → {len(unresolved)} (unchanged)")
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
