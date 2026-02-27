#!/usr/bin/env python3
"""
21_promote_v20.py — Truth merge (G23) + Unresolved round-2 cleanup

Phase B: G23 Truth consolidation
  Merge 3 Juan Palomino Truth rows → 1 canonical row
  KEEP: 'juan David Palomino Mosquera' (466ed7b5) → fix canon to 'Juan David Palomino Mosquera'
  MERGE: 'JUAN DAVID PALOMINO' (ca7eab65, 0 app)
  MERGE: 'Juan Palomino Mosquera' (00953160, 0 app)

Phase C: Unresolved round-2 cleanup
  Canon remaps: strip remaining country/city/nickname noise
  Non-person: Rorz/Forra (single-token ambiguous), Matthias Schmidtt (0 app noise)

Reads:  inputs/identity_lock/Persons_Truth_Final_v19.csv
        inputs/identity_lock/Persons_Unresolved_Organized_v17.csv
        inputs/identity_lock/Placements_ByPerson_v20.csv

Writes: inputs/identity_lock/Persons_Truth_Final_v20.csv
        inputs/identity_lock/Persons_Unresolved_Organized_v18.csv
        inputs/identity_lock/Placements_ByPerson_v21.csv   (if any rows changed)

Usage:
  python tools/21_promote_v20.py          # dry run
  python tools/21_promote_v20.py --apply
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

ROOT         = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v19.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v17.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v20.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v20.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v18.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v21.csv"

# ---------------------------------------------------------------------------
# Phase B: G23 Truth merge
# ---------------------------------------------------------------------------
# (keep_pid, new_canon, [merge_pids])
G23_MERGE = (
    "466ed7b5-b107-5e4c-9c44-8f8474773d97",   # KEEP (1 placement)
    "Juan David Palomino Mosquera",             # fix canon (lowercase j → J)
    [
        "ca7eab65-1e29-57a2-a17c-76958f64a2d6", # JUAN DAVID PALOMINO (0 app)
        "00953160-7695-5a8c-8719-346c6392a91e", # Juan Palomino Mosquera (0 app)
    ],
)

# ---------------------------------------------------------------------------
# Phase C: Unresolved round-2 canon remaps
# ---------------------------------------------------------------------------
CANON_REMAP: dict[str, str] = {
    "Milan Ardalic Slovenia":          "Milan Ardalic",
    "Audrey Tumelin France":           "Audrey Tumelin",
    "Eduardo Martinez Venezuela":      "Eduardo Martinez",
    "Wiktor Dębski Poland":            "Wiktor Dębski",
    "Paweł Rożek Poland":              "Paweł Rożek",
    "Jim Penske Rexburg":              "Jim Penske",
    "James Geraci Austin":             "James Geraci",
    "Francois Depatie Pelletier ID":   "Francois Depatie Pelletier",
    "Peter 'The Executioner Irish":    "Peter Irish",
    "Martin Coté":                     "Martin Cote",
}

NONPERSON: set[str] = {
    "Rorz Logan HackStars",   # single-token ambiguous nickname
    "Forra Logan HackStars",  # single-token ambiguous nickname
    "Matthias Schmidtt",      # 0-app noise / typo
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_RE_PUNCT = re.compile(r"[^\w\s]")
_RE_WS    = re.compile(r"\s+")


def normalize_key(name: str) -> str:
    s = unicodedata.normalize("NFKD", name.strip())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = _RE_PUNCT.sub(" ", s)
    return _RE_WS.sub(" ", s).strip()


def pipe_merge(a: str, b: str) -> str:
    parts = set(x.strip() for x in (a + "|" + b).split("|") if x.strip())
    return "|".join(sorted(parts))


def append_note(existing: str, note: str) -> str:
    existing = existing.strip()
    return existing if not existing else (
        existing if note in existing else existing + " | " + note
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dry = not args.apply

    tru = pd.read_csv(TRUTH_IN,      dtype=str).fillna("")
    unr = pd.read_csv(UNRESOLVED_IN, dtype=str).fillna("")
    pla = pd.read_csv(PLACEMENTS_IN, dtype=str).fillna("")

    print(f"Truth v19:      {len(tru)} rows")
    print(f"Unresolved v17: {len(unr)} rows")
    print(f"Placements v20: {len(pla)} rows")
    print()

    truth_canons = set(tru["person_canon"].str.strip())

    # ---- Phase B: G23 Truth merge ----
    keep_pid, new_canon, merge_pids = G23_MERGE
    keep_mask = tru["effective_person_id"] == keep_pid
    assert keep_mask.sum() == 1, f"KEEP pid not found: {keep_pid}"

    # Absorb player_ids_seen / player_names_seen / aliases from merged rows
    for merge_pid in merge_pids:
        m_mask = tru["effective_person_id"] == merge_pid
        if not m_mask.any():
            print(f"  WARNING: merge pid not found: {merge_pid}")
            continue
        for col in ("player_ids_seen", "player_names_seen", "aliases"):
            if col in tru.columns:
                merge_val = tru.loc[m_mask, col].iloc[0]
                keep_val  = tru.loc[keep_mask, col].iloc[0]
                tru.loc[keep_mask, col] = pipe_merge(keep_val, merge_val)

    # Fix canon on KEEP row
    tru.loc[keep_mask, "person_canon"] = new_canon
    tru.loc[keep_mask, "norm_key"]     = normalize_key(new_canon)

    # Remove merged rows
    tru = tru[~tru["effective_person_id"].isin(merge_pids)].copy()

    # Remap Placements for merged pids (0-app pids — should be no-op)
    pla_remapped = 0
    for merge_pid in merge_pids:
        m = pla["person_id"] == merge_pid
        if m.any():
            pla.loc[m, "person_id"]    = keep_pid
            pla.loc[m, "person_canon"] = new_canon
            pla_remapped += m.sum()

    # Also update the KEEP row's placement canons (lowercase j fix)
    keep_old_canon = "juan David Palomino Mosquera"
    pla_canon_fix = pla["person_canon"] == keep_old_canon
    pla.loc[pla_canon_fix, "person_canon"] = new_canon
    pla_remapped += int(pla_canon_fix.sum())

    print(f"Phase B (G23): Truth {3345}→{len(tru)} (-{3345-len(tru)}), "
          f"{pla_remapped} Placements rows updated")
    truth_canons = set(tru["person_canon"].str.strip())

    # ---- Phase C: Unresolved round-2 ----
    remap_note = "2026-02-26: canon cleaned (stripped country/city/nickname noise)"
    for old, new in CANON_REMAP.items():
        mask = unr["person_canon"] == old
        if not mask.any():
            continue
        unr.loc[mask, "person_canon"] = new
        unr.loc[mask, "token_count"]  = str(len(new.split()))
        unr.loc[mask, "norm_key"]     = normalize_key(new)
        unr.loc[mask, "notes"]        = unr.loc[mask, "notes"].apply(
            lambda n: append_note(n, remap_note)
        )
        # Update Placements
        pm = pla["person_canon"] == old
        if pm.any():
            pla.loc[pm, "person_canon"] = new
            pla_remapped += int(pm.sum())

    # Non-person → remove from Unresolved, update Placements
    np_removed = 0
    np_pla = 0
    for c in NONPERSON:
        pm = pla["person_canon"] == c
        if pm.any():
            pla.loc[pm, "person_canon"] = "__NON_PERSON__"
            pla.loc[pm, "person_id"]    = ""
            np_pla += int(pm.sum())
        um = unr["person_canon"] == c
        np_removed += int(um.sum())

    # Remove exits-to-Truth and NONPERSON from Unresolved
    exits = unr["person_canon"].isin(truth_canons)
    keep_mask_unr = (~exits) & (~unr["person_canon"].isin(NONPERSON))
    exits_count = int(exits.sum())
    unr = unr[keep_mask_unr].copy()

    print(f"Phase C (Unresolved round-2):")
    print(f"  exits-to-Truth:  {exits_count}")
    print(f"  NONPERSON drops: {np_removed}  ({np_pla} Placements → __NON_PERSON__)")
    print(f"  Unresolved: 312 → {len(unr)}")
    print()

    # Sanity check: no Unresolved canon in Truth
    collisions = set(unr["person_canon"]) & truth_canons
    if collisions:
        print(f"WARNING: {len(collisions)} Unresolved canons still in Truth!")
        for c in sorted(collisions):
            print(f"  {c!r}")

    print(f"=== {'DRY RUN' if dry else 'APPLYING'} ===")
    print(f"  Truth:      v19 ({3345}) → v20 ({len(tru)})  (-{3345-len(tru)})")
    print(f"  Unresolved: v17 (312)    → v18 ({len(unr)})  (-{312-len(unr)})")
    print(f"  Placements: v20/v21 ({len(pla)}, {pla_remapped} rows updated)")

    if dry:
        print("\nDry run. Pass --apply to write files.")
        return 0

    tru.to_csv(TRUTH_OUT,      index=False)
    unr.to_csv(UNRESOLVED_OUT, index=False)
    pla.to_csv(PLACEMENTS_OUT, index=False)
    print(f"\nWrote: {TRUTH_OUT} ({len(tru)} rows)")
    print(f"Wrote: {UNRESOLVED_OUT} ({len(unr)} rows)")
    print(f"Wrote: {PLACEMENTS_OUT} ({len(pla)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
