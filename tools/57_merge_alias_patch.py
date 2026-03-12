"""
tools/57_merge_alias_patch.py
──────────────────────────────
Apply 5 confirmed identity merges from ALIAS_MATCH analysis + 5 new ADD_ALIAS entries.

Merges (loser → winner):
  Anthony Mehok  (0d68e5a5) → Tony Mehok       (d2e6589e)  [0 pbp rows]
  Henry Rautio   (04ec794c) → Heikki Rautio    (98cee1b4)  [2 pbp rows]
  Jacob Hall     (aad02764) → Jakob Hall        (998b1b7c)  [1 pbp row]
  Johnny Leys    (50358c1f) → John Leys         (3b938feb)  [0 pbp rows; John legacyid=12279]
  Laurence DuMont(e9ba4bd8) → Lawrence Dumont   (44a4a929)  [0 pbp rows]

ADD_ALIAS only (no PT change):
  Bryan S. Nelson        → Byran Nelson
  Errol H. Stryker       → Erroll Stryker
  Forrester Winterburn   → Forra
  Jonathan F. Araquistain → Jonathan Felipe Araquistain
  Nathan Oates           → Nate Oates

Ambiguous ALIAS_MATCH entries (marked uncertain, no action):
  Cody Carr / Corey L. Carr
  Janne Eronen / Johanna A. Eronen
  Shawn Fisher / Steven M. Fisher
  Rodney Cook / Richard Cook

Produces:
  inputs/identity_lock/Persons_Truth_Final_v42.csv  (v41 −5 rows)
  inputs/identity_lock/Placements_ByPerson_v61.csv  (v60 with redirected pids)
  overrides/person_aliases.csv                       (5 ADD_ALIAS appended)

Usage:
    .venv/bin/python tools/57_merge_alias_patch.py [--dry-run]
"""
from __future__ import annotations
import argparse, csv, sys, shutil
from pathlib import Path
csv.field_size_limit(sys.maxsize)

ROOT  = Path(__file__).resolve().parent.parent
ILOCK = ROOT / "inputs" / "identity_lock"

PT_IN   = ILOCK / "Persons_Truth_Final_v41.csv"
PT_OUT  = ILOCK / "Persons_Truth_Final_v42.csv"
PBP_IN  = ILOCK / "Placements_ByPerson_v60.csv"
PBP_OUT = ILOCK / "Placements_ByPerson_v61.csv"
ALIASES = ROOT / "overrides" / "person_aliases.csv"

# ── merge decisions ───────────────────────────────────────────────────────────
# (loser_pid, loser_canon, winner_pid, winner_canon)
MERGES = [
    ("0d68e5a5-9068-5484-8468-e21ea144dbc6", "Anthony Mehok",
     "d2e6589e-1f6c-538d-8426-a2747597fe2f", "Tony Mehok"),
    ("04ec794c-27b3-5d3f-a9c0-4058fbaa133d", "Henry Rautio",
     "98cee1b4-8771-5d83-b62c-24d9dcba0450", "Heikki Rautio"),
    ("aad02764-b994-59c7-8ac6-d71457dc5253", "Jacob Hall",
     "998b1b7c-26da-5fd3-8957-4398616d55b9", "Jakob Hall"),
    ("50358c1f-03f8-543e-b4b2-a059b9fc0abb", "Johnny Leys",
     "3b938feb-b4c7-59a1-929f-7b62be77c1ce", "John Leys"),
    ("e9ba4bd8-0453-5cc4-923f-c1e398dd530f", "Laurence DuMont",
     "44a4a929-c3fc-5e7e-8398-8c795cd5dc8a", "Lawrence Dumont"),
]
LOSER_PIDS = {m[0] for m in MERGES}

# ── new alias-only entries ────────────────────────────────────────────────────
# (alias_text, winner_pid, winner_canon)
ADD_ALIASES = [
    ("Bryan S. Nelson",          "3a9a28f9-3065-55af-8f68-049c7cf09cc9", "Byran Nelson"),
    ("Errol H. Stryker",         "c256196e-aeb4-5011-9dce-81719a307619", "Erroll Stryker"),
    ("Forrester Winterburn",      "cafcd1b7-1e6c-54a2-befb-09e51f1f9813", "Forra"),
    ("Jonathan F. Araquistain",  "081e9873-5730-55c0-86ec-4fe1706d52e0", "Jonathan Felipe Araquistain"),
    ("Nathan Oates",             "d909eae1-a7da-53e8-ab94-4dc6df3c5120", "Nate Oates"),
]

# loser_canon → winner_canon for alias-only merges (loser name becomes an alias)
# These are also added to person_aliases.csv
MERGE_ALIASES = [(m[1], m[2], m[3]) for m in MERGES]  # (loser_canon, winner_pid, winner_canon)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    dry = args.dry_run

    # ── 1. Read PT ─────────────────────────────────────────────────────────────
    with open(PT_IN, encoding="utf-8") as f:
        pt_rows = list(csv.DictReader(f))
        pt_fields = list(pt_rows[0].keys())

    # Build pid→row map for winners so we can update their aliases column
    winner_rows: dict[str, dict] = {}
    for m in MERGES:
        _, _, wpid, _ = m
        for row in pt_rows:
            if row["effective_person_id"] == wpid:
                winner_rows[wpid] = row
                break

    # Remove losers; update winners' aliases column
    new_pt = []
    for row in pt_rows:
        pid = row["effective_person_id"]
        if pid in LOSER_PIDS:
            # Find the loser's canon
            loser_canon = next(m[1] for m in MERGES if m[0] == pid)
            winner_pid  = next(m[2] for m in MERGES if m[0] == pid)
            # Append loser_canon to winner's aliases column
            wr = winner_rows[winner_pid]
            existing_aliases = wr.get("aliases", "").strip()
            if loser_canon not in existing_aliases:
                wr["aliases"] = (existing_aliases + " | " + loser_canon).lstrip(" | ")
            print(f"  REMOVE: {loser_canon} ({pid[:8]}) → merged into {wr['person_canon']}")
            continue
        new_pt.append(row)

    print(f"\nPT: {len(pt_rows)} → {len(new_pt)} rows (−{len(pt_rows)-len(new_pt)})")
    assert len(new_pt) == len(pt_rows) - len(MERGES), "Row count mismatch"

    # ── 2. Read PBP and redirect loser pids ───────────────────────────────────
    with open(PBP_IN, encoding="utf-8") as f:
        pbp_rows = list(csv.DictReader(f))
        pbp_fields = list(pbp_rows[0].keys())

    pid_remap = {m[0]: m[2] for m in MERGES}
    redirected = 0
    for row in pbp_rows:
        old = row.get("person_id", "")
        if old in pid_remap:
            row["person_id"] = pid_remap[old]
            redirected += 1

    print(f"PBP: {len(pbp_rows)} rows, {redirected} redirected")

    # ── 3. Build alias rows to append ────────────────────────────────────────
    # Read existing aliases to avoid duplicates
    existing_alias_texts: set[str] = set()
    with open(ALIASES, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            existing_alias_texts.add(row.get("alias", "").strip())

    new_alias_rows = []
    # From ADD_ALIASES
    for alias_text, wpid, wcanon in ADD_ALIASES:
        if alias_text.strip() not in existing_alias_texts:
            new_alias_rows.append({
                "alias": alias_text.strip(),
                "person_id": wpid,
                "person_canon": wcanon,
                "status": "suggested",
                "notes": "ALIAS_MATCH via footbag.org member ID enrichment",
            })
    # From MERGE losers (loser name becomes alias for winner)
    for loser_canon, wpid, wcanon in MERGE_ALIASES:
        if loser_canon.strip() not in existing_alias_texts:
            new_alias_rows.append({
                "alias": loser_canon.strip(),
                "person_id": wpid,
                "person_canon": wcanon,
                "status": "verified",
                "notes": "merged identity — confirmed same person via footbag.org member ID",
            })

    print(f"Aliases to append: {len(new_alias_rows)}")
    for r in new_alias_rows:
        print(f"  {r['alias']!r} → {r['person_canon']!r}  [{r['status']}]")

    if dry:
        print("\n[DRY RUN — no files written]")
        return

    # ── 4. Write outputs ──────────────────────────────────────────────────────
    with open(PT_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pt_fields)
        w.writeheader()
        w.writerows(new_pt)
    print(f"\nWrote {PT_OUT} ({len(new_pt)} rows)")

    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pbp_fields)
        w.writeheader()
        w.writerows(pbp_rows)
    print(f"Wrote {PBP_OUT} ({len(pbp_rows)} rows)")

    # Append aliases
    with open(ALIASES, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["alias","person_id","person_canon","status","notes"])
        w.writerows(new_alias_rows)
    print(f"Appended {len(new_alias_rows)} rows to {ALIASES}")


if __name__ == "__main__":
    main()
