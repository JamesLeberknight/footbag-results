#!/usr/bin/env python3
"""
patch_pbp_v73_to_v74.py

Fixes event 1000071985 (2001 Frankfurt Footbag Open).

Problem: Results were in HTML <ol><li> format. The parser only captured the
first <li> item per division, giving p1-only coverage for both divisions.

Mirror truth:
  Ladies Open Freestyle: Julia Böhm p1, Nadine Beyeler p2, Isabelle Widmer p3
  Gents Open Freestyle:  Brain McKenzie p1, Christian Löw p2, Dexter p3,
                         Ales Zelinka p4, Ole Schnack p5, Christoph Hitz p6,
                         Tomas Tyrpekl p7, Venca Runstuk p8

Fix: Remove 2 existing p1-only rows; add all 11 rows with correct placements.

Person resolution:
  Julia Böhm       → db8cc373-cc56-52c8-8e51-588d6a86ba1e  (PT)
  Nadine Beyeler   → 7d5a5f40-0f4c-5c3e-ae53-f2d1aed6af19  (PT)
  Isabelle Widmer  → bdd1f637-2375-503d-b2c3-01094f7ab049  (PT)
  Brain McKenzie   → 16aa924d-87a4-5b4a-8b80-7ed2f14da5a9  (PT)
  Christian Löw    → c859420d-4487-5539-8a89-7fd09956d790  (PT)
  Dexter           → __NON_PERSON__  (single name)
  Ales Zelinka     → f2e73bec-d859-51b9-8312-edb5a473e83e  (PT)
  Ole Schnack      → 246cf8ea-f186-5e7e-a781-3fc4205a702e  (PT)
  Christoph Hitz   → 7191ace0-3944-5433-85bb-6c80a6df482b  (PT)
  Tomas Tyrpekl    → 97b4db80-0f51-5701-9dc2-aba083758a58  (PT)
  Venca Runstuk    → unresolved (multi-token, not in PT)

Coverage flags:
  Ladies: 3/3 resolved → complete
  Gents:  9/11 resolved (Dexter=__NON_PERSON__, Venca Runstuk=unresolved)
          → 9/11 = 0.818 → mostly_complete

Row counts:
  Event 1000071985 before:  2 rows
  Event 1000071985 after:  11 rows
  Net PBP change: +9 rows (28,840 → 28,849)

Output: inputs/identity_lock/Placements_ByPerson_v74.csv
"""

import csv
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
LOCK    = REPO / "inputs" / "identity_lock"
PBP_IN  = LOCK / "Placements_ByPerson_v73.csv"
PBP_OUT = LOCK / "Placements_ByPerson_v74.csv"

EVENT_ID = "1000071985"

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]


def row(place, pid, canon, div, cflag, unres=""):
    return {
        "event_id":          EVENT_ID,
        "year":              "2001",
        "division_canon":    div,
        "division_category": "freestyle",
        "place":             str(place),
        "competitor_type":   "player",
        "person_id":         pid,
        "team_person_key":   "",
        "person_canon":      canon,
        "team_display_name": "",
        "coverage_flag":     cflag,
        "person_unresolved": unres,
        "norm":              canon.lower() if pid else "",
        "division_raw":      div,
    }


LADIES = "Ladies Open Freestyle"
GENTS  = "Gents Open Freestyle"

REPLACEMENT_ROWS = [
    # Ladies Open Freestyle — 3/3 resolved → complete
    row(1, "db8cc373-cc56-52c8-8e51-588d6a86ba1e", "Julia Böhm",      LADIES, "complete"),
    row(2, "7d5a5f40-0f4c-5c3e-ae53-f2d1aed6af19", "Nadine Beyeler",  LADIES, "complete"),
    row(3, "bdd1f637-2375-503d-b2c3-01094f7ab049", "Isabelle Widmer", LADIES, "complete"),

    # Gents Open Freestyle — 9/11 resolved → mostly_complete
    row(1, "16aa924d-87a4-5b4a-8b80-7ed2f14da5a9", "Brain McKenzie",  GENTS, "mostly_complete"),
    row(2, "c859420d-4487-5539-8a89-7fd09956d790", "Christian Löw",   GENTS, "mostly_complete"),
    row(3, "",                                       "__NON_PERSON__",  GENTS, "mostly_complete"),   # Dexter — single name
    row(4, "f2e73bec-d859-51b9-8312-edb5a473e83e", "Ales Zelinka",    GENTS, "mostly_complete"),
    row(5, "246cf8ea-f186-5e7e-a781-3fc4205a702e", "Ole Schnack",     GENTS, "mostly_complete"),
    row(6, "7191ace0-3944-5433-85bb-6c80a6df482b", "Christoph Hitz",  GENTS, "mostly_complete"),
    row(7, "97b4db80-0f51-5701-9dc2-aba083758a58", "Tomas Tyrpekl",   GENTS, "mostly_complete"),
    row(8, "",                                       "Venca Runstuk",   GENTS, "mostly_complete", "1"),  # unresolved
]


def main() -> None:
    print(f"Reading {PBP_IN} …")
    with open(PBP_IN, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  Loaded {len(rows):,} rows")

    removed = [r for r in rows if r["event_id"] == EVENT_ID]
    kept    = [r for r in rows if r["event_id"] != EVENT_ID]

    print(f"\nRemoved {len(removed)} rows for {EVENT_ID}:")
    for r in removed:
        print(f"  {r['division_canon']:25s} p{r['place']}  {r['person_canon']}")

    all_rows = kept + REPLACEMENT_ROWS
    print(f"\nAdded {len(REPLACEMENT_ROWS)} replacement rows:")
    for r in REPLACEMENT_ROWS:
        print(f"  {r['division_canon']:25s} p{r['place']}  {r['person_canon']:20s}  {r['coverage_flag']}")

    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PBP_FIELDS)
        w.writeheader()
        for r in all_rows:
            w.writerow({fld: r.get(fld, "") for fld in PBP_FIELDS})

    print(f"  v74 total: {len(all_rows):,} rows  (delta: {len(all_rows) - len(rows):+d})")
    print("\nDone.")


if __name__ == "__main__":
    main()
