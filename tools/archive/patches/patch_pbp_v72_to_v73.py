#!/usr/bin/env python3
"""
patch_pbp_v72_to_v73.py

Fixes event 948943299 (First Annual Arica Open, 2000, Arica Chile).

Problem: Doubles Net had 9 messy rows — a mix of old-format (2 rows/team with
short MD5 team_person_key) and new-format (1 row/team with piped UUIDs), plus
spurious player rows (competitor_type=player) at p1/p2/p3/p4. This created
apparent ties at every place.

Mirror truth:
  Doubles Net
  1. John Leys / Fabian
  2. Marcos Marquino / Chico Luis
  3. Lito Marley / Pelao
  (no ties)

Fix: Replace all 9 Doubles Net rows with 3 clean new-format team rows.

Person resolution:
  John Leys       → 3b938feb-b4c7-59a1-929f-7b62be77c1ce  (PT)
  Fabian          → 55a5e368-024e-54e3-8f86-4231fd82cc9d  (__NON_PERSON__, single-name)
  Marcos Marquino → e7f232a4-b31e-558a-8f15-0bc22f4f3c54  (PT)
  Chico Luis      → 26d71726-9c88-5e92-b0cf-ee991fe09e4e  (PT)
  Lito Marley     → a006d38e-4f1e-52aa-a18f-21b3813e9f99  (PT)
  Pelao           → 89a96529-7a1d-5dcc-8fd1-dd77f4afa0dc  (__NON_PERSON__, single-name)

Row counts:
  Event 948943299 Doubles Net before: 9 rows
  Event 948943299 Doubles Net after:  3 rows
  Net PBP change: -6 rows (28,846 → 28,840)

Output: inputs/identity_lock/Placements_ByPerson_v73.csv
"""

import csv
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
LOCK    = REPO / "inputs" / "identity_lock"
PBP_IN  = LOCK / "Placements_ByPerson_v72.csv"
PBP_OUT = LOCK / "Placements_ByPerson_v73.csv"

EVENT_ID   = "948943299"
DIVISION   = "Doubles Net"

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]

# Replacement rows — 3 clean team rows, new format (piped UUIDs)
REPLACEMENT_ROWS = [
    {
        "event_id":          EVENT_ID,
        "year":              "2000",
        "division_canon":    DIVISION,
        "division_category": "net",
        "place":             "1",
        "competitor_type":   "team",
        "person_id":         "",
        # John Leys (PT) | Fabian (UUID5 from "fabian", __NON_PERSON__)
        "team_person_key":   "3b938feb-b4c7-59a1-929f-7b62be77c1ce|55a5e368-024e-54e3-8f86-4231fd82cc9d",
        "person_canon":      "__NON_PERSON__",
        "team_display_name": "John Leys / Fabian",
        "coverage_flag":     "partial",
        "person_unresolved": "",
        "norm":              "",
        "division_raw":      DIVISION,
    },
    {
        "event_id":          EVENT_ID,
        "year":              "2000",
        "division_canon":    DIVISION,
        "division_category": "net",
        "place":             "2",
        "competitor_type":   "team",
        "person_id":         "",
        # Marcos Marquino (PT) | Chico Luis (PT)
        "team_person_key":   "26d71726-9c88-5e92-b0cf-ee991fe09e4e|e7f232a4-b31e-558a-8f15-0bc22f4f3c54",
        "person_canon":      "__NON_PERSON__",
        "team_display_name": "Marcos Marquino / Chico Luis",
        "coverage_flag":     "complete",
        "person_unresolved": "",
        "norm":              "",
        "division_raw":      DIVISION,
    },
    {
        "event_id":          EVENT_ID,
        "year":              "2000",
        "division_canon":    DIVISION,
        "division_category": "net",
        "place":             "3",
        "competitor_type":   "team",
        "person_id":         "",
        # Lito Marley (PT) | Pelao (UUID5 from "pelao", __NON_PERSON__)
        "team_person_key":   "a006d38e-4f1e-52aa-a18f-21b3813e9f99|89a96529-7a1d-5dcc-8fd1-dd77f4afa0dc",
        "person_canon":      "__NON_PERSON__",
        "team_display_name": "Lito Marley / Pelao",
        "coverage_flag":     "partial",
        "person_unresolved": "",
        "norm":              "",
        "division_raw":      DIVISION,
    },
]


def main() -> None:
    print(f"Reading {PBP_IN} …")
    with open(PBP_IN, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  Loaded {len(rows):,} rows")

    # Remove all Doubles Net rows for event 948943299
    removed = [r for r in rows if r["event_id"] == EVENT_ID
               and r["division_canon"] == DIVISION]
    kept    = [r for r in rows if not (r["event_id"] == EVENT_ID
               and r["division_canon"] == DIVISION)]

    print(f"\nRemoved {len(removed)} rows for {EVENT_ID} / {DIVISION!r}:")
    for r in removed:
        print(f"  p{r['place']} {r['competitor_type']:6s}  "
              f"canon={r['person_canon']!r:30s}  display={r['team_display_name']!r}")

    # Append replacement rows
    all_rows = kept + REPLACEMENT_ROWS
    print(f"\nAdded {len(REPLACEMENT_ROWS)} replacement rows:")
    for r in REPLACEMENT_ROWS:
        print(f"  p{r['place']} team  {r['team_display_name']!r}  coverage={r['coverage_flag']}")

    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PBP_FIELDS)
        w.writeheader()
        for r in all_rows:
            w.writerow({fld: r.get(fld, "") for fld in PBP_FIELDS})

    print(f"  v73 total: {len(all_rows):,} rows  (delta: {len(all_rows) - len(rows):+d})")

    # Verify event
    event_rows = [r for r in all_rows if r["event_id"] == EVENT_ID]
    print(f"\nEvent {EVENT_ID} final rows: {len(event_rows)}")
    for r in event_rows:
        print(f"  {r['division_canon']:20s}  p{r['place']}  {r['competitor_type']:6s}  "
              f"{r['team_display_name'] or r['person_canon']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
