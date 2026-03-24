#!/usr/bin/env python3
"""
patch_pbp_v74_to_v75.py

Fixes event 984694623 (1st Montreal Summer Freestyle Challenge, 2001, Montreal Canada).

Problem: Source data used "Name, City, Province" format. The parser treated city/province
as teammates, generating spurious doubles entries and missing all 15 correct placements.
Open Sick 10 was missed entirely. Stage2 captured 8/23 correct placements.

Mirror truth:
  Intermediate Freestyle - Routines (8 players)
  Open Freestyle - Routines (10 players)
  Open Sick 10 (5 players, with 3-way tie at p1)

Fix: Remove all 8 existing rows; add all 23 rows with correct placements.

Person resolution:
  Kaiser Ahmad              → 2d338829-... (PT)
  Simon-François Kolodenchuk → unresolved (not in PT)
  Sébastien Desgens         → 2a875256-... (PT)
  Stan Sagalovsky           → aef1fce3-... (PT, alias: "Stan Sagalovskiy")
  Caroline Bourgoin         → 13f2e206-... (PT)
  Jean-François Veillette   → unresolved (not in PT)
  Ted Fritch                → 738cbf71-... (PT, alias: "Ted Fritch" → Theodore Fritsch)
  Alexandre Colpron         → unresolved (not in PT)
  Yacine Merzouk            → 97d60d0e-... (PT)
  Alex Faber                → 16b2083f-... (PT)
  Samuel Jobin              → c57afdbe-... (PT)
  Scott Bevier              → e1b0b026-... (PT)
  Luc Legault               → c86e3f98-... (PT, alias: "Luc Legault" → Luc Legeault)
  Sébastien Duchesne        → 6dd904b7-... (PT)
  Eli Piltz                 → 1685a8aa-... (PT)
  Danny Cardonne            → d34a5fa9-... (PT)
  Xavier Beauchamp-Tremblay → unresolved (not in PT)
  Gabriel Gaudette          → 079f9c62-... (PT)
  Gordon Bevier             → 10573dc4-... (PT, nickname "Flash")

Coverage flags:
  Intermediate Freestyle: 5/8 resolved → partial (Kolodenchuk, Veillette, Colpron unresolved)
  Open Freestyle:         9/10 resolved → mostly_complete (Beauchamp-Tremblay unresolved)
  Open Sick 10:           4/5 resolved → mostly_complete (Beauchamp-Tremblay unresolved)

Row counts:
  Event 984694623 before:  8 rows
  Event 984694623 after:  23 rows
  Net PBP change: +15 rows (28,849 → 28,864)

Output: inputs/identity_lock/Placements_ByPerson_v75.csv
"""

import csv
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
LOCK    = REPO / "inputs" / "identity_lock"
PBP_IN  = LOCK / "Placements_ByPerson_v74.csv"
PBP_OUT = LOCK / "Placements_ByPerson_v75.csv"

EVENT_ID = "984694623"

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]


def row(place, pid, canon, div, div_raw, cflag, unres=""):
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
        "division_raw":      div_raw,
    }


INT_DIV  = "Intermediate Freestyle - Routines"
OPEN_DIV = "Open Freestyle - Routines"
SICK_DIV = "Open Sick 10"

# Intermediate Freestyle — 5/8 resolved → partial
# Simon-François Kolodenchuk, Jean-François Veillette, Alexandre Colpron unresolved
INT_CF = "partial"

# Open Freestyle — 9/10 resolved → mostly_complete (Xavier Beauchamp-Tremblay unresolved)
OPEN_CF = "mostly_complete"

# Open Sick 10 — 4/5 resolved → mostly_complete (Xavier Beauchamp-Tremblay unresolved)
SICK_CF = "mostly_complete"

REPLACEMENT_ROWS = [
    # Intermediate Freestyle - Routines (5/8 resolved → partial)
    row(1, "2d338829-1ec2-5f5c-9910-bccbf1252af3", "Kaiser Ahmad",               INT_DIV, INT_DIV, INT_CF),
    row(2, "",                                       "Simon-François Kolodenchuk", INT_DIV, INT_DIV, INT_CF, "1"),
    row(3, "2a875256-44b3-5eec-b0f5-185e5ca69b9f", "Sébastien Desgens",          INT_DIV, INT_DIV, INT_CF),
    row(4, "aef1fce3-d6e2-5f7b-9469-d4bd175cf0d1", "Stan Sagalovsky",            INT_DIV, INT_DIV, INT_CF),
    row(5, "13f2e206-6318-590d-b2a3-e9b90fc94c3c", "Caroline Bourgoin",          INT_DIV, INT_DIV, INT_CF),
    row(6, "",                                       "Jean-François Veillette",    INT_DIV, INT_DIV, INT_CF, "1"),
    row(7, "738cbf71-ad21-598f-a5b3-afdb8bdf543d", "Ted Fritch",                 INT_DIV, INT_DIV, INT_CF),
    row(8, "",                                       "Alexandre Colpron",          INT_DIV, INT_DIV, INT_CF, "1"),

    # Open Freestyle - Routines (9/10 resolved → mostly_complete)
    row(1,  "97d60d0e-3503-5814-8b1d-3fe6bb6bff86", "Yacine Merzouk",             OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(2,  "16b2083f-d41a-5944-8669-d41494e21159", "Alex Faber",                 OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(3,  "c57afdbe-f73a-5740-a2d3-0ec71764ee18", "Samuel Jobin",               OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(4,  "e1b0b026-9914-5ebb-8321-fe835ac96c25", "Scott Bevier",               OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(5,  "c86e3f98-1abf-59a0-bd1a-2dcb61f7e241", "Luc Legault",                OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(6,  "6dd904b7-11c6-5d62-af34-dad61040e67e", "Sébastien Duchesne",         OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(7,  "1685a8aa-0446-562c-bfe0-186e50c8c93b", "Eli Piltz",                  OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(8,  "d34a5fa9-fcb9-5031-9e74-8a2434f9cae8", "Danny Cardonne",             OPEN_DIV, OPEN_DIV, OPEN_CF),
    row(9,  "",                                       "Xavier Beauchamp-Tremblay",  OPEN_DIV, OPEN_DIV, OPEN_CF, "1"),
    row(10, "079f9c62-8932-55ad-8ae9-204ed637793f", "Gabriel Gaudette",           OPEN_DIV, OPEN_DIV, OPEN_CF),

    # Open Sick 10 (3-way tie at p1; 4/5 resolved → mostly_complete)
    row(1, "97d60d0e-3503-5814-8b1d-3fe6bb6bff86", "Yacine Merzouk",            SICK_DIV, SICK_DIV, SICK_CF),
    row(1, "10573dc4-31ee-5440-9df4-a68d11bd4801", "Gordon Bevier",              SICK_DIV, SICK_DIV, SICK_CF),
    row(1, "6dd904b7-11c6-5d62-af34-dad61040e67e", "Sébastien Duchesne",         SICK_DIV, SICK_DIV, SICK_CF),
    row(4, "",                                       "Xavier Beauchamp-Tremblay",  SICK_DIV, SICK_DIV, SICK_CF, "1"),
    row(5, "2d338829-1ec2-5f5c-9910-bccbf1252af3", "Kaiser Ahmad",               SICK_DIV, SICK_DIV, SICK_CF),
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
        print(f"  {r['division_canon']:35s} p{r['place']}  {r['person_canon']}")

    all_rows = kept + REPLACEMENT_ROWS
    print(f"\nAdded {len(REPLACEMENT_ROWS)} replacement rows:")
    for r in REPLACEMENT_ROWS:
        print(f"  {r['division_canon']:35s} p{r['place']}  {r['person_canon']:35s}  {r['coverage_flag']}")

    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PBP_FIELDS)
        w.writeheader()
        for r in all_rows:
            w.writerow({fld: r.get(fld, "") for fld in PBP_FIELDS})

    print(f"  v75 total: {len(all_rows):,} rows  (delta: {len(all_rows) - len(rows):+d})")

    event_rows = [r for r in all_rows if r["event_id"] == EVENT_ID]
    print(f"\nEvent {EVENT_ID} final rows: {len(event_rows)}")
    for r in event_rows:
        print(f"  {r['division_canon']:35s}  p{r['place']}  {r['person_canon']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
