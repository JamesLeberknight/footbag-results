#!/usr/bin/env python3
"""
patch_pbp_v71_to_v72.py

Resolves quarantined event 910551956 (1999 Western Regional Footbag Championships).

Changes applied:
  1. Name corrections (mirror as truth):
       - Tuan Vu → Tu Vu          (Open Singles Freestyle p8)
       - Chris Seibert → Chris Siebert  (Open Singles Net p7)
       - Jimmy Caveney → Jim Caveney    (Open Singles Net p10)
       - Brian Fournier → Bryan Fournier (Intermediate Singles Freestyle p4)

  2. Structural correction: split "Novice Doubles Net" into three separate
     divisions per the multi-day mirror structure:
       - Novice Doubles Net (Saturday)  — 4 teams
       - Novice Doubles Net (Sunday)    — 3 teams
       - Novice Doubles Net (Monday)    — 4 teams

     Place numbers are reassigned to match each day's standings exactly.

Row counts:
  Event 910551956 before: 105 rows
  Event 910551956 after:  107 rows (+2 because the Zebulon/Avery team that
                          placed both Sunday-3rd and Monday-1st needs two
                          correctly-placed rows; old merged data had 11 Novice
                          rows, new split data has 11 rows across 3 divisions)
  Net PBP change: 0 rows (105 → 105; the split reallocates, does not add)

Output: inputs/identity_lock/Placements_ByPerson_v72.csv
"""

import csv
import sys
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
LOCK    = REPO / "inputs" / "identity_lock"
PBP_IN  = LOCK / "Placements_ByPerson_v71.csv"
PBP_OUT = LOCK / "Placements_ByPerson_v72.csv"

EVENT_ID = "910551956"

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]

# ── Name corrections ─────────────────────────────────────────────────────────
# Applied to (division_canon, place, old_canon) → new_canon
# Only for player rows (competitor_type == "player") in event 910551956.
NAME_FIXES: dict[tuple[str, str, str], str] = {
    ("Open Singles Freestyle",       "8",  "Tuan Vu"):       "Tu Vu",
    ("Open Singles Net",             "7",  "Chris Seibert"): "Chris Siebert",
    ("Open Singles Net",             "10", "Jimmy Caveney"): "Jim Caveney",
    ("Intermediate Singles Freestyle","4", "Brian Fournier"):"Bryan Fournier",
}

# ── Novice Doubles Net split ──────────────────────────────────────────────────
# Identifies the 11 existing merged rows and assigns them to the correct day.
# Key: (place_as_str, team_display_name_startswith) → (new_division, new_place)
# team_display_name_startswith is enough to uniquely identify each row.

NOVICE_SPLIT: list[tuple[str, str, str, str]] = [
    # (old_place, display_prefix,         new_division,                    new_place)
    ("1", "Mike Ingle",    "Novice Doubles Net (Saturday)", "1"),
    ("2", "Jeff Lowe",     "Novice Doubles Net (Saturday)", "2"),
    ("3", "Jeff Howse / Brian", "Novice Doubles Net (Saturday)", "3"),
    ("4", "Louis Brasher", "Novice Doubles Net (Saturday)", "4"),

    ("1", "Jeff Howse / Maggi", "Novice Doubles Net (Sunday)",   "1"),
    ("2", "Joel Neilson",  "Novice Doubles Net (Sunday)",   "2"),
    # Zebulon p3 → Sunday p3; Zebulon p1 → Monday p1 (same team_person_key)
    ("3", "Zebulon Jones", "Novice Doubles Net (Sunday)",   "3"),

    # Monday: identified by old place values not yet assigned above
    ("1", "Zebulon Jones", "Novice Doubles Net (Monday)",   "1"),
    ("2", "Kevin Fine",    "Novice Doubles Net (Monday)",   "2"),
    ("3", "Sonny Fahimi",  "Novice Doubles Net (Monday)",   "3"),
    ("4", "Travis Tips",   "Novice Doubles Net (Monday)",   "4"),
]


def apply_name_fix(row: dict) -> dict:
    """Fix person_canon and norm for the 4 known name corrections."""
    if row["event_id"] != EVENT_ID or row["competitor_type"] != "player":
        return row
    key = (row["division_canon"], row["place"], row["person_canon"])
    if key in NAME_FIXES:
        new_canon = NAME_FIXES[key]
        row = dict(row)
        row["person_canon"] = new_canon
        row["norm"] = new_canon.lower()
        print(f"  NAME FIX: {key[0]} p{key[1]}: '{key[2]}' → '{new_canon}'")
    return row


def split_novice_doubles(rows: list[dict]) -> list[dict]:
    """
    Replace all 'Novice Doubles Net' rows for event 910551956 with the
    correctly split Saturday/Sunday/Monday rows.
    """
    # Separate novice rows from the rest
    other_rows   = [r for r in rows if not (r["event_id"] == EVENT_ID
                                            and r["division_canon"] == "Novice Doubles Net")]
    novice_rows  = [r for r in rows if     (r["event_id"] == EVENT_ID
                                            and r["division_canon"] == "Novice Doubles Net")]

    print(f"  Novice Doubles Net rows to reclassify: {len(novice_rows)}")

    # Build a lookup: (old_place, display_prefix) → row
    # There are two Zebulon rows (old p1 and old p3) — track separately.
    # We iterate NOVICE_SPLIT in order and pop matched rows from a working list.
    remaining = list(novice_rows)
    new_novice: list[dict] = []

    for old_place, prefix, new_div, new_place in NOVICE_SPLIT:
        matched = None
        for i, r in enumerate(remaining):
            if (r["place"] == old_place
                    and r["team_display_name"].startswith(prefix)):
                matched = remaining.pop(i)
                break
        if matched is None:
            print(f"  WARNING: no match for old_place={old_place!r} prefix={prefix!r}",
                  file=sys.stderr)
            continue
        updated = dict(matched)
        updated["division_canon"] = new_div
        updated["division_raw"]   = new_div
        updated["place"]          = new_place
        new_novice.append(updated)
        print(f"  NOVICE SPLIT: '{matched['team_display_name']}' "
              f"p{old_place} → {new_div} p{new_place}")

    if remaining:
        print(f"  WARNING: {len(remaining)} unmatched novice rows left over:",
              file=sys.stderr)
        for r in remaining:
            print(f"    p{r['place']} {r['team_display_name']}", file=sys.stderr)

    return other_rows + new_novice


def main() -> None:
    print(f"Reading {PBP_IN} …")
    with open(PBP_IN, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  Loaded {len(rows):,} rows")

    print("\nApplying name corrections …")
    rows = [apply_name_fix(r) for r in rows]

    print("\nSplitting Novice Doubles Net …")
    rows = split_novice_doubles(rows)

    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PBP_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({fld: r.get(fld, "") for fld in PBP_FIELDS})

    print(f"  v72 total: {len(rows):,} rows")

    # Verify event row count
    event_rows = [r for r in rows if r["event_id"] == EVENT_ID]
    divs = sorted({r["division_canon"] for r in event_rows})
    print(f"\nEvent {EVENT_ID} row count: {len(event_rows)}")
    print("Divisions:")
    for d in divs:
        cnt = sum(1 for r in event_rows if r["division_canon"] == d)
        print(f"  {d}: {cnt} rows")

    print("\nDone. Run pipeline with Placements_ByPerson_v72.csv")


if __name__ == "__main__":
    main()
