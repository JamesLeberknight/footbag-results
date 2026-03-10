"""
tools/52_patch_placements_v45_to_v46.py
Patch Placements_ByPerson v45 → v46.

Event 1741024635 — 44th Annual IFPA World Footbag Championships 2025.

The "Battles" division was not recognised as a division header in an earlier
parser run, so its 9 individual player rows were erroneously merged into
"Open Doubles Routines" alongside the correct 3 team rows.

Current stage2 correctly separates them. Fix PBP to match.

Changes:
  - Open Doubles Routines: drop 9 spurious individual player rows
    (Battles placements that were merged into this division)
  - ADD Battles: 9 rows with correct tied-place ordering
    (1-2-3-4-5-5-7-7-9)
"""

import csv, pathlib

ROOT     = pathlib.Path(__file__).parent.parent
IN_FILE  = ROOT / "inputs/identity_lock/Placements_ByPerson_v45.csv"
OUT_FILE = ROOT / "inputs/identity_lock/Placements_ByPerson_v46.csv"

csv.field_size_limit(10**7)

EID = "1741024635"

_BATTLES_ROWS = [
    # place, person_canon  (ties: 5-5, 7-7, then 9)
    ("1",  "Jakub Mosciszewski"),
    ("2",  "Maciej Niczyporuk"),
    ("3",  "Pawel Nowak"),
    ("4",  "Mathieu Gauthier"),
    ("5",  "Tuomas Riisalo"),
    ("5",  "Santeri Karvinen"),
    ("7",  "Sergio Garcia"),
    ("7",  "Guillermo Ramirez"),
    ("9",  "Philliph Valencia"),
]


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    # Build name → person_id from all PBP rows + PT
    name_to_pid: dict[str, str] = {}
    for row in rows:
        if row["person_id"] and row["person_canon"] not in ("__NON_PERSON__", ""):
            name_to_pid[row["person_canon"]] = row["person_id"]

    pt_path = ROOT / "inputs/identity_lock/Persons_Truth_Final_v36.csv"
    with open(pt_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            canon = row["person_canon"]
            pid   = row["effective_person_id"]
            if canon and pid and canon not in name_to_pid:
                name_to_pid[canon] = pid

    # Grab template from this event
    template: dict | None = None
    for row in rows:
        if row["event_id"] == EID:
            template = row.copy()
            break
    assert template

    out_rows = []
    dropped = []

    for row in rows:
        if row["event_id"] == EID and row["division_canon"] == "Open Doubles Routines" \
                and row["competitor_type"] == "player":
            dropped.append(f"DROP Open Doubles Routines p{row['place']} {row['person_canon']}")
            continue
        out_rows.append(row)

    print(f"\nDropped {len(dropped)} rows:")
    for d in dropped:
        print(" ", d)

    added = []
    for place, pcanon in _BATTLES_ROWS:
        pid = name_to_pid.get(pcanon, "")
        if not pid:
            print(f"  WARNING: no person_id for {pcanon!r}")
        r = template.copy()
        r["division_canon"]    = "Battles"
        r["division_category"] = "freestyle"
        r["place"]             = place
        r["competitor_type"]   = "player"
        r["person_id"]         = pid
        r["team_person_key"]   = ""
        r["person_canon"]      = pcanon
        r["team_display_name"] = ""
        r["coverage_flag"]     = "complete"
        r["person_unresolved"] = ""
        r["norm"]              = pcanon.lower()
        out_rows.append(r)
        added.append(f"ADD  Battles p{place} {pcanon}")

    print(f"\nAdded {len(added)} rows:")
    for a in added:
        print(" ", a)

    print(f"\nNet change: {len(rows):,} → {len(out_rows):,}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Written: {OUT_FILE.name}")


if __name__ == "__main__":
    main()
