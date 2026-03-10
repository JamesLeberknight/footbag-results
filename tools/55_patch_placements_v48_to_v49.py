"""
tools/55_patch_placements_v48_to_v49.py
Patch Placements_ByPerson v48 → v49.

Event 979089216 — First Annual Eugene Freestyle Freekout!!!

"45 Second Shred" and "Sick 10 Trick" each appear twice in PBP with
duplicate places — Open and Intermediate divisions were collapsed into
a single division_canon.

Changes:
  - DROP all "45 Second Shred" rows (9 garbled)
  - DROP all "Sick 10 Trick" rows (8 garbled)
  - ADD "Open 45 Second Shred" p1-p4 (Toby Robinson, Alex Zerbe, Matt Baker, Jane Jones)
  - ADD "Intermediate 45 Second Shred" p1-p5 (Jason Hall, Andrew Johnson, Logan Dethman, Erik???, Travis Strickland)
  - ADD "Open Sick 10 Trick" p1-p4 (Toby Robinson, Matt Baker, Alex Zerbe, Jane Jones)
  - ADD "Intermediate Sick 10 Trick" p1-p4 tied (Jason Hall, Andrew Johnson, Erik???, tied p4: Logan Dethman + Travis Strickland)

"Erik ???" / "Erik ?????" — unresolved name, person_unresolved=1, partial coverage.
"""

import csv, pathlib

ROOT     = pathlib.Path(__file__).parent.parent
IN_FILE  = ROOT / "inputs/identity_lock/Placements_ByPerson_v48.csv"
OUT_FILE = ROOT / "inputs/identity_lock/Placements_ByPerson_v49.csv"

csv.field_size_limit(10**7)

EID = "979089216"

# (division_canon, division_category, place, person_canon, coverage_flag)
# person_canon="" for unresolved entries
_REPLACEMENTS = [
    # Open 45 Second Shred
    ("Open 45 Second Shred", "freestyle", "1", "Toby Robinson",    "complete"),
    ("Open 45 Second Shred", "freestyle", "2", "Alex Zerbe",       "complete"),
    ("Open 45 Second Shred", "freestyle", "3", "Matt Baker",       "complete"),
    ("Open 45 Second Shred", "freestyle", "4", "Jane Jones",       "complete"),

    # Intermediate 45 Second Shred
    ("Intermediate 45 Second Shred", "freestyle", "1", "Jason Hall",        "complete"),
    ("Intermediate 45 Second Shred", "freestyle", "2", "Andrew Johnson",    "complete"),
    ("Intermediate 45 Second Shred", "freestyle", "3", "Logan Dethman",     "complete"),
    ("Intermediate 45 Second Shred", "freestyle", "4", "Erik ???",          "partial"),
    ("Intermediate 45 Second Shred", "freestyle", "5", "Travis Strickland", "complete"),

    # Open Sick 10 Trick
    ("Open Sick 10 Trick", "freestyle", "1", "Toby Robinson", "complete"),
    ("Open Sick 10 Trick", "freestyle", "2", "Matt Baker",    "complete"),
    ("Open Sick 10 Trick", "freestyle", "3", "Alex Zerbe",    "complete"),
    ("Open Sick 10 Trick", "freestyle", "4", "Jane Jones",    "complete"),

    # Intermediate Sick 10 Trick (p4 is a tie: Logan Dethman and Travis Strickland)
    ("Intermediate Sick 10 Trick", "freestyle", "1", "Jason Hall",        "complete"),
    ("Intermediate Sick 10 Trick", "freestyle", "2", "Andrew Johnson",    "complete"),
    ("Intermediate Sick 10 Trick", "freestyle", "3", "Erik ?????",        "partial"),
    ("Intermediate Sick 10 Trick", "freestyle", "4", "Logan Dethman",     "complete"),
    ("Intermediate Sick 10 Trick", "freestyle", "4", "Travis Strickland", "complete"),
]

_DROP_DIVS = {"45 Second Shred", "Sick 10 Trick"}


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

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

    template: dict | None = None
    for row in rows:
        if row["event_id"] == EID:
            template = row.copy()
            break
    assert template, f"Event {EID} not found"

    out_rows = []
    dropped = []

    for row in rows:
        if row["event_id"] == EID and row["division_canon"] in _DROP_DIVS:
            dropped.append(f"DROP {row['division_canon']} p{row['place']} {row['person_canon']}")
            continue
        out_rows.append(row)

    print(f"\nDropped {len(dropped)} rows:")
    for d in dropped:
        print(" ", d)

    added = []
    for (div, cat, place, pcanon, cflag) in _REPLACEMENTS:
        pid = name_to_pid.get(pcanon, "")
        is_unresolved = pid == "" and cflag != "partial"
        if not pid and cflag == "complete":
            print(f"  WARNING: no person_id for {pcanon!r}")

        r = template.copy()
        r["division_canon"]    = div
        r["division_category"] = cat
        r["place"]             = place
        r["competitor_type"]   = "player"
        r["person_id"]         = pid
        r["team_person_key"]   = ""
        r["person_canon"]      = pcanon
        r["team_display_name"] = ""
        r["coverage_flag"]     = cflag
        r["person_unresolved"] = "1" if not pid else ""
        r["norm"]              = pcanon.lower()
        out_rows.append(r)
        added.append(f"ADD  {div} p{place} {pcanon} ({cflag})")

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
