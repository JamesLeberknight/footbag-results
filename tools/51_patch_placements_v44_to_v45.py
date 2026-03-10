"""
tools/51_patch_placements_v44_to_v45.py
Patch Placements_ByPerson v44 → v45.

Event 1378666423 — Danish Footbag Open 2013.

The event had preliminary pool rounds and finals. Stage2 collapsed all
sub-events under the same division_canon ("Circle", "Routines"), producing
garbled PBP rows with multiple competitors at the same place.

Changes:
  - Circle:  replace 16 garbled rows with 4 finals rows
             (p4 Anssi Sundberg inferred from score listing, coverage=partial)
  - Routines: replace 14 garbled rows with 9 finals rows
  - Request Contest: replace 2 __NON_PERSON__ team rows (p5, p9) with
             individual rows for each tied competitor (1-2-3-4-5-5-5-5-9-9-9-12-13)
  - ADD Big 1: 5 new rows (trick-based circle format, finals only)
  - ADD Sick 3: 3 new rows
  - ADD Danish Championships: 3 new rows (Routines, Danish athletes only)
  - Shred 30: unchanged (10 rows already correct)
"""

import csv, pathlib

ROOT     = pathlib.Path(__file__).parent.parent
IN_FILE  = ROOT / "inputs/identity_lock/Placements_ByPerson_v44.csv"
OUT_FILE = ROOT / "inputs/identity_lock/Placements_ByPerson_v45.csv"

csv.field_size_limit(10**7)

EID = "1378666423"

# Finals standings derived from mirror results_raw
# format: (division_canon, division_category, place, person_canon, coverage_flag)
_REPLACEMENTS = [
    # Circle finals (3 numbered + Anssi Sundberg inferred p4 from score listing)
    ("Circle", "freestyle", "1", "Milan Benda",       "complete"),
    ("Circle", "freestyle", "2", "Juho Marjo",         "complete"),
    ("Circle", "freestyle", "3", "Aleksi Airinen",     "complete"),
    ("Circle", "freestyle", "4", "Anssi Sundberg",     "partial"),   # unlabelled in source

    # Routines finals
    ("Routines", "freestyle", "1", "Aleksi Airinen",   "complete"),
    ("Routines", "freestyle", "2", "Milan Benda",       "complete"),
    ("Routines", "freestyle", "3", "Anssi Sundberg",    "complete"),
    ("Routines", "freestyle", "4", "Mathias Blau",      "complete"),
    ("Routines", "freestyle", "5", "David Castillo",    "complete"),
    ("Routines", "freestyle", "6", "Mikko Lepisto",     "complete"),
    ("Routines", "freestyle", "7", "Tuomas Riisalo",    "complete"),
    ("Routines", "freestyle", "8", "Santeri Karvinen",  "complete"),
    ("Routines", "freestyle", "9", "Nis Petersen",      "complete"),

    # Request Contest — p1-p4 unchanged (kept from existing rows),
    # p5 4-way tie expanded, p9 3-way tie expanded, p12-p13 unchanged
    # (existing p1-p4, p12, p13 rows are kept; only __NON_PERSON__ rows replaced)
    ("Request Contest", "freestyle", "5",  "David Castillo",      "complete"),
    ("Request Contest", "freestyle", "5",  "Tuukka Antikainen",   "complete"),
    ("Request Contest", "freestyle", "5",  "Anssi Sundberg",      "complete"),
    ("Request Contest", "freestyle", "5",  "Tuomas Riisalo",      "complete"),
    ("Request Contest", "freestyle", "9",  "Aleksi Airinen",      "complete"),
    ("Request Contest", "freestyle", "9",  "Alexander Trenner",   "complete"),
    ("Request Contest", "freestyle", "9",  "Mathias Blau",        "complete"),

    # Big 1 (trick-based circle contest, finals)
    ("Big 1", "freestyle", "1", "Aleksi Airinen",      "complete"),
    ("Big 1", "freestyle", "2", "Lauri Airinen",        "complete"),
    ("Big 1", "freestyle", "3", "Milan Benda",          "complete"),
    ("Big 1", "freestyle", "4", "Tuomas Riisalo",       "complete"),
    ("Big 1", "freestyle", "5", "Mikko Lepisto",        "complete"),

    # Sick 3 (finals)
    ("Sick 3", "freestyle", "1", "Aleksi Airinen",      "complete"),
    ("Sick 3", "freestyle", "2", "Lauri Airinen",       "complete"),
    ("Sick 3", "freestyle", "3", "Tuomas Riisalo",      "complete"),

    # Danish Championships (Routines sub-award, Danish athletes only)
    ("Danish Championships", "freestyle", "1", "Mathias Blau",   "complete"),
    ("Danish Championships", "freestyle", "2", "Nis Petersen",   "complete"),
    ("Danish Championships", "freestyle", "3", "Asmus Helms",    "complete"),
]

# Divisions to drop entirely and replace with _REPLACEMENTS
_DROP_DIVS = {"Circle", "Routines"}

# Within Request Contest, only drop the __NON_PERSON__ team rows
def _is_rc_nonperson(row: dict) -> bool:
    return (row["division_canon"] == "Request Contest"
            and row["person_canon"] == "__NON_PERSON__"
            and row["competitor_type"] == "team")


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    # Build name → person_id lookup from all PBP rows
    name_to_pid: dict[str, str] = {}
    for row in rows:
        if row["person_id"] and row["person_canon"] not in ("__NON_PERSON__", ""):
            name_to_pid[row["person_canon"]] = row["person_id"]
            # normalised fallback
            name_to_pid[row["person_canon"].lower()] = row["person_id"]

    # Also load PT for any person not yet in PBP
    pt_path = ROOT / "inputs/identity_lock/Persons_Truth_Final_v36.csv"
    with open(pt_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            canon = row["person_canon"]
            pid   = row["effective_person_id"]
            if canon and pid and canon not in name_to_pid:
                name_to_pid[canon] = pid
                name_to_pid[canon.lower()] = pid

    # Grab a template row from this event for metadata
    template: dict | None = None
    for row in rows:
        if row["event_id"] == EID:
            template = row.copy()
            break
    assert template, f"Event {EID} not found in PBP"

    out_rows = []
    dropped = []

    for row in rows:
        if row["event_id"] != EID:
            out_rows.append(row)
            continue

        div = row["division_canon"]

        # Drop entire Circle and Routines (prelim pool data)
        if div in _DROP_DIVS:
            dropped.append(f"DROP {div} p{row['place']} {row['person_canon']}")
            continue

        # Drop __NON_PERSON__ tie-placeholder rows in Request Contest
        if _is_rc_nonperson(row):
            dropped.append(f"DROP Request Contest p{row['place']} __NON_PERSON__ (tie placeholder)")
            continue

        # Upgrade surviving Request Contest rows to complete (ties now resolved)
        if div == "Request Contest" and row["coverage_flag"] == "partial":
            row["coverage_flag"] = "complete"

        out_rows.append(row)

    print(f"\nDropped {len(dropped)} rows:")
    for d in dropped:
        print(" ", d)

    # Append replacement + new rows
    added = []
    for (div, cat, place, pcanon, cflag) in _REPLACEMENTS:
        pid = name_to_pid.get(pcanon) or name_to_pid.get(pcanon.lower(), "")
        if not pid:
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
        r["person_unresolved"] = ""
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
