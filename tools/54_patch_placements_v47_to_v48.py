"""
tools/54_patch_placements_v47_to_v48.py
Patch Placements_ByPerson v47 → v48.

Event 941418343 — 22nd Annual IFPA World Footbag Championships 2000.

"Womens Singles Net" in PBP incorrectly merged Open and Intermediate
players together (p1×2, p2×2, p3×2, p4×2 = obvious collapse).

Stage2 correctly separates them:
  - Womens Singles Net:              p1-p4, tied p5×2, tied p7×2, p9
  - Womens Intermediate Singles Net: p1-p4

Changes:
  - DROP all 9 garbled Womens Singles Net rows
  - ADD 9 correct Womens Singles Net rows (real ties at p5, p7)
  - ADD 4 Womens Intermediate Singles Net rows
"""

import csv, pathlib

ROOT     = pathlib.Path(__file__).parent.parent
IN_FILE  = ROOT / "inputs/identity_lock/Placements_ByPerson_v47.csv"
OUT_FILE = ROOT / "inputs/identity_lock/Placements_ByPerson_v48.csv"

csv.field_size_limit(10**7)

EID = "941418343"

_WOMENS_SINGLES_NET = [
    # (place, person_canon)
    ("1",  "Lisa McDaniel"),
    ("2",  "Jody Welch"),
    ("3",  "Julie Symons"),
    ("4",  "Evanne Lemarche"),
    ("5",  "Marilyn Demuy"),
    ("5",  "Maude Laudreville"),
    ("7",  "Kelly Kelley"),
    ("7",  "Pauline Bechtel"),
    ("9",  "Francesca Ryan"),
]

_WOMENS_INTERMEDIATE_SINGLES_NET = [
    ("1",  "Tara Wolczuk"),
    ("2",  "Leanne Makcrow"),
    ("3",  "Amy Noller"),
    ("4",  "Judith Lyn Arney"),
]


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    # Build name → person_id from all PBP rows
    name_to_pid: dict[str, str] = {}
    for row in rows:
        if row["person_id"] and row["person_canon"] not in ("__NON_PERSON__", ""):
            name_to_pid[row["person_canon"]] = row["person_id"]

    # Also load PT
    pt_path = ROOT / "inputs/identity_lock/Persons_Truth_Final_v36.csv"
    with open(pt_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            canon = row["person_canon"]
            pid   = row["effective_person_id"]
            if canon and pid and canon not in name_to_pid:
                name_to_pid[canon] = pid

    # Template row from this event
    template: dict | None = None
    for row in rows:
        if row["event_id"] == EID:
            template = row.copy()
            break
    assert template, f"Event {EID} not found"

    out_rows = []
    dropped = []

    for row in rows:
        if row["event_id"] == EID and row["division_canon"] == "Womens Singles Net":
            dropped.append(f"DROP Womens Singles Net p{row['place']} {row['person_canon']}")
            continue
        out_rows.append(row)

    print(f"\nDropped {len(dropped)} rows:")
    for d in dropped:
        print(" ", d)

    added = []
    for (place, pcanon) in _WOMENS_SINGLES_NET:
        pid = name_to_pid.get(pcanon, "")
        if not pid:
            print(f"  WARNING: no person_id for {pcanon!r}")
        r = template.copy()
        r["division_canon"]    = "Womens Singles Net"
        r["division_category"] = "net"
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
        added.append(f"ADD  Womens Singles Net p{place} {pcanon}")

    for (place, pcanon) in _WOMENS_INTERMEDIATE_SINGLES_NET:
        pid = name_to_pid.get(pcanon, "")
        if not pid:
            print(f"  WARNING: no person_id for {pcanon!r}")
        r = template.copy()
        r["division_canon"]    = "Womens Intermediate Singles Net"
        r["division_category"] = "net"
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
        added.append(f"ADD  Womens Intermediate Singles Net p{place} {pcanon}")

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
