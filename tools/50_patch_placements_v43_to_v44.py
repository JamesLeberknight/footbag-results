"""
tools/50_patch_placements_v43_to_v44.py
Patch Placements_ByPerson v43 → v44.

Fixes two events with garbled PBP rows caused by multiple sub-events sharing
the same division_canon, producing duplicate placements for the same person
at multiple places within the same division.

Event 1102788509 — 3rd Annual Sunshred Footbag Open (2005):
  Sick 3 p1 had a spurious team row (trick-list line parsed as team entry).
  Sick 3 p3 was Felix Zenger (wrong); source shows p3 = "Dexter" = Jan Struz.

Event 1173467292 — 4th Annual Montana Freestyle Jam (2007):
  All 28 PBP rows replaced with 14 clean rows using only the Shred30
  standings (the primary / most-complete sub-event). The other sub-events
  (Rippin Run, Sick Trick, Sick3) shared the same "Intermediate"/"Open"
  division_canon, creating duplicates across every placement.
"""

import csv, pathlib

ROOT     = pathlib.Path(__file__).parent.parent
IN_FILE  = ROOT / "inputs/identity_lock/Placements_ByPerson_v43.csv"
OUT_FILE = ROOT / "inputs/identity_lock/Placements_ByPerson_v44.csv"

csv.field_size_limit(10**7)

# Shred30 standings for Montana 1173467292.
# person_ids are resolved at runtime from existing PBP rows for this event.
_MONTANA_CLEAN = [
    # division,       place, person_canon
    ("Intermediate",  "1",  "Zeb Jackson"),
    ("Intermediate",  "2",  "Jake Wren"),
    ("Intermediate",  "3",  "Kyle Hewitt"),
    ("Intermediate",  "4",  "Zac Jackson"),
    ("Intermediate",  "5",  "Scott Behmer"),
    ("Intermediate",  "6",  "Ben Benulis"),
    ("Open",          "1",  "Jim Penske"),
    ("Open",          "2",  "Andrew Grant"),
    ("Open",          "3",  "Byrin Wylie"),
    ("Open",          "4",  "Nick Landes"),
    ("Open",          "5",  "Rory Dawson"),
    ("Open",          "6",  "Daryl Genz"),
    ("Open",          "7",  "Kevin Regamey"),
    ("Open",          "8",  "Luis Monhollon"),
]


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    # Build name→person_id lookup from existing Montana rows
    montana_id_lookup: dict[str, str] = {}
    montana_template: dict | None = None
    for row in rows:
        if row["event_id"] == "1173467292" and row["person_id"]:
            montana_id_lookup[row["person_canon"]] = row["person_id"]
            if montana_template is None:
                montana_template = row.copy()

    # Jan Struz UUID for Sick 3 p3 fix
    jan_struz_id = None
    for row in rows:
        if row["person_canon"] == "Jan Struz" and row["person_id"]:
            jan_struz_id = row["person_id"]
            break
    assert jan_struz_id, "Jan Struz not found in PBP"

    out_rows = []
    changes = []
    montana_dropped = 0

    for row in rows:
        eid = row["event_id"]

        # ------------------------------------------------------------------
        # Event 1102788509 — Sunshred 2005: fix Sick 3
        # ------------------------------------------------------------------
        if eid == "1102788509" and row["division_canon"] == "Sick 3":
            # Drop the spurious p1 team row (trick-list parsed as team)
            if row["place"] == "1" and row["competitor_type"] == "team":
                changes.append("DROP   1102788509 Sick 3 p1 __NON_PERSON__ team (trick-list artifact)")
                continue

            # Fix p3: Felix Zenger → Jan Struz
            # "Dexter" in source = Jan Struz's nickname (confirmed by Open Freestyle
            # entry "Jan Struz (Dexter)" in the same event)
            if row["place"] == "3" and row["person_canon"] == "Felix Zenger":
                row["person_id"]     = jan_struz_id
                row["person_canon"]  = "Jan Struz"
                row["coverage_flag"] = "complete"
                row["norm"]          = "jan struz"
                changes.append("FIX    1102788509 Sick 3 p3: Felix Zenger → Jan Struz (Dexter)")

        # ------------------------------------------------------------------
        # Event 1173467292 — Montana Freestyle Jam 2007: drop all rows
        # ------------------------------------------------------------------
        if eid == "1173467292":
            montana_dropped += 1
            continue

        out_rows.append(row)

    # Append clean Montana replacement rows
    changes.append(f"DROP   1173467292: {montana_dropped} garbled rows removed")
    for (div, place, pcanon) in _MONTANA_CLEAN:
        pid = montana_id_lookup.get(pcanon, "")
        r = montana_template.copy()
        r["division_canon"]    = div
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
        if not pid:
            print(f"  WARNING: no person_id found for {pcanon!r}", flush=True)
        out_rows.append(r)
    changes.append(f"ADD    1173467292: {len(_MONTANA_CLEAN)} clean rows (Shred30 standings only)")

    print(f"\nChanges ({len(changes)}):")
    for c in changes:
        print(" ", c)

    print(f"\nTotal rows: {len(out_rows):,}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Written: {OUT_FILE.name}")


if __name__ == "__main__":
    main()
