#!/usr/bin/env python3
"""
patch_pbp_v84_to_v85.py

Removes 2 date-contamination rows from event 1721817655
(2024 VI. Basque Tournament of Footbag Net, Bilbao).

Source narrative contains round-date headers:
  "9th january"   → parsed as place 9,  player "january"
  "11th of january" → parsed as place 11, player "of january"

The definitive final Classification section (p1–p6) is correct.
These two rows have no person_id (unresolved) and are not real players.
A RESULTS_FILE_OVERRIDE (legacy_data/event_results/1721817655.txt) fixes
rebuild mode; this patch removes them from the identity-lock PBP file.

Row count: 27,982 → 27,980 (-2)
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v84.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v85.csv"

EVENT = "1721817655"

# Exact person_canon values to remove (both are unresolved, no person_id)
REMOVE_NAMES = {"january", "of january"}

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

removed = 0
rows_out = []

for row in rows_in:
    if (row["event_id"] == EVENT
            and row.get("person_canon", "").strip().lower() in REMOVE_NAMES):
        removed += 1
        print(f"  REMOVE: event={row['event_id']} division={row['division_canon']!r} "
              f"place={row['place']} person_canon={row['person_canon']!r}")
        continue
    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)
OUT.write_text(buf.getvalue(), encoding="utf-8")

print(f"\nRemoved: {removed}")
print(f"In:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
