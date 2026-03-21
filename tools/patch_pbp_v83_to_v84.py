#!/usr/bin/env python3
"""
patch_pbp_v83_to_v84.py

Fixes event 1050140930 (2003 Footbag Jam Zeil am Main, Frankfurt Germany).

Source format: "Name - Club" for individual placements, "Name & Name - Club" for doubles.
The parser mishandled this event throughout:
  - "Name - Club" in singles/freestyle → split on "-" creating spurious team rows
  - "Name / Sole Rebels Zuerich" in Open Freestyle → split on "/" creating phantom partner

Changes:
  1. Remove 2 spurious team rows in Open Singles Net (duplicate the correct player rows)
  2. Fix team_display_name for all remaining team rows — strip "/ Club", "/ City",
     "- Club" suffixes. Team rows in freestyle are individual placements with
     club affiliation; doubles rows (Open Doubles Net) are real pairs.

Row count: 27,984 → 27,982 (-2 spurious singles team rows)
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v83.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v84.csv"

EVENT = "1050140930"

# team_display_name → corrected team_display_name
# For rows that should be removed entirely, value is None
# Doubles Net rows (Open Doubles Net) keep both player names; club suffixes stripped
# Individual rows (freestyle, etc.) keep only the player's name

DISPLAY_FIXES = {
    # Open Doubles Net — real doubles pair; strip club suffix from p2's name
    "Philipp Nierstheimer / Patrick Schrickel - FC Footstar Berlin": "Philipp Nierstheimer / Patrick Schrickel",

    # German Ranking Open Freestyle — solo placements, strip club
    "Paul Cronjaeger / FC Footstar Berlin":                    "Paul Cronjaeger",
    "Jakob Wagner / FC Footstar Berlin":                       "Jakob Wagner",
    "Yves Kreil / FC Footstar Berlin":                         "Yves Kreil",
    "Jan Zimmermann - FC Footstar Berlin / Sole Rebels Zuerich": "Jan Zimmermann",
    "Matthias Schmidt / FC Footstar Berlin":                   "Matthias Schmidt",
    "Stefan Siegert / Sacktreter Frankenberg":                 "Stefan Siegert",

    # Intermediate Freestyle — solo placements, strip city/club
    "Max Kerkhoff / FC Footstar Berlin":                       "Max Kerkhoff",
    "Daniel Schuldes / Muenchen":                              "Daniel Schuldes",
    "Jochen Bauer / Amberg":                                   "Jochen Bauer",
    "Rico Boehme / Sacktreter Frankenberg":                    "Rico Boehme",
    "Markus Kaspczak / Frankfurt Footbag":                     "Markus Kaspczak",

    # Open Freestyle — solo placements, strip club/team
    "Vasek Klouda / Dictators Prague":                         "Vasek Klouda",
    "Jan Weber / Dictators Prague":                            "Jan Weber",
    "Pavel Cerveny / Dictators Prague":                        "Pavel Cerveny",
    "Jan Struz / Dictators Prague":                            "Jan Struz",
    # Paul Cronjaeger, Jakob Wagner, Yves Kreil, Jan Zimmermann, Matthias Schmidt,
    # Stefan Siegert covered above (same key in both divisions)

    # Women's Freestyle — solo placements, strip club/city
    "Julia Boehm / FC Footstar Berlin":                        "Julia Boehm",
    "Anne Busch / Cologne":                                    "Anne Busch",
    "Mildret Rumpf / Frankfurt Footbag":                       "Mildret Rumpf",
    "Esther Strauch / Frankfurt Footbag":                      "Esther Strauch",
    "Josefine Schwengbeck / FC Footstar Berlin":               "Josefine Schwengbeck",
}

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

removed = 0
fixed = 0
rows_out = []

for row in rows_in:
    if row["event_id"] != EVENT:
        rows_out.append(row)
        continue

    # Remove spurious team rows in Open Singles Net (clean player rows exist)
    if (row["division_canon"] == "Open Singles Net"
            and row["competitor_type"] == "team"):
        removed += 1
        print(f"  REMOVE: {row['division_canon']} p{row['place']} "
              f"team_display_name={row['team_display_name']!r}")
        continue

    # Fix team_display_name for contaminated rows
    tdn = row.get("team_display_name", "")
    if tdn in DISPLAY_FIXES:
        row = dict(row)
        old = row["team_display_name"]
        row["team_display_name"] = DISPLAY_FIXES[tdn]
        fixed += 1
        print(f"  FIX: {row['division_canon']} p{row['place']} "
              f"{old!r} → {row['team_display_name']!r}")

    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)
OUT.write_text(buf.getvalue(), encoding="utf-8")

print(f"\nRemoved: {removed}")
print(f"Fixed:   {fixed}")
print(f"In:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
