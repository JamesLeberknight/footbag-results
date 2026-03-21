#!/usr/bin/env python3
"""
patch_pbp_v79_to_v80.py

Fixes for event 920579000 (1999 East Coast Footbag Championships):

1. Remove spurious "And Qdog Pro Footbag." row — source title/sponsor line
   parsed as a division header; the "player" value is the document title
   "RESULTS: 1999 EAST COAST FOOTBAG CHAMPIONSHIPS".  Not a real placement.

2. Fix "Open Freesyle" (source typo) → "Open Freestyle", category net → freestyle
   (4 rows: Peter Irish p1, Eric Wulff p2, Scott Davidson p3, Jonathan Schneider p4).

Net: 27,985 → 27,984 rows (-1).
"""

from pathlib import Path
import csv, io

ROOT  = Path(__file__).resolve().parent.parent
IN    = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v79.csv"
OUT   = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v80.csv"

TARGET_EVENT = "920579000"
BAD_DIVISION = "And Qdog Pro Footbag."
FIX_DIVISION_OLD = "Open Freesyle"
FIX_DIVISION_NEW = "Open Freestyle"
FIX_CATEGORY_OLD = "net"
FIX_CATEGORY_NEW = "freestyle"

removed = 0
fixed   = 0

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

rows_out = []
for row in rows_in:
    eid = row.get("event_id", "").strip()
    dc  = row.get("division_canon", "").strip()
    cat = row.get("division_category", "").strip()

    if eid == TARGET_EVENT and dc == BAD_DIVISION:
        removed += 1
        continue  # drop the spurious row

    if eid == TARGET_EVENT and dc == FIX_DIVISION_OLD:
        row["division_canon"]    = FIX_DIVISION_NEW
        row["division_category"] = FIX_CATEGORY_NEW
        fixed += 1

    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)

OUT.write_text(buf.getvalue(), encoding="utf-8")

print(f"Removed:  {removed} row(s)  ({BAD_DIVISION!r})")
print(f"Fixed:    {fixed} row(s)   ({FIX_DIVISION_OLD!r} → {FIX_DIVISION_NEW!r}, category {FIX_CATEGORY_OLD!r} → {FIX_CATEGORY_NEW!r})")
print(f"In:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
