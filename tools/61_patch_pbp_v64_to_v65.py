#!/usr/bin/env python3
"""
Patch PBP v64 → v65: targeted data quality fixes for 3 events.

Changes:
  1502997442  Strip spurious " ID" suffix from team_display_name (10 rows)
  1529239199  Strip spurious " ID" suffix from team_display_name (7 rows)
              Pattern: name ends with " ID" before " /" separator or end of string.
              Safe: does not affect "(Pocatello / ID -USA)" style location strings.
  1657486956  Remove duplicate player row: Ivan Cuende comp=player place=4
              (he also appears correctly as a doubles-team member in the same division)

Net row delta: 28511 - 1 = 28510 rows.
"""

import csv
import re
import sys
from pathlib import Path

IN_FILE  = Path("inputs/identity_lock/Placements_ByPerson_v64.csv")
OUT_FILE = Path("inputs/identity_lock/Placements_ByPerson_v65.csv")

# Regex strips " ID" immediately before " /" separator or end of string
_ID_SUFFIX = re.compile(r' ID(?= /| *$)')

_ID_EVENTS = {"1502997442", "1529239199"}


def strip_id_suffix(s: str) -> str:
    return _ID_SUFFIX.sub("", s)


def main() -> None:
    if not IN_FILE.exists():
        sys.exit(f"ERROR: {IN_FILE} not found")

    rows = list(csv.DictReader(IN_FILE.open(encoding="utf-8")))
    fieldnames = list(rows[0].keys())

    out_rows: list[dict] = []
    stats = {"id_stripped": 0, "ivan_removed": 0}

    for r in rows:
        eid  = r["event_id"]
        # ── 1. Strip " ID" suffix from team_display_name ───────────────────
        if eid in _ID_EVENTS:
            td = r.get("team_display_name", "")
            fixed = strip_id_suffix(td)
            if fixed != td:
                r = dict(r)
                r["team_display_name"] = fixed
                stats["id_stripped"] += 1

        # ── 2. Remove duplicate Ivan Cuende player row (1657486956) ─────────
        if (eid == "1657486956"
                and r.get("competitor_type") == "player"
                and r.get("person_canon") == "Ivan Cuende"
                and r.get("team_person_key", "") == ""):
            stats["ivan_removed"] += 1
            continue  # drop this row

        out_rows.append(r)

    with OUT_FILE.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Rows in:  {len(rows)}")
    print(f"Rows out: {len(out_rows)}")
    print(f"  ID suffix stripped: {stats['id_stripped']} team_display_name cells")
    print(f"  Ivan Cuende duplicate removed: {stats['ivan_removed']} row")
    print(f"Written: {OUT_FILE}")


if __name__ == "__main__":
    main()
