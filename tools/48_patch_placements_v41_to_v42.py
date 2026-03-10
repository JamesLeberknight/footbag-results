#!/usr/bin/env python3
"""
tools/48_patch_placements_v41_to_v42.py — PBP v41 → v42

Single change: rename event_id 2001983003 → 2001983002 for all rows
(1983 WFA championship — stage2 uses 2001983002, PBP was using the
stale 2001983003 ID assigned before the FREESTYLE sub-event was dropped).

Reads:  inputs/identity_lock/Placements_ByPerson_v41.csv
Writes: inputs/identity_lock/Placements_ByPerson_v42.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LOCK = REPO / "inputs" / "identity_lock"

IN_FILE  = LOCK / "Placements_ByPerson_v41.csv"
OUT_FILE = LOCK / "Placements_ByPerson_v42.csv"

OLD_EVENT_ID = "2001983003"
NEW_EVENT_ID = "2001983002"


def main() -> int:
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    changed = 0
    for row in rows:
        if row.get("event_id") == OLD_EVENT_ID:
            row["event_id"] = NEW_EVENT_ID
            changed += 1

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Rows updated: {changed} ({OLD_EVENT_ID} → {NEW_EVENT_ID})")
    print(f"Total rows:   {len(rows)}")
    print(f"Written:      {OUT_FILE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
