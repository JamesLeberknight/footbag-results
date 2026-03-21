#!/usr/bin/env python3
"""
patch_pbp_v80_to_v81.py

Updates person_canon in Placements_ByPerson to reflect PT v45 canonical renames.
Row count unchanged (no adds/removes); content corrected only.

Renames applied to person_canon field:
  Becca English Ross        → Becca English-Ross       (19 rows)
  Eli Piltz                 → Eliot Piltz Galán         (3 rows)
  Evanne Lemarche           → Evanne LaMarche           (14 rows)
  Genevieve Bousquet        → Geneviève Bousquet        (30 rows)
  Jimmy Caveney             → Jim Caveney               (15 rows)
  Jody Welch                → Jody Badger Welch         (16 rows)
  Jolene Welch              → Jody Badger Welch         (0 rows, defensive)
  Lisa McDaniel             → Lisa McDaniel Jones       (22 rows)
  Paul Lovern               → P.T. Lovern               (12 rows)
  Paul (PT) Lovern          → P.T. Lovern               (1 row)
  PT Lovern                 → P.T. Lovern               (1 row)
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v80.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v81.csv"

RENAMES = {
    "Becca English Ross":  "Becca English-Ross",
    "Eli Piltz":           "Eliot Piltz Galán",
    "Evanne Lemarche":     "Evanne LaMarche",
    "Genevieve Bousquet":  "Geneviève Bousquet",
    "Jimmy Caveney":       "Jim Caveney",
    "Jody Welch":          "Jody Badger Welch",
    "Jolene Welch":        "Jody Badger Welch",
    "Lisa McDaniel":       "Lisa McDaniel Jones",
    "Paul Lovern":         "P.T. Lovern",
    "Paul (PT) Lovern":    "P.T. Lovern",
    "PT Lovern":           "P.T. Lovern",
}

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

counts: dict[str, int] = {}
rows_out = []

for row in rows_in:
    pc = row.get("person_canon", "").strip()
    if pc in RENAMES:
        row["person_canon"] = RENAMES[pc]
        counts[pc] = counts.get(pc, 0) + 1
    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)

OUT.write_text(buf.getvalue(), encoding="utf-8")

total = sum(counts.values())
print("Renames applied:")
for old, n in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"  {n:3d}  {old!r} → {RENAMES[old]!r}")
print(f"\nTotal rows updated: {total:,}")
print(f"In:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
