#!/usr/bin/env python3
"""
patch_pt_v44_to_v45.py

Applies authoritative canonical-name corrections to Persons_Truth v44 → v45.
These are human-verified name standardisations based on IFPA member profiles,
official Hall-of-Fame reference pages, and the player's own documented usage.

Changes
-------
Renames (person_canon updated, UUID unchanged):
  Becca English Ross  → Becca English-Ross      (hyphen is official form)
  Eli Piltz           → Eliot Piltz Galán        (full legal/professional name)
  Evanne Lemarche     → Evanne LaMarche          (IFPA profile capitalisation)
  Genevieve Bousquet  → Geneviève Bousquet       (diacritic confirmed in official sources)
  Jimmy Caveney       → Jim Caveney              (official IFPA/Hall form)
  Jody Welch          → Jody Badger Welch        (full married name; IFPA profile)
  Lisa McDaniel       → Lisa McDaniel Jones      (Hall-of-Fame married-name form)
  Paul Lovern         → P.T. Lovern              (IFPA profile + Hall page use initials)

Remove (merge into Jody Badger Welch):
  Jolene Welch — same person as Jody Welch; 0 PBP rows; added as alias instead.

No change needed (already correct):
  Amy Westberg, Ethan Husted, Jim Derricott, Kendall KIC
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v44.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v45.csv"

# old person_canon → new person_canon
RENAMES = {
    "Becca English Ross":  "Becca English-Ross",
    "Eli Piltz":           "Eliot Piltz Galán",
    "Evanne Lemarche":     "Evanne LaMarche",
    "Genevieve Bousquet":  "Geneviève Bousquet",
    "Jimmy Caveney":       "Jim Caveney",
    "Jody Welch":          "Jody Badger Welch",
    "Lisa McDaniel":       "Lisa McDaniel Jones",
    "Paul Lovern":         "P.T. Lovern",
}

# UUIDs to REMOVE from PT (person merged into another entry)
REMOVE_UUIDS = {
    "e894e6c5-3f98-51e0-a86c-61b1d28b585c",  # Jolene Welch → alias for Jody Badger Welch
}

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

rows_out = []
renamed = 0
removed = 0

for row in rows_in:
    pc  = row.get("person_canon", "").strip()
    pid = row.get("effective_person_id", "").strip()

    if pid in REMOVE_UUIDS:
        removed += 1
        print(f"  REMOVE: {pid}  {pc}")
        continue

    if pc in RENAMES:
        old = pc
        row["person_canon"] = RENAMES[pc]
        # Also update person_canon_clean if it exists and matches old value
        if row.get("person_canon_clean", "").strip() == old:
            row["person_canon_clean"] = RENAMES[pc]
        renamed += 1
        print(f"  RENAME: {old!r} → {RENAMES[pc]!r}  ({pid[:8]})")

    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)

OUT.write_text(buf.getvalue(), encoding="utf-8")

print(f"\nIn:     {len(rows_in):,} rows")
print(f"Out:    {len(rows_out):,} rows")
print(f"Renamed: {renamed}")
print(f"Removed: {removed}")
print(f"Written: {OUT}")
