#!/usr/bin/env python3
"""
patch_pt_v46_to_v47.py

Cleans Persons_Truth v46 → v47:

1. ALL_CAPS → Title Case (26 entries — mostly Latin-American competitors)
   Special case: LOVELL MCILWAIN → Lovell McIlwain
2. Wrong-case fixes from alias_candidates-v2.csv (~17 entries)
3. Merges (9 duplicate/variant entries removed from PT):
   - Ka wak Szymon       → Szymon Kalwak
   - Aleksander Smirnov  → Alexander Smirnov  (0 PBP rows)
   - Alex Smirnov        → Alexander Smirnov  (1 PBP row)
   - Tony Fritsch        → Toni Fritsch        (1 PBP row)
   - Mathias Lino Schmidt → Matthias Lino Schmidt (1 PBP row)
   - Jose Cocolan        → José Cocolan        (1 PBP row)
   - Sébastien Verdy     → Sebastien Verdy     (1 PBP row)
   - Stéphane Comeau     → Stephane Comeau     (1 PBP row)
   - Wiktor Dębski       → Wiktor Debski       (3 PBP rows)

Row count: 3,482 → 3,473
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v46.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v47.csv"

# ── Renames (person_canon only, UUID unchanged) ──────────────────────────────

RENAMES = {
    # ALL_CAPS → Title Case
    "ALEJANDRO RODRIGUEZ":          "Alejandro Rodriguez",
    "ANDRES ARCE":                  "Andres Arce",
    "ANDRES FELIPE VALENCIA":       "Andres Felipe Valencia",
    "ANIBAL MONTES":                "Anibal Montes",
    "ANTONIO LINEROS":              "Antonio Lineros",
    "BERNARDO PALACIOS":            "Bernardo Palacios",
    "CARLOS RAMIREZ":               "Carlos Ramirez",
    "CHECK DONELLY":                "Check Donelly",
    "CHRIS GARVIN":                 "Chris Garvin",
    "DANIEL BOYLE":                 "Daniel Boyle",
    "DANIEL CADAVID":               "Daniel Cadavid",
    "EDISSON DUQUE":                "Edisson Duque",
    "EDWIN JARAMILLO":              "Edwin Jaramillo",
    "GABRIEL JAIME RAMIREZ":        "Gabriel Jaime Ramirez",
    "GRED NICE":                    "Gred Nice",
    "JAKUB JANCICH":                "Jakub Jancich",
    "JAVIER ANDRÉS OSORIO MORALES": "Javier Andrés Osorio Morales",
    "JIRI BARTACEK":                "Jiri Bartacek",
    "JUAN BERNARDO PALACIOS LEMOS": "Juan Bernardo Palacios Lemos",
    "LOVELL MCILWAIN":              "Lovell McIlwain",
    "MARTIN OBECOVSKY":             "Martin Obecovsky",
    "MATTHEW CROSS":                "Matthew Cross",
    "MICHAL VELEBA":                "Michal Veleba",
    "SHANE KELLY":                  "Shane Kelly",
    "STEPAN KLIMEK":                "Stepan Klimek",
    "TOMAS ZEMAN":                  "Tomas Zeman",

    # Wrong-case fixes from alias_candidates-v2.csv
    "alex smith":                   "Alex Smith",
    "Alexander kermit Zamotin":     "Alexander Kermit Zamotin",
    "Arkady kemp Lobankov":         "Arkady Kemp Lobankov",
    "brain dennis":                 "Brain Dennis",
    "David andrik":                 "David Andrik",
    "dustin yanofsky":              "Dustin Yanofsky",
    "Francois Depatie Pelletier":   "Francois Depatie-Pelletier",
    "geber Orta":                   "Geber Orta",
    "Iván acosta":                  "Iván Acosta",
    "Jaime alberto Navarro":        "Jaime Alberto Navarro",
    "jermey mirken":                "Jermey Mirken",
    "Marek andrik":                 "Marek Andrik",
    "mike mccarthy":                "Mike McCarthy",
    "Ondrej krabal":                "Ondrej Krabal",
    "paul heckel":                  "Paul Heckel",
    "trey lykins":                  "Trey Lykins",
    "Vilalba martin":               "Vilalba Martin",
}

# ── Merges: UUID of merged-out entry → UUID of surviving entry ───────────────
# person_canon of merged-out entries → surviving person_canon

MERGE_INTO = {
    "Ka wak Szymon":        "Szymon Kalwak",
    "Aleksander Smirnov":   "Alexander Smirnov",
    "Alex Smirnov":         "Alexander Smirnov",
    "Tony Fritsch":         "Toni Fritsch",
    "Mathias Lino Schmidt": "Matthias Lino Schmidt",
    "Jose Cocolan":         "José Cocolan",
    "Sébastien Verdy":      "Sebastien Verdy",
    "Stéphane Comeau":      "Stephane Comeau",
    "Wiktor Dębski":        "Wiktor Debski",
    # Polish encoding-corruption duplicates — broken/diacritic forms → ASCII canonical
    "Pawe Ro ek":           "Pawel Rozek",
    "Paweł Rożek":          "Pawel Rozek",
    "Pawe Fr czek":         "Pawel Fraczek",
    "Pawe cierski":         "Pawel Scierski",
    # Mathias/Matthias Schmidt — same person, merge into dominant canonical
    "Mathias Schmidt":      "Matthias Lino Schmidt",
}

# ── Load ─────────────────────────────────────────────────────────────────────

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

# Index rows by person_canon for merge target lookups
by_canon = {r["person_canon"]: r for r in rows_in}

# ── Pass 1: collect merge-out player_ids to add to survivors ─────────────────
# Map: surviving_canon → list of player_ids to append
merge_ids: dict[str, list[str]] = {}
merge_person_ids: dict[str, list[str]] = {}
for old_canon, survivor_canon in MERGE_INTO.items():
    old_row = by_canon.get(old_canon)
    if not old_row:
        print(f"  WARN: merge source {old_canon!r} not found in PT")
        continue
    # collect player_ids_seen and effective_person_id from merged-out row
    old_ids = [x.strip() for x in old_row["player_ids_seen"].split("|") if x.strip()]
    old_eff = old_row["effective_person_id"].strip()
    if old_eff:
        old_ids.append(old_eff)
    if survivor_canon not in merge_ids:
        merge_ids[survivor_canon] = []
    merge_ids[survivor_canon].extend(old_ids)

# ── Pass 2: build output rows ────────────────────────────────────────────────
rows_out = []
renamed = 0
merged_out = 0

for row in rows_in:
    pc = row["person_canon"]

    # Skip merged-out entries
    if pc in MERGE_INTO:
        merged_out += 1
        continue

    # Apply rename
    if pc in RENAMES:
        old = pc
        row = dict(row)
        row["person_canon"] = RENAMES[pc]
        # also update person_canon_clean if present and matches
        if row.get("person_canon_clean", "").strip() == old:
            row["person_canon_clean"] = RENAMES[pc]
        renamed += 1

    # Append any IDs from merged entries
    current_pc = row["person_canon"]
    if current_pc in merge_ids:
        existing_ids = [x.strip() for x in row["player_ids_seen"].split("|") if x.strip()]
        new_ids = [i for i in merge_ids[current_pc] if i not in existing_ids]
        if new_ids:
            all_ids = existing_ids + new_ids
            row = dict(row)
            row["player_ids_seen"] = " | ".join(all_ids)

    rows_out.append(row)

# ── Write ────────────────────────────────────────────────────────────────────
buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)
OUT.write_text(buf.getvalue(), encoding="utf-8")

print(f"Renames:    {renamed}")
print(f"Merged out: {merged_out}")
print(f"In:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
