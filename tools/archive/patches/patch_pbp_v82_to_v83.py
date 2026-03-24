#!/usr/bin/env python3
"""
patch_pbp_v82_to_v83.py

Updates person_canon in Placements_ByPerson to reflect PT v47 changes.

Changes applied (in priority order):
1. PT RENAMES — ALL_CAPS → title case (26), wrong-case fixes (17)
2. MERGE remaps — merged-out PT entries redirected to surviving canonical
3. Alias resolution — 56 stale canons resolved via person_aliases.csv
4. Encoding-corrupt direct maps — U+FFFD / mojibake forms resolved to PT canonical

Row count: ~27,984 (unchanged; only person_canon values updated)
"""

from pathlib import Path
import csv, io

ROOT = Path(__file__).resolve().parent.parent
IN   = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v82.csv"
OUT  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v83.csv"

# ── Priority-1: PT renames (same dict as patch_pt_v46_to_v47) ────────────────

PT_RENAMES = {
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
    # Wrong-case fixes
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

# ── Priority-2: Merge remaps (merged-out PT entries) ─────────────────────────

MERGE_REMAP = {
    "Ka wak Szymon":        "Szymon Kalwak",
    "Aleksander Smirnov":   "Alexander Smirnov",
    "Alex Smirnov":         "Alexander Smirnov",
    "Tony Fritsch":         "Toni Fritsch",
    "Mathias Lino Schmidt": "Matthias Lino Schmidt",
    "Jose Cocolan":         "José Cocolan",
    "Sébastien Verdy":      "Sebastien Verdy",
    "Stéphane Comeau":      "Stephane Comeau",
    "Wiktor Dębski":        "Wiktor Debski",
    # Polish PT duplicates
    "Pawe Ro ek":           "Pawel Rozek",
    "Paweł Rożek":          "Pawel Rozek",
    "Pawe Fr czek":         "Pawel Fraczek",
    "Paweł Frączek":        "Pawel Fraczek",
    "Pawe cierski":         "Pawel Scierski",
    "Paweł Ścierski":       "Pawel Scierski",
}

# ── Priority-3: Alias resolution ─────────────────────────────────────────────
# Load aliases; only include entries where target is in PT v47

PT_V47 = set()
pt_path = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v47.csv"
for r in csv.DictReader(pt_path.open(newline="", encoding="utf-8")):
    PT_V47.add(r["person_canon"])

alias_path = ROOT / "overrides" / "person_aliases.csv"
ALIAS_MAP: dict[str, str] = {}
for r in csv.DictReader(alias_path.open(newline="", encoding="utf-8")):
    canon = r["person_canon"]
    alias = r["alias"]
    if canon in PT_V47 and alias not in PT_RENAMES and alias not in MERGE_REMAP and alias != canon:
        ALIAS_MAP[alias] = canon

# ── Priority-4: Encoding-corrupt direct maps ─────────────────────────────────
# U+FFFD replacement chars and other mojibake → known PT canonical

CORRUPT_MAP = {
    # ── U+FFFD corruption (exact keys from file repr) ───────────────────────
    # Polish names with ó/ę/ł etc. replaced by U+FFFD
    "Filip W\ufffdjcik":             "Filip Wojcik",        # Wójcik
    "Krzysztof Sob\ufffdtka":        "Krzysztof Sob\u00f3tka",  # Sobótka
    "Micha\u0142 R\ufffdg":          "Michal Rog",          # Michał Róg (ł=U+0142, ó=FFFD)
    "Tuomas K\ufffdrki":             "Tuomas Karki",        # Kärki
    "Rados\ufffdaw Turek":           "Rados Turek",         # Radosław (PT has short form)
    "V\ufffdclav Klouda":            "Vaclav Klouda",       # Václav
    "Jin\ufffddra Smola":            "Jindrich Smola",      # Jindřa
    "S\ufffdbastion Duchesne":       "Sebastien Duchesne",
    "S\ufffdbastion Duchesne CAN":   "Sebastien Duchesne",
    "S\ufffdbastion Maillet":        "Sebastien Maillet",
    "S\ufffdbastion Maillet FRA":    "Sebastien Maillet",
    "H\ufffdkan Hellberg":           "Hakan Hellberg",
    "Mikko Lepist\ufffd":            "Mikko Lepisto",
    "Genevi\ufffdve Bousquet":       "Geneviève Bousquet",
    "Genevi\ufffdve Bousquet CAN":   "Geneviève Bousquet",
    "Robin P\ufffdchel GER":         "Robin Puchel",
    "Rene R\ufffdhr GER":            "Rene Ruehr",
    "Rene R\ufffdhr (Israel)":       "Rene Ruehr",
    "Tuomas K\ufffdrki FIN":         "Tuomas Karki",
    "Mikko Lepist\ufffd FIN":        "Mikko Lepisto",
    "Fran\ufffdois Pelletier CAN":   "Francois Pelletier",
    # Mixed proper Unicode + FFFD
    "Micha\u0142 R\ufffdag":         "Michal Rog",          # alternate FFFD position
    # Country suffix without corruption
    "Rene Rühr (Israel)":            "Rene Ruehr",
    "Václav Klouda (Czech Republic)": "Vaclav Klouda",
    "Jindra Smola (Czech Republic)": "Jindrich Smola",
    # ── Fully-correct diacritics for PT entries stored in ASCII ─────────────
    "Michał Róg":                    "Michal Rog",
    "Michał Klimczak":               "Michal Klimczak",
    "Aleš Zelinka":                  "Ales Zelinka",
    "Sergio Hernández Santiago":     "Sergio Hernandez Santiago",
    "Szymon Kałwak":                 "Szymon Kalwak",
    "Łukasz Domin":                  "Lukasz Domin",
    "Dawid Michałowicz":             "Dawid Michalowicz",
    "Paweł Frączek":                 "Pawel Fraczek",
    "Paweł Rożek":                   "Pawel Rozek",
    "Michał Rog":                    "Michal Rog",
    "Michał R\ufffdg":               "Michal Rog",          # partial corruption
    # Remaining high-count FFFD stale (exact keys verified from file repr)
    "Jakub Mo\ufffdciszewski":       "Jakub Mosciszewski",
    "Wiktor D\ufffdbski":            "Wiktor Debski",
    "Rafa\ufffd Kaleta":             "Rafal Kaleta",
    "Micha\ufffd R\ufffdg":          "Michal Rog",
    "Fran\ufffdois Pelletier":       "Francois Pelletier",
    "Dawid Micha\ufffdowicz":        "Dawid Michalowicz",
    "Marcin Staro\ufffd":            "Marcin Staron",
    "Hanna Mick\ufffdiewicz":        "Hannia Mickiewicz",
    "\ufffdukasz Domin":             "Lukasz Domin",
    "Szymon Ka\ufffdwak":            "Szymon Kalwak",
    "Va\ufffdek Klouda CZE":         "Vaclav Klouda",
    "S\ufffdbastien Duchesne":       "Sebastien Duchesne",
    "S\ufffdbastien Duchesne CAN":   "Sebastien Duchesne",
    "\ufffdukasz Krysiewicz":        "Łukasz Krysiewicz",
}

# Filter corrupt map to only targets actually in PT v47 and where it's a real change
CORRUPT_MAP = {
    k: v for k, v in CORRUPT_MAP.items()
    if v in PT_V47 and k != v
}

# ── Additional targeted fixes ─────────────────────────────────────────────────

EXTRA_MAP = {
    # Typo variants with known PT canonical
    "Jasper Schults":    "Jasper Shults",
    # Matthias Schmidt (13 rows) not in PT; alias_candidates groups with Matthias Lino Schmidt
    "Matthias Schmidt":  "Matthias Lino Schmidt",
    "Schmidt Matthias":  "Matthias Lino Schmidt",
    # Mathias Schmidt (2 rows in PT) alias of Matthias Lino Schmidt per alias_candidates
    "Mathias Schmidt":   "Matthias Lino Schmidt",
}
EXTRA_MAP = {k: v for k, v in EXTRA_MAP.items() if v in PT_V47 and k != v}

# ── Build master remap dict (priority order: PT > MERGE > ALIAS > EXTRA > CORRUPT) ──

REMAP: dict[str, str] = {}
REMAP.update(CORRUPT_MAP)   # lowest priority
REMAP.update(EXTRA_MAP)
REMAP.update(ALIAS_MAP)
REMAP.update(MERGE_REMAP)
REMAP.update(PT_RENAMES)    # highest priority

# ── Auto-resolve country-suffix variants (Name (Country) → Name if Name in PT v47) ──
# Applied after explicit remaps so explicit overrides take precedence
import re
_BRACKET_RE = re.compile(r"^(.+?)\s*\([^)]+\)$")

auto_country: dict[str, str] = {}
for row in csv.DictReader(IN.open(newline="", encoding="utf-8")):
    pc = row.get("person_canon", "").strip()
    if pc in PT_V47 or pc in REMAP or pc == "__NON_PERSON__":
        continue
    m = _BRACKET_RE.match(pc)
    if m:
        base = m.group(1).strip()
        if base in PT_V47:
            auto_country[pc] = base

REMAP.update({k: v for k, v in auto_country.items() if k not in REMAP})

# ── Person-ID remap (merged-out UUIDs → surviving UUIDs) ─────────────────────
# When PT entries are merged, PBP rows retain the old person_id.
# These must be updated so stage 04 QC (PF person_id ∈ PT) passes.

PERSON_ID_REMAP = {
    # format: old_uuid (merged-out) → new_uuid (surviving)
    # From PT v46→v47 merges
    "09e50ebb-e69b-593a-8a22-6f5f60d3ccda": "9dac33d2-6aed-52eb-8231-a5a469a63f5a",  # Tony Fritsch → Toni Fritsch
    "106f11af-2fef-5591-905a-0f63e12a3a09": "9b0f06c8-7ef6-5145-9e2b-e80c48a06615",  # Alex Smirnov → Alexander Smirnov
    "4fe4f26c-8c31-5464-a3cf-d0ddfd4776f0": "9b0f06c8-7ef6-5145-9e2b-e80c48a06615",  # Aleksander Smirnov → Alexander Smirnov
    "97c85c3d-4a44-5986-bd86-5daf5b89ad55": "a1c7ef80-cbe3-50d0-aced-cdafcd490d4d",  # Jose Cocolan → José Cocolan
    "ab72fbeb-c2e7-59e6-b98c-d87d722ee7c7": "4ae1304e-960b-554f-8ce7-c59e0d7607b2",  # Ka wak Szymon → Szymon Kalwak
    "431a10dd-7c0b-570c-be3a-e3a4e8a391b9": "f52e0fe8-4eff-5532-9af4-14124f1e92ee",  # Mathias Lino Schmidt → Matthias Lino Schmidt
    "a729ed87-422a-5f42-bd5f-2ab57de8b232": "f52e0fe8-4eff-5532-9af4-14124f1e92ee",  # Mathias Schmidt → Matthias Lino Schmidt
    "221deee8-ff9a-5945-8f0c-8caa42130f4b": "9a45d2ec-cf90-5686-9e97-65d0b06918ae",  # Pawe Fr czek → Pawel Fraczek
    "95c0f894-56f9-5676-acc1-01a7942bea7c": "0b3ffb10-2257-510e-a69a-26ff2fc87508",  # Pawe Ro ek → Pawel Rozek
    "a2e1c080-1c17-5bfc-8c31-6c5327cc240e": "0b3ffb10-2257-510e-a69a-26ff2fc87508",  # Paweł Rożek → Pawel Rozek
    "c5245d6c-3e3a-58b2-939d-c55083a9ecca": "ac0d2fdd-9032-5edc-9023-e8d611dfc8c6",  # Pawe cierski → Pawel Scierski
    "c75a8cbe-1bdf-5e15-a636-4a854c2b3ef8": "d7ee4909-a76d-5639-aa82-ce4a8a7a53ba",  # Stéphane Comeau → Stephane Comeau
    "f2df2e46-9db7-5cfd-b95f-d525c99cf92c": "c0ba05a1-16a9-51e0-bcf5-7f6b879710a9",  # Sébastien Verdy → Sebastien Verdy
    "97c0fff7-ec3a-5fc6-8c8a-382851c8e28c": "c917c04b-83fa-5c1a-8f2c-292f24db5616",  # Wiktor Dębski → Wiktor Debski
    # From prior patch (v81→v82): Johnathon Murphy → Jonathan Murphy
    "126a7db1-2603-5589-848f-c0af515660a1": "a449f6c7-f0e2-5b34-ab9e-81e946d65fa9",  # Johnathon Murphy → Jonathan Murphy
}

# ── Process PBP ───────────────────────────────────────────────────────────────

rows_in = list(csv.DictReader(IN.open(newline="", encoding="utf-8")))
fieldnames = list(rows_in[0].keys())

counts: dict[str, int] = {}
pid_counts: dict[str, int] = {}
rows_out = []

for row in rows_in:
    row = dict(row)
    pc = row.get("person_canon", "").strip()
    if pc in REMAP:
        new_pc = REMAP[pc]
        row["person_canon"] = new_pc
        counts[pc] = counts.get(pc, 0) + 1
    # Also remap person_id field for merged entries
    pid = row.get("person_id", "").strip()
    if pid in PERSON_ID_REMAP:
        row["person_id"] = PERSON_ID_REMAP[pid]
        pid_counts[pid] = pid_counts.get(pid, 0) + 1
    rows_out.append(row)

buf = io.StringIO()
w = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
w.writeheader()
w.writerows(rows_out)
OUT.write_text(buf.getvalue(), encoding="utf-8")

total = sum(counts.values())
print(f"Rows updated: {total:,}")
print()
print("Top remaps applied:")
for old, n in sorted(counts.items(), key=lambda x: -x[1])[:30]:
    print(f"  {n:3d}  {old!r} → {REMAP[old]!r}")
print(f"\nIn:  {len(rows_in):,} rows")
print(f"Out: {len(rows_out):,} rows")
print(f"Written: {OUT}")
