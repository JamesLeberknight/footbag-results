"""
tools/patch_pbp_v69_to_v70.py
PBP patch: fix California State Footbag Championships 1997 (event 859923755).

The source HTML used a tabular RESULTS/SEED/PARTNERS format that the parser
misread as "Last / First" team separators. All doubles divisions were mangled:
 - Open Doubles Net: 13 broken rows (header parsed as placement + 12 wrong teams)
 - Mixed Doubles Net: partners not parsed; half shown as __NON_PERSON__
 - Intermediate Doubles: singles-only rows (partners missing)
 - Novice Doubles: duplicate row for Doug/Chris; winners Marc Weber/Bob Silva
   not resolved

This patch:
 1. Removes all 36 existing rows for event 859923755.
 2. Adds 57 correctly structured rows (4+3+24+10+16+6 = 63... but partial
    resolution means some teams produce 1 row rather than 2, giving 57 total).

Input:  inputs/identity_lock/Placements_ByPerson_v69.csv
Output: inputs/identity_lock/Placements_ByPerson_v70.csv
"""

import csv
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "inputs" / "identity_lock"

IN_FILE  = LOCK / "Placements_ByPerson_v69.csv"
OUT_FILE = LOCK / "Placements_ByPerson_v70.csv"

csv.field_size_limit(10 * 1024 * 1024)

EVENT_ID = "859923755"

# ── Resolved name → (person_id, person_canon) ────────────────────────────────
# Keys are names as written in the legacy override file.
RESOLVE = {
    "Sam Conlon":       ("ff744d13-860e-5ab6-8920-dac66414b828", "Samantha Conlon"),
    "Tuan Vu":          ("28565dd0-2196-5404-bf23-6cf0617ce79b", "Tuan Vu"),
    "Steve Goldberg":   ("0ae92001-5680-502c-95fa-500e6a808f14", "Steve Goldberg"),
    "John Leys":        ("3b938feb-b4c7-59a1-929f-7b62be77c1ce", "John Leys"),
    "Aaron de Glanville":("eea1063c-fcc1-5849-9e35-592bdf13fd7b", "Aaron De Glanville"),
    "Jeff Gran":        ("0483531e-d852-52f4-a50b-1f46272cf636", "Jeff Jboy Gran"),
    "James Deans":      ("6257b1c3-bbae-5937-b773-ffd5c2152715", "James Dean"),
    "Brent Welch":      ("95643c20-ec23-57a5-a742-5b44534ae959", "Brent Welch"),
    "Chris Ott":        ("8959d778-41d3-554e-a831-6e1f73534b0f", "Chris Ott"),
    "Brent Stewart":    ("144c052c-ff9e-5ccc-8e3f-4c5817f7613c", "Brent Stewart"),
    # Mike McCarthy: ambiguous (3 PT entries). Left unresolved.
    "Jimmy Caveney":    ("df329352-6f3b-5e98-b23b-1af6737d100b", "Jimmy Caveney"),
    "Lisa McDaniel":    ("f995558d-69d9-557a-892a-0e695d85250b", "Lisa McDaniel"),
    "Hung Chang":       ("36324869-c679-502e-8adb-e1a6ea777432", "Hung Chang"),
    "David Butcher":    ("c1f6d696-5444-5cd0-919f-9dd06ca54bea", "David Butcher"),
    "Bill Langbehn":    ("40fa16ff-a418-5da9-914e-3a09e231f2a7", "Bill Langbehn"),
    "Radhy Esposito":   ("71a7d160-629d-50d4-b5ea-8f24bfd85a7a", "Radhy Esposito"),
    "Jimmy Evans":      ("1df424ed-8425-5452-ac6a-f7abb11bb468", "Jimmy Evans"),
    "J.J. Jones":       ("8fc6832d-0715-5df5-b96c-9bf5fdad2a9a", "JJ Jones"),
    "Jody Welch":       ("e333a5e6-3d76-513c-a9a4-f34c09c31b96", "Jody Welch"),
    "Tim Tucker":       ("22b351a0-b875-5d6a-b91e-519074f988a8", "Tim Tucker"),
    "Wayne Foresman":   ("639c9b0d-f8f6-5a04-ad4d-0ffc908e2282", "Wayne Foresman"),
    "Jason Davis":      ("ebf2e110-5d96-51f4-8e32-45de6c759ee7", "Jason Davis"),
    "Kerry Chun":       ("ed0fa218-2a4b-5697-a596-9ee141732268", "Kerry Chun"),
    "Greg Durrett":     ("64337714-3133-53d6-b29a-511ae9f14920", "Greg Durrett"),
    "Greg Landis":      ("e3d3cfae-53a0-5dbf-a6d4-1ead2a224b7a", "Greg Landis"),
    "Lonya Julin":      ("0bb608ec-43de-5a37-a2a7-9e5c5717853a", "Lonya Julin"),
    "Dea Evans":        ("bbd1e614-ad30-5a7e-bd75-501dceb9664b", "Dea Evans"),
    "Anne Miller":      ("20e4a023-1bb9-5d19-a6dc-73da6eb30a8d", "Anne Miller"),
    "Debbie Fisher":    ("d8bab804-1700-51fb-b5e6-2c2d7eab0e46", "Debbie Fisher"),
    "Gina Meyer":       None,  # not in PT
    "Ben Hutchinson":   ("89d1406d-ac35-5820-829c-1ecf239b5dd2", "Ben Hutchinson"),
    "Ben Little":       ("b1f1ebbc-9b2a-58f9-bb36-6c8b6d751f19", "Ben Little"),
    "Kern McNutt":      ("269fbcf6-8772-5e44-8572-78a0916722c6", "Kern McNutt"),
    "Edwin Veltman":    ("62bdd1f4-4414-5259-9996-91770a34d09f", "Edwin Veltman"),
    "Craig McNair":     ("048574ff-0ca9-509a-863f-22f02a6ef7b9", "Craig McNair"),
    "Sage Woodmansee":  ("eda693e2-258b-595c-b7d1-af67657564ce", "Sage Woodmansee"),
    "Chris Young":      ("646452e8-7ede-58d9-aa7d-f82c092800ad", "Chris Young"),
    "Craig Lewis":      ("ccd309d7-5e6a-565a-b7c6-d45ab1ca0091", "Craig Lewis"),
    "Eric Duggan":      ("9801a0a0-14a3-5d66-b574-dc69fb1036ad", "Eric Duggan"),
    "Randy Pace":       ("d97bdb08-5caa-51bc-8f26-aca27accb727", "Randy Pace"),
    "Mike Scheele":     ("ee312ddb-1d0b-5e91-b59f-1860309e0c67", "Mike Scheele"),
    "Rob Sorenson":     None,  # not in PT
    "Brian Jones":      ("c51c1328-bb7f-53ca-ad7c-2d17cc60bd87", "Brian Jones"),
    "Marc Weber":       None,  # not in PT (one-time appearance, competing from abroad)
    "Bob Silva":        None,  # not in PT
    "Doug Toth":        None,  # not in PT
    "Chris Cleaver":    ("ae007583-9bf0-520a-ba7c-b2e6f52b12ef", "Chris Cleaver"),
    "Sunil Jani":       ("8f15594c-f3f9-53d9-bfe5-409e746b7c29", "Sunil Jani"),
    "Fred Husted":      ("a67698e9-2dd8-5853-b75a-3c6e535d7610", "Red Fred Husted"),
    # Steve Campbell: not in PT (common name, no other appearances found)
    "Steve Campbell":   None,
    # Ben Lindahl: not in PT
    "Ben Lindahl":      None,
    "Mike McCarthy":    None,  # ambiguous: 3 PT entries (Michael, mike, Mike Patrick)
}

# ── Division data ─────────────────────────────────────────────────────────────
# (division_canon, division_category, place, p1_name, p2_name_or_None)
PLACEMENTS = [
    # Open Singles Freestyle
    ("Open Singles Freestyle",    "freestyle", 1, "Tuan Vu",      None),
    ("Open Singles Freestyle",    "freestyle", 2, "Sam Conlon",   None),
    ("Open Singles Freestyle",    "freestyle", 3, "Steve Goldberg", None),
    ("Open Singles Freestyle",    "freestyle", 4, "John Leys",    None),
    # Intermediate Singles Freestyle
    ("Intermediate Singles Freestyle", "freestyle", 1, "Aaron de Glanville", None),
    ("Intermediate Singles Freestyle", "freestyle", 2, "Jeff Gran",          None),
    ("Intermediate Singles Freestyle", "freestyle", 3, "James Deans",        None),
    # Open Doubles Net
    ("Open Doubles Net", "net",  1, "Brent Welch",    "Chris Ott"),
    ("Open Doubles Net", "net",  2, "Brent Stewart",  "Mike McCarthy"),
    ("Open Doubles Net", "net",  3, "Jimmy Caveney",  "Lisa McDaniel"),
    ("Open Doubles Net", "net",  4, "Hung Chang",     "David Butcher"),
    ("Open Doubles Net", "net",  5, "Bill Langbehn",  "John Leys"),
    ("Open Doubles Net", "net",  6, "Radhy Esposito", "Jimmy Evans"),
    ("Open Doubles Net", "net",  7, "J.J. Jones",     "Steve Campbell"),
    ("Open Doubles Net", "net",  8, "Jody Welch",     "Tuan Vu"),
    ("Open Doubles Net", "net",  9, "Tim Tucker",     "Wayne Foresman"),
    ("Open Doubles Net", "net", 10, "Steve Goldberg", "Jason Davis"),
    ("Open Doubles Net", "net", 11, "Kerry Chun",     "Ben Lindahl"),
    ("Open Doubles Net", "net", 12, "Greg Durrett",   "Greg Landis"),
    # Mixed Doubles Net
    ("Mixed Doubles Net", "net", 1, "Lonya Julin",   "David Butcher"),
    ("Mixed Doubles Net", "net", 2, "Dea Evans",     "Radhy Esposito"),
    ("Mixed Doubles Net", "net", 3, "Jimmy Evans",   "Debbie Fisher"),
    ("Mixed Doubles Net", "net", 4, "Gina Meyer",    "J.J. Jones"),
    ("Mixed Doubles Net", "net", 5, "Anne Miller",   "Greg Durrett"),
    # Intermediate Doubles (net)
    ("Intermediate Doubles", "net", 1, "Ben Hutchinson", "Ben Little"),
    ("Intermediate Doubles", "net", 2, "Kern McNutt",    "Edwin Veltman"),
    ("Intermediate Doubles", "net", 3, "Craig McNair",   "Sage Woodmansee"),
    ("Intermediate Doubles", "net", 3, "Chris Young",    "Aaron de Glanville"),
    ("Intermediate Doubles", "net", 5, "Craig Lewis",    "Eric Duggan"),
    ("Intermediate Doubles", "net", 5, "Lonya Julin",    "Randy Pace"),
    ("Intermediate Doubles", "net", 7, "Dea Evans",      "Mike Scheele"),
    ("Intermediate Doubles", "net", 8, "Rob Sorenson",   "Brian Jones"),
    # Novice Doubles (net)
    ("Novice Doubles", "net", 1, "Marc Weber",  "Bob Silva"),
    ("Novice Doubles", "net", 2, "Doug Toth",   "Chris Cleaver"),
    ("Novice Doubles", "net", 3, "Sunil Jani",  "Fred Husted"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def resolve(name):
    """Return (person_id, person_canon, is_unresolved) for a given source name."""
    entry = RESOLVE.get(name)
    if entry is None:
        # Unresolved real person
        return ("", name, True)
    return (entry[0], entry[1], False)


def team_key_for_both_unresolved(tdn):
    """Generate a stable 12-char hex key from team display name."""
    return hashlib.md5(tdn.encode("utf-8")).hexdigest()[:12]


def make_base(div_canon, div_cat, place, coverage="mostly_complete"):
    return {
        "event_id":          EVENT_ID,
        "year":              "1997",
        "division_canon":    div_canon,
        "division_category": div_cat,
        "place":             str(place),
        "coverage_flag":     coverage,
        "division_raw":      "",
    }


def make_single_row(base, name):
    pid, canon, unres = resolve(name)
    row = dict(base)
    row["competitor_type"]  = "player"
    row["person_id"]        = pid
    row["person_canon"]     = canon
    row["person_unresolved"]= "1" if unres else ""
    row["team_display_name"]= ""
    row["team_person_key"]  = ""
    row["norm"]             = canon.lower()
    return [row]


def make_team_rows(base, p1_name, p2_name):
    pid1, can1, unres1 = resolve(p1_name)
    pid2, can2, unres2 = resolve(p2_name)

    tdn = f"{can1} / {can2}"
    rows = []

    if not unres1 and not unres2:
        # Both resolved → 2 rows, tpk = pid1
        tpk = pid1
        for pid, canon in [(pid1, can1), (pid2, can2)]:
            r = dict(base)
            r["competitor_type"]   = "team"
            r["person_id"]         = pid
            r["person_canon"]      = canon
            r["person_unresolved"] = ""
            r["team_display_name"] = tdn
            r["team_person_key"]   = tpk
            r["norm"]              = canon.lower()
            rows.append(r)

    elif not unres1 and unres2:
        # p1 resolved only → 1 row for p1, tpk = ''
        r = dict(base)
        r["competitor_type"]   = "team"
        r["person_id"]         = pid1
        r["person_canon"]      = can1
        r["person_unresolved"] = ""
        r["team_display_name"] = tdn
        r["team_person_key"]   = ""
        r["norm"]              = can1.lower()
        rows.append(r)

    elif unres1 and not unres2:
        # p2 resolved only → 1 row for p2, tpk = ''
        r = dict(base)
        r["competitor_type"]   = "team"
        r["person_id"]         = pid2
        r["person_canon"]      = can2
        r["person_unresolved"] = ""
        r["team_display_name"] = tdn
        r["team_person_key"]   = ""
        r["norm"]              = can2.lower()
        rows.append(r)

    else:
        # Both unresolved → 2 rows with unresolved, tpk = md5 hash of tdn
        tpk = team_key_for_both_unresolved(tdn)
        for canon in [can1, can2]:
            r = dict(base)
            r["competitor_type"]   = "team"
            r["person_id"]         = ""
            r["person_canon"]      = canon
            r["person_unresolved"] = "1"
            r["team_display_name"] = tdn
            r["team_person_key"]   = tpk
            r["norm"]              = canon.lower()
            rows.append(r)

    return rows


# ── Load PBP ──────────────────────────────────────────────────────────────────

with open(IN_FILE, newline="", encoding="utf-8") as f:
    dr = csv.DictReader(f)
    fieldnames = list(dr.fieldnames)
    rows = list(dr)

print(f"Loaded {len(rows):,} rows from v69")

# ── Remove all existing rows for this event ───────────────────────────────────

before = len(rows)
rows = [r for r in rows if r["event_id"] != EVENT_ID]
removed = before - len(rows)
print(f"Removed {removed} rows for event {EVENT_ID}")
assert removed == 36, f"Expected 36 removed, got {removed}"

# ── Build new rows ────────────────────────────────────────────────────────────

new_rows = []
for (div_canon, div_cat, place, p1, p2) in PLACEMENTS:
    base = make_base(div_canon, div_cat, place)
    if p2 is None:
        new_rows.extend(make_single_row(base, p1))
    else:
        new_rows.extend(make_team_rows(base, p1, p2))

print(f"Generated {len(new_rows)} new rows for event {EVENT_ID}")

# Verify expected row count
# Singles: 4+3=7 rows
# Open Doubles Net: 12 teams → mostly both-resolved; Mike McCarthy unresolved:
#   p2=Brent Stewart(resolved)+Mike McCarthy(unres) → 1 row
#   p7=J.J.Jones(resolved)+Steve Campbell(unres) → 1 row
#   p11=Kerry Chun(resolved)+Ben Lindahl(unres) → 1 row
#   all others both resolved → 2 rows each = 9 teams × 2 = 18
#   Total Open Doubles Net = 3×1 + 9×2 = 21 rows
# Mixed Doubles Net: Gina Meyer unres, J.J.Jones resolved:
#   p4=Gina Meyer(unres)+J.J.Jones(resolved) → 1 row (for J.J.)
#   all others both resolved → 4×2 = 8 rows
#   Total Mixed Doubles = 8+1 = 9 rows
# Intermediate Doubles: Rob Sorenson unres, Brian Jones resolved:
#   p8=Rob Sorenson(unres)+Brian Jones(resolved) → 1 row
#   all others both resolved → 7×2 = 14 rows
#   Total Intermediate Doubles = 14+1 = 15 rows
# Novice Doubles: Marc Weber(unres)+Bob Silva(unres)=2; Doug Toth(unres)+Chris Cleaver(res)=1; Sunil+Fred(res)=2
#   p1: both unres → 2 rows
#   p2: Toth unres, Cleaver res → 1 row
#   p3: both res → 2 rows
#   Total Novice = 5 rows
# Grand total: 7 + 21 + 9 + 15 + 5 = 57
assert len(new_rows) == 57, f"Expected 57 new rows, got {len(new_rows)}"

# ── Merge and write ───────────────────────────────────────────────────────────

rows.extend(new_rows)

with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

print(f"\nWritten {len(rows):,} rows → {OUT_FILE.name}")
print(f"Net change: {len(rows) - (before - removed + removed):+d} "
      f"({removed} removed, {len(new_rows)} added, delta={len(new_rows)-removed:+d})")
