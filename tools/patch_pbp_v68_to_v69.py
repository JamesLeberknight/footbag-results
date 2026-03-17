"""
tools/patch_pbp_v68_to_v69.py
PBP patch: fix Zocha Jam 2005 and Zocha Jam 2006 quarantined events.

Zocha 2005 (1127155729):
  The second "Open double Freestyle" block in the source is actually Open Doubles Net
  (appears after the Open Singles Net section in the HTML). The parser duplicated
  the division name from the first block, creating dup (div, place) pairs.
  Fix: rename 5 team rows to division_canon='Open Doubles Net', category='net'.

Zocha 2006 (1158263300):
  Three divisions were merged into "Women Freestyle" because parser failed to recognize
  "Interdmediate:" (typo) and lacked coverage for "Most Rippin Run".
  Fix 1: 3 Women Freestyle rows (Piotr Kurt, Kacper Kudyniuk, Michal Miszkiel)
         → division_canon='Intermediate Freestyle'
  Fix 2: 3 Women Freestyle rows (Vaclav Klouda, Szymon Kalwak, Kamil Wysocki)
         → division_canon='Shred30' (replacing 3 unresolved __NON_PERSON__ Shred30 rows)
  Fix 3: Remove the 3 unresolved Shred30 rows (duplicated by fix 2)
  Fix 4: Add 3 new Most Rippin Run rows (Vasek Klouda p1, Damian Gielnicki p2,
         Wiktor Debski p3) — these placements were previously dropped by the parser.

Input:  inputs/identity_lock/Placements_ByPerson_v68.csv
Output: inputs/identity_lock/Placements_ByPerson_v69.csv
"""

import csv
import copy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "inputs" / "identity_lock"

IN_FILE  = LOCK / "Placements_ByPerson_v68.csv"
OUT_FILE = LOCK / "Placements_ByPerson_v69.csv"

csv.field_size_limit(10 * 1024 * 1024)

# ── Load ──────────────────────────────────────────────────────────────────────

with open(IN_FILE, newline="", encoding="utf-8") as f:
    dr = csv.DictReader(f)
    fieldnames = list(dr.fieldnames)
    rows = list(dr)

print(f"Loaded {len(rows):,} rows from v68")

# ── Zocha Jam 2005 fix ────────────────────────────────────────────────────────
# Second "Open double Freestyle" block → Open Doubles Net

ZOCHA_2005 = "1127155729"
NET_DOUBLES_TEAMS = {
    "Jan Struz / Michał Róg",
    "Kamil Wysocki / Sławek Brzeziński",
    "Piotr Bałtrukiewicz / Grzesiek Łatuszyński",
    "Antoni Szczeniowski / Damian Budzik",
    "Patrycja Szczeniowska / Łukasz Osiński",
}

zocha05_fixed = 0
for row in rows:
    if (row["event_id"] == ZOCHA_2005
            and row["division_canon"] == "Open Double Freestyle"
            and row["team_display_name"] in NET_DOUBLES_TEAMS):
        row["division_canon"]    = "Open Doubles Net"
        row["division_category"] = "net"
        zocha05_fixed += 1

print(f"\nZocha 2005: renamed {zocha05_fixed} 'Open Double Freestyle' → 'Open Doubles Net'")
assert zocha05_fixed == 5, f"Expected 5 rows, got {zocha05_fixed}"

# ── Zocha Jam 2006 fixes ──────────────────────────────────────────────────────

ZOCHA_2006 = "1158263300"

# Fix 1: Piotr Kurt / Kacper Kudyniuk / Michal Miszkiel → Intermediate Freestyle
INTERMEDIATE_PIDS = {
    "cceb5e54-aea1-5282-ac59-6074119056bb",  # Piotr Kurt
    "3058e63d-f79f-58de-a433-2732a7caca12",  # Kacper Kudyniuk
    "68248357-f8c2-555c-acf9-2c42d9e9fb82",  # Michal Miszkiel
}
# Fix 2: Vaclav Klouda / Szymon Kalwak / Kamil Wysocki → Shred30
SHRED30_PIDS = {
    "98ce4e04-05ca-56ca-b8c6-83f51c164c89",  # Vaclav Klouda
    "4ae1304e-960b-554f-8ce7-c59e0d7607b2",  # Szymon Kalwak
    "565a1720-a819-5c05-b44e-0a53c2027237",  # Kamil Wysocki
}

fix1_count = fix2_count = 0
for row in rows:
    if row["event_id"] != ZOCHA_2006 or row["division_canon"] != "Women Freestyle":
        continue
    if row["person_id"] in INTERMEDIATE_PIDS:
        row["division_canon"] = "Intermediate Freestyle"
        fix1_count += 1
    elif row["person_id"] in SHRED30_PIDS:
        row["division_canon"] = "Shred30"
        fix2_count += 1

print(f"Zocha 2006 fix1: {fix1_count} Women Freestyle rows → 'Intermediate Freestyle'")
print(f"Zocha 2006 fix2: {fix2_count} Women Freestyle rows → 'Shred30'")
assert fix1_count == 3
assert fix2_count == 3

# Fix 3: Remove unresolved Shred30 rows (replaced by fix2 rows above)
before = len(rows)
rows = [
    r for r in rows
    if not (r["event_id"] == ZOCHA_2006
            and r["division_canon"] == "Shred30"
            and r["person_id"] == "")
]
removed = before - len(rows)
print(f"Zocha 2006 fix3: removed {removed} unresolved Shred30 rows")
assert removed == 3

# Fix 4: Add Most Rippin Run rows (previously dropped by parser)
# Source: 1. Vasek Klouda, 2. Damian Gielnicki, 3. Wiktor Debski
MOST_RIPPIN_RUN_ENTRIES = [
    ("1", "98ce4e04-05ca-56ca-b8c6-83f51c164c89", "Vaclav Klouda",    "vaclav klouda"),
    ("2", "ac1268dc-a961-568f-860e-63e9ea815c01", "Damian Gielnicki", "damian gielnicki"),
    ("3", "c917c04b-83fa-5c1a-8f2c-292f24db5616", "Wiktor Debski",    "wiktor debski"),
]
new_rows = []
for place, pid, canon, norm in MOST_RIPPIN_RUN_ENTRIES:
    new_rows.append({
        "event_id":         ZOCHA_2006,
        "year":             "2006",
        "division_canon":   "Most Rippin Run",
        "division_category": "freestyle",
        "place":            place,
        "competitor_type":  "player",
        "person_id":        pid,
        "team_person_key":  "",
        "person_canon":     canon,
        "team_display_name": "",
        "coverage_flag":    "partial",
        "person_unresolved": "",
        "norm":             norm,
        "division_raw":     "",
    })

rows.extend(new_rows)
print(f"Zocha 2006 fix4: added {len(new_rows)} Most Rippin Run rows")

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\nTotal rows: {len(rows):,} (was 28,677; net delta = {len(rows) - 28677:+d})")

# ── Save ──────────────────────────────────────────────────────────────────────

with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)

print(f"\nSaved {OUT_FILE.name} with {len(rows):,} rows")

# ── Verify no dup pairs ───────────────────────────────────────────────────────

from collections import Counter, defaultdict

dup_events = {"1127155729", "1158263300"}
for eid in dup_events:
    ev = [r for r in rows if r["event_id"] == eid]
    place_counts: Counter = Counter(
        (r["division_canon"], r["place"], r["team_display_name"])
        for r in ev
    )
    dups = [(k, v) for k, v in place_counts.items() if v > 1]
    if dups:
        print(f"\nWARNING: {eid} still has {len(dups)} dup (div,place,team) combos:")
        for (d, p, t), n in sorted(dups):
            print(f"  {d!r} p{p}  team={t!r}  ×{n}")
    else:
        print(f"PASS: {eid} — no dup (div,place,team) pairs")
