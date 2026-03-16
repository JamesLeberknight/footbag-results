"""
tools/99_canonical_fixer.py
Relational Integrity & Logic Remediation for canonical CSVs.

Fixes applied (in order):
  1. Discipline type correction  — doubles → singles when participant density = 1.0
  2. Tie enforcement             — participant_order forced to 1 for all singles rows
  3. Name sanitation             — display_name replaced from persons.csv (or regex-cleaned)
  4. Referential integrity audit — stubs created for orphaned keys

Reads from and writes back to out/canonical/.
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

ROOT      = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "out" / "canonical"

csv.field_size_limit(10 * 1024 * 1024)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name: str) -> list[dict]:
    with open(CANONICAL / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save(name: str, rows: list[dict], fieldnames: list[str]) -> None:
    with open(CANONICAL / name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def fieldnames_of(name: str) -> list[str]:
    with open(CANONICAL / name, newline="", encoding="utf-8") as f:
        return csv.DictReader(f).fieldnames


# ── Load ──────────────────────────────────────────────────────────────────────

print("Loading canonical CSVs...")
events       = load("events.csv")
disciplines  = load("event_disciplines.csv")
results      = load("event_results.csv")
participants = load("event_result_participants.csv")
persons      = load("persons.csv")

fields = {
    "events":                   fieldnames_of("events.csv"),
    "event_disciplines":        fieldnames_of("event_disciplines.csv"),
    "event_results":            fieldnames_of("event_results.csv"),
    "event_result_participants":fieldnames_of("event_result_participants.csv"),
    "persons":                  fieldnames_of("persons.csv"),
}

print(f"  events: {len(events)}, disciplines: {len(disciplines)}, "
      f"results: {len(results)}, participants: {len(participants)}, "
      f"persons: {len(persons)}")

# ── Fix 1: Discipline Type Correction ─────────────────────────────────────────
# Doubles discipline → singles when every participant slot has only 1 person
# (average participant_order = 1.0 across all placements in that discipline).
#
# NOTE: A density of 1.0 may indicate missing partner data rather than a true
# singles discipline. Each remapped discipline is printed for manual review.

print("\n[Fix 1] Discipline type correction (doubles → singles at density 1.0)...")

# Build density: max participant_order per (event_key, discipline_key)
slot_max = defaultdict(int)
for r in participants:
    k = (r["event_key"], r["discipline_key"])
    slot_max[k] = max(slot_max[k], int(r["participant_order"]))

# Count participants per (event_key, discipline_key, placement)
placement_counts = defaultdict(int)
for r in participants:
    k = (r["event_key"], r["discipline_key"], r["placement"])
    placement_counts[k] += 1

# Average participants per placement per discipline
disc_density: dict[tuple, float] = {}
placement_group = defaultdict(set)
for r in participants:
    placement_group[(r["event_key"], r["discipline_key"])].add(r["placement"])

for (ek, dk), placements in placement_group.items():
    total = sum(placement_counts[(ek, dk, p)] for p in placements)
    disc_density[(ek, dk)] = total / len(placements)

remapped_disciplines = 0
for row in disciplines:
    k = (row["event_key"], row["discipline_key"])
    if row["team_type"] == "doubles" and disc_density.get(k, 2.0) == 1.0:
        print(f"  WARN remap doubles→singles: {row['event_key']} / "
              f"{row['discipline_key']} ({row['discipline_name']}, "
              f"{len(placement_group[k])} placements — partner data may be missing)")
        row["team_type"] = "singles"
        remapped_disciplines += 1

print(f"  Remapped: {remapped_disciplines} discipline(s)")

# ── Fix 2: Tie Enforcement ────────────────────────────────────────────────────
# For all singles disciplines (including those just remapped), force
# participant_order to 1.  In singles, multiple names at the same placement
# represent ties, not a team — participant_order is meaningless.

print("\n[Fix 2] Tie enforcement (participant_order → 1 for singles)...")

singles_keys = {
    (row["event_key"], row["discipline_key"])
    for row in disciplines
    if row["team_type"] == "singles"
}

tie_fixes = 0
for row in participants:
    k = (row["event_key"], row["discipline_key"])
    if k in singles_keys and int(row["participant_order"]) > 1:
        row["participant_order"] = "1"
        tie_fixes += 1

print(f"  Fixed: {tie_fixes} participant row(s)")

# ── Fix 3: Name Sanitation via Person Master ──────────────────────────────────
# - Rows with person_id   → overwrite display_name from persons.csv
# - Rows without person_id → regex-clean display_name (strip leading ordinals
#   and trailing parentheticals)

print("\n[Fix 3] Name sanitation...")

person_name_map = {row["person_id"]: row["person_name"] for row in persons}

_RE_LEADING_ORDINAL   = re.compile(r"^\d+[.):\-]\s*")
_RE_LEADING_ORDINAL2  = re.compile(r"^\d+(?:st|nd|rd|th)\.?\s+", re.IGNORECASE)
_RE_TRAILING_PAREN    = re.compile(r"\s*\([^)]*\)\s*$")
_RE_MULTI_SPACE       = re.compile(r"\s{2,}")

def clean_unresolved_name(name: str) -> str:
    name = _RE_LEADING_ORDINAL2.sub("", name)
    name = _RE_LEADING_ORDINAL.sub("", name)
    name = _RE_TRAILING_PAREN.sub("", name)
    name = _RE_MULTI_SPACE.sub(" ", name).strip()
    return name

names_from_master  = 0
names_regex_cleaned = 0

for row in participants:
    pid = row["person_id"]
    if pid and pid in person_name_map:
        canonical = person_name_map[pid]
        if row["display_name"] != canonical:
            row["display_name"] = canonical
            names_from_master += 1
    elif not pid:
        cleaned = clean_unresolved_name(row["display_name"])
        if cleaned != row["display_name"]:
            row["display_name"] = cleaned
            names_regex_cleaned += 1

print(f"  Overwritten from person master: {names_from_master}")
print(f"  Regex-cleaned (unresolved):     {names_regex_cleaned}")

# ── Fix 4: Referential Integrity Audit ───────────────────────────────────────
# Check for orphaned event_key / discipline_key references and create stubs.

print("\n[Fix 4] Referential integrity audit...")

event_keys_set = {r["event_key"] for r in events}
disc_keys_set  = {(r["event_key"], r["discipline_key"]) for r in disciplines}

# Check results
orphan_events_in_results = set()
orphan_discs_in_results  = set()
for r in results:
    if r["event_key"] not in event_keys_set:
        orphan_events_in_results.add(r["event_key"])
    if (r["event_key"], r["discipline_key"]) not in disc_keys_set:
        orphan_discs_in_results.add((r["event_key"], r["discipline_key"]))

# Check participants
orphan_discs_in_parts = set()
for r in participants:
    if (r["event_key"], r["discipline_key"]) not in disc_keys_set:
        orphan_discs_in_parts.add((r["event_key"], r["discipline_key"]))

orphan_discs = orphan_discs_in_results | orphan_discs_in_parts

stubs_events = 0
for ek in sorted(orphan_events_in_results):
    print(f"  STUB event: {ek}")
    events.append({
        "event_key": ek, "legacy_event_id": "", "year": "",
        "event_name": f"[STUB] {ek}", "event_slug": ek,
        "start_date": "", "end_date": "", "city": "", "region": "",
        "country": "", "host_club": "", "event_type": "", "status": "stub",
        "notes": "Auto-generated stub for referential integrity",
        "source": "",
    })
    stubs_events += 1

stubs_discs = 0
for (ek, dk) in sorted(orphan_discs):
    print(f"  STUB discipline: {ek} / {dk}")
    disciplines.append({
        "event_key": ek, "discipline_key": dk,
        "discipline_name": f"[STUB] {dk}", "discipline_category": "",
        "team_type": "singles", "sort_order": "99", "coverage_flag": "",
        "notes": "Auto-generated stub for referential integrity",
    })
    disc_keys_set.add((ek, dk))
    stubs_discs += 1

if stubs_events == 0 and stubs_discs == 0:
    print("  OK — no orphaned references found")

# ── Save ──────────────────────────────────────────────────────────────────────

print("\nSaving cleaned CSVs...")
save("events.csv",                    events,       fields["events"])
save("event_disciplines.csv",         disciplines,  fields["event_disciplines"])
save("event_results.csv",             results,      fields["event_results"])
save("event_result_participants.csv", participants, fields["event_result_participants"])
save("persons.csv",                   persons,      fields["persons"])

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"""
════════════════════════════════════════
 Canonical Fixer — Summary
════════════════════════════════════════
 Fix 1  Disciplines remapped (doubles→singles): {remapped_disciplines}
 Fix 2  Participant rows tie-enforced:           {tie_fixes}
 Fix 3  Names from person master:               {names_from_master}
        Names regex-cleaned (unresolved):       {names_regex_cleaned}
 Fix 4  Event stubs created:                    {stubs_events}
        Discipline stubs created:               {stubs_discs}
════════════════════════════════════════
""")
