"""
pipeline/05p5_remediate_canonical.py
Stage 05p5 — Canonical CSV Remediation

Runs immediately after stage 05 (05_export_canonical_csv.py) and applies
five logic fixes to the canonical CSV set before downstream consumption.

Fixes (in order):
  1. Identity Sync        — overwrite display_name from persons.csv when person_id present
  2. Regex Deep-Clean     — strip ordinals, scores, parentheticals for unresolved rows
  3. Singles Density Check— remap doubles→singles when participant density = 1.0
                            unless the discipline appears in keep_doubles_overrides.csv
  4. Tie Enforcement      — force participant_order=1 for all singles disciplines
  5. Ghost Partnering     — for doubles disciplines still missing a partner slot,
                            insert __UNKNOWN_PARTNER__ at participant_order=2

Keep-doubles override:
  Create inputs/keep_doubles_overrides.csv with columns event_key, discipline_key
  to prevent specific disciplines from being remapped to singles even at density 1.0.
  These will instead receive a ghost __UNKNOWN_PARTNER__ partner row.

Input/output: out/canonical/ (repo-relative)
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

ROOT      = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "out" / "canonical"
OVERRIDES = ROOT / "inputs" / "keep_doubles_overrides.csv"

csv.field_size_limit(10 * 1024 * 1024)

# ── Helpers ───────────────────────────────────────────────────────────────────

def load(name: str) -> tuple[list[dict], list[str]]:
    path = CANONICAL / name
    with open(path, newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        rows = list(dr)
        return rows, list(dr.fieldnames)


def save(name: str, rows: list[dict], fieldnames: list[str]) -> None:
    with open(CANONICAL / name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {name} ({len(rows):,} rows)")


# ── Load ──────────────────────────────────────────────────────────────────────

print("Stage 05p5: Canonical CSV Remediation")
print(f"  Source: {CANONICAL}\n")

events,       fields_events       = load("events.csv")
disciplines,  fields_disciplines  = load("event_disciplines.csv")
results,      fields_results      = load("event_results.csv")
participants, fields_participants = load("event_result_participants.csv")
persons,      fields_persons      = load("persons.csv")

print(f"  Loaded: {len(events)} events, {len(disciplines)} disciplines, "
      f"{len(results)} results, {len(participants)} participants, "
      f"{len(persons)} persons")

# Load keep-doubles overrides (optional)
keep_doubles: set[tuple[str, str]] = set()
if OVERRIDES.exists():
    with open(OVERRIDES, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            keep_doubles.add((row["event_key"].strip(), row["discipline_key"].strip()))
    print(f"  Keep-doubles overrides: {len(keep_doubles)} discipline(s)")
else:
    print(f"  Keep-doubles overrides: none (create {OVERRIDES.name} to add)")

# ── Fix 1 & 2: Identity Sync + Regex Deep-Clean ───────────────────────────────

print("\n[Fix 1+2] Identity sync & regex cleaning...")

person_name_map = {r["person_id"]: r["person_name"] for r in persons}

_RE_DAY_PREFIX   = re.compile(r"^[A-Za-z]+:\s*\d+\.\s*")         # "Saturday: 1. "
_RE_SCORE_SUFFIX = re.compile(r"\s+\d+\.\d+.*$")                  # " 9.20 9.20 1"
_RE_ORDINAL      = re.compile(r"^\d+(?:st|nd|rd|th)?[.):\-]?\s+", re.IGNORECASE)
_RE_PAREN        = re.compile(r"\s*\([^)]*\)\s*$")
_RE_TIE_LABEL    = re.compile(r"\(tie\)", re.IGNORECASE)           # "(tie)" annotation
_RE_SPACES       = re.compile(r"\s{2,}")

def clean_unresolved(name: str) -> str:
    name = _RE_DAY_PREFIX.sub("", name)
    name = _RE_SCORE_SUFFIX.sub("", name)
    name = _RE_ORDINAL.sub("", name)
    name = _RE_TIE_LABEL.sub("", name)
    name = _RE_PAREN.sub("", name)
    name = _RE_SPACES.sub(" ", name).strip()
    return name

names_from_master  = 0
names_regex_cleaned = 0

for row in participants:
    pid = row.get("person_id", "")
    if pid and pid in person_name_map:
        canonical_name = person_name_map[pid]
        if row["display_name"] != canonical_name:
            row["display_name"] = canonical_name
            names_from_master += 1
    elif not pid:
        cleaned = clean_unresolved(row["display_name"])
        if cleaned != row["display_name"]:
            row["display_name"] = cleaned
            names_regex_cleaned += 1

print(f"  Overwritten from person master: {names_from_master:,}")
print(f"  Regex-cleaned (unresolved):     {names_regex_cleaned:,}")

# ── Fix 3: Singles Density Check ──────────────────────────────────────────────
# Doubles → singles when every placement slot has exactly 1 participant
# (density = 1.0), UNLESS the discipline is in the keep_doubles override set.

print("\n[Fix 3] Singles density check...")

# Count unique placements and total participants per (event_key, discipline_key)
placement_sets:   dict[tuple, set]  = defaultdict(set)
participant_count: dict[tuple, int] = defaultdict(int)

for row in participants:
    k = (row["event_key"], row["discipline_key"])
    placement_sets[k].add(row["placement"])
    participant_count[k] += 1

remapped   = 0
kept_double = 0

for row in disciplines:
    if row["team_type"] != "doubles":
        continue
    k = (row["event_key"], row["discipline_key"])
    n_placements = len(placement_sets.get(k, set()))
    n_participants = participant_count.get(k, 0)
    if n_placements == 0 or n_participants == 0:
        continue
    density = n_participants / n_placements
    if density != 1.0:
        continue

    if k in keep_doubles:
        # Keep as doubles; ghost partner inserted in Fix 5
        print(f"  KEEP doubles (override): {row['event_key']} / "
              f"{row['discipline_key']} ({row['discipline_name']})")
        kept_double += 1
    else:
        print(f"  WARN remap doubles→singles: {row['event_key']} / "
              f"{row['discipline_key']} ({row['discipline_name']}, "
              f"{n_placements} placements — partner data may be missing)")
        row["team_type"] = "singles"
        remapped += 1

print(f"  Remapped to singles: {remapped}")
print(f"  Kept doubles (override): {kept_double}")

# ── Fix 4: Tie Enforcement ────────────────────────────────────────────────────

print("\n[Fix 4] Tie enforcement (participant_order → 1 for singles)...")

singles_keys = {
    (r["event_key"], r["discipline_key"])
    for r in disciplines
    if r["team_type"] == "singles"
}

tie_fixes = 0
for row in participants:
    k = (row["event_key"], row["discipline_key"])
    if k in singles_keys and int(row["participant_order"]) > 1:
        row["participant_order"] = "1"
        tie_fixes += 1

print(f"  Fixed: {tie_fixes:,} row(s)")

# ── Fix 5: Ghost Partnering ───────────────────────────────────────────────────
# For doubles disciplines (including keep_doubles overrides) where a placement
# has only one participant, insert an __UNKNOWN_PARTNER__ row at order=2.

print("\n[Fix 5] Ghost partnering for doubles missing partner...")

doubles_keys = {
    (r["event_key"], r["discipline_key"])
    for r in disciplines
    if r["team_type"] == "doubles"
}

# Find placements that already have participant_order=2
has_partner: set[tuple] = set()
for row in participants:
    k = (row["event_key"], row["discipline_key"], row["placement"])
    if int(row["participant_order"]) == 2:
        has_partner.add(k)

ghost_rows = []
for row in participants:
    k = (row["event_key"], row["discipline_key"])
    slot = (row["event_key"], row["discipline_key"], row["placement"])
    if (k in doubles_keys
            and int(row["participant_order"]) == 1
            and slot not in has_partner):
        ghost_rows.append({
            "event_key":         row["event_key"],
            "discipline_key":    row["discipline_key"],
            "placement":         row["placement"],
            "participant_order": "2",
            "display_name":      "__UNKNOWN_PARTNER__",
            "person_id":         "",
            "notes":             "auto:ghost_partner",
        })
        has_partner.add(slot)  # prevent double-insertion

participants.extend(ghost_rows)

# Re-sort: (event_key, discipline_key, placement as int, participant_order as int)
participants.sort(key=lambda r: (
    r["event_key"],
    r["discipline_key"],
    int(r["placement"]) if r["placement"].isdigit() else 0,
    int(r["participant_order"]),
))

print(f"  Ghost rows inserted: {len(ghost_rows):,}")

# ── Save ──────────────────────────────────────────────────────────────────────

print("\nSaving...")
save("events.csv",                    events,       fields_events)
save("event_disciplines.csv",         disciplines,  fields_disciplines)
save("event_results.csv",             results,      fields_results)
save("event_result_participants.csv", participants, fields_participants)
save("persons.csv",                   persons,      fields_persons)

# ── Relational Health Report ──────────────────────────────────────────────────

# Final integrity counts
disc_set  = {(r["event_key"], r["discipline_key"]) for r in disciplines}
event_set = {r["event_key"] for r in events}

orphan_discs_results = sum(
    1 for r in results
    if (r["event_key"], r["discipline_key"]) not in disc_set
)
orphan_events_results = sum(
    1 for r in results
    if r["event_key"] not in event_set
)
orphan_discs_parts = sum(
    1 for r in participants
    if (r["event_key"], r["discipline_key"]) not in disc_set
)

singles_count = sum(1 for r in disciplines if r["team_type"] == "singles")
doubles_count = sum(1 for r in disciplines if r["team_type"] == "doubles")
ghost_count   = sum(1 for r in participants if r["display_name"] == "__UNKNOWN_PARTNER__")
resolved      = sum(1 for r in participants if r.get("person_id"))
unresolved    = sum(1 for r in participants if not r.get("person_id"))

print(f"""
╔══════════════════════════════════════════╗
║   Relational Health Report — Stage 05p5 ║
╠══════════════════════════════════════════╣
║ Fix 1  Names synced from person master   {names_from_master:>6,} ║
║ Fix 2  Names regex-cleaned (unresolved)  {names_regex_cleaned:>6,} ║
║ Fix 3  Disciplines remapped→singles      {remapped:>6,} ║
║        Disciplines kept doubles          {kept_double:>6,} ║
║ Fix 4  Tie rows enforced (order→1)       {tie_fixes:>6,} ║
║ Fix 5  Ghost partner rows inserted       {len(ghost_rows):>6,} ║
╠══════════════════════════════════════════╣
║ Disciplines: singles {singles_count:<5} doubles {doubles_count:<5}       ║
║ Participants: resolved {resolved:<6} unresolved {unresolved:<5} ║
║ Ghost partners total                    {ghost_count:>6,} ║
╠══════════════════════════════════════════╣
║ Orphaned discipline refs (results)       {orphan_discs_results:>6,} ║
║ Orphaned event refs (results)            {orphan_events_results:>6,} ║
║ Orphaned discipline refs (participants)  {orphan_discs_parts:>6,} ║
╚══════════════════════════════════════════╝
""")
