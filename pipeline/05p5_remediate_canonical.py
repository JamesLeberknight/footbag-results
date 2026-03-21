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
  4. (removed)            — was "force participant_order=1 for singles"; stage 05 now
                            emits sequential participant_order for all disciplines, making
                            (event_key, discipline_key, placement, participant_order) unique
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

TARGET_EVENT = "1997_eugene_celebration"
TARGET_DIV = "Doubles Golf"

VALID_TEAMS = {
    "Jim Fitzgerald / Jack Schoolcraft",
    "Jeff Johnson / Steve Dusablon",
    "Becca English-Ross / Dave Bernard",
    "Brent Welch / Brandon Crum",
    "Aaron Gregg / Bobby Heiney",
}

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
        if not cleaned:
            # Cleaning stripped the entire name (e.g. "()" → "").
            # Use a meaningful sentinel rather than leaving blank.
            cleaned = "__UNKNOWN_PARTNER__"
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

# ── Fix 4: (removed) ──────────────────────────────────────────────────────────
# Stage 05 now emits sequential participant_order for all disciplines (singles
# and doubles alike), so (event_key, discipline_key, placement, participant_order)
# is always a unique key.  No post-processing needed here.

tie_fixes = 0  # keep variable for report formatting

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

# ── Targeted Team Remediation ─────────────────────────────────────────────────
# Keep only a known team whitelist for one specific event/division.

print("\n[Targeted remediation] Filtering invalid teams for specific event/division...")

target_discipline_keys = {
    row["discipline_key"]
    for row in disciplines
    if row["event_key"] == TARGET_EVENT and row.get("discipline_name") == TARGET_DIV
}

valid_slots: set[tuple[str, str, str]] = set()
if target_discipline_keys:
    names_by_slot_order: dict[tuple[str, str, str], dict[int, str]] = defaultdict(dict)
    for row in participants:
        event_key = row["event_key"]
        discipline_key = row["discipline_key"]
        if event_key != TARGET_EVENT or discipline_key not in target_discipline_keys:
            continue
        slot = (event_key, discipline_key, row["placement"])
        names_by_slot_order[slot][int(row["participant_order"])] = row["display_name"]

    for slot, order_map in names_by_slot_order.items():
        name_1 = order_map.get(1, "").strip()
        name_2 = order_map.get(2, "").strip()
        if not name_1 or not name_2:
            continue
        team_name = f"{name_1} / {name_2}"
        if team_name in VALID_TEAMS:
            valid_slots.add(slot)

before_results = len(results)
results = [
    row for row in results
    if (
        row["event_key"] != TARGET_EVENT
        or row["discipline_key"] not in target_discipline_keys
        or (row["event_key"], row["discipline_key"], row["placement"]) in valid_slots
    )
]

before_participants = len(participants)
participants = [
    row for row in participants
    if (
        row["event_key"] != TARGET_EVENT
        or row["discipline_key"] not in target_discipline_keys
        or (row["event_key"], row["discipline_key"], row["placement"]) in valid_slots
    )
]

print(f"  Target discipline keys: {len(target_discipline_keys):,}")
print(f"  Valid team slots kept:  {len(valid_slots):,}")
print(f"  Results removed:        {before_results - len(results):,}")
print(f"  Participants removed:   {before_participants - len(participants):,}")

# ── Fix 6: Sequential Placement Normalization ─────────────────────────────────
# For any discipline where a placement slot has the wrong number of participants
# (>1 for singles, ≠2 for doubles), renumber all placements in that discipline
# sequentially so each slot gets exactly the expected count.
# Singles: each participant gets its own placement (1, 2, 3, ...).
# Doubles: participants are grouped into consecutive pairs, each pair is a team
# at one sequential placement.  Lone remainders get a ghost partner.
# Preserves original placement in notes as "seq_from:<N>" when changed.

print("\n[Fix 6] Sequential placement normalization...")

team_type_lookup = {
    (r["event_key"], r["discipline_key"]): r["team_type"]
    for r in disciplines
}

# Build slot → rows mapping
from collections import defaultdict as _dd

slots_map: dict = _dd(list)
for row in participants:
    k = (row["event_key"], row["discipline_key"], row["placement"])
    slots_map[k].append(row)

# Identify which (event, disc) have violations
disc_slots: dict = _dd(list)  # (ek, dk) → [(int_placement, rows)]
for (ek, dk, pl), rows in slots_map.items():
    disc_slots[(ek, dk)].append((int(pl), rows))

# Sort each discipline's slots by original placement
for k in disc_slots:
    disc_slots[k].sort(key=lambda x: x[0])

seq_normalized = 0
normalized_groups = 0
normalized_participants = 0
new_participants_6: list = []
new_results_map: dict = {}  # (ek, dk, pl_str) → result row

# Pre-populate result map from existing results
results_by_key: dict = {}
for r in results:
    results_by_key[(r["event_key"], r["discipline_key"], r["placement"])] = r

for (ek, dk), place_groups in disc_slots.items():
    tt = team_type_lookup.get((ek, dk), "singles")
    expected = 2 if tt == "doubles" else 1

    has_violation = any(len(rows) != expected for _, rows in place_groups)
    if not has_violation:
        # No change — copy all rows verbatim
        for _, rows in place_groups:
            for row in rows:
                rk = (row["event_key"], row["discipline_key"], row["placement"])
                new_participants_6.append(row)
                if rk not in new_results_map:
                    new_results_map[rk] = results_by_key.get(rk, {
                        "event_key": ek, "discipline_key": dk,
                        "placement": row["placement"],
                        "score_text": "", "notes": "", "source": "",
                    })
        continue

    normalized_groups += 1
    normalized_participants += sum(len(rows) for _, rows in place_groups)

    # Collect all participants for this discipline in order
    all_rows: list = []
    for _, rows in place_groups:
        sorted_rows = sorted(rows, key=lambda r: int(r["participant_order"]))
        all_rows.extend(sorted_rows)

    if tt == "singles":
        # Each participant → own sequential placement, participant_order = 1
        next_place = 1
        for row in all_rows:
            orig = row["placement"]
            new_row = dict(row)
            new_row["placement"] = str(next_place)
            new_row["participant_order"] = "1"
            if orig != str(next_place):
                note = f"seq_from:{orig}"
                new_row["notes"] = (new_row["notes"] + ";" + note).lstrip(";")
                seq_normalized += 1
            rk = (ek, dk, str(next_place))
            new_participants_6.append(new_row)
            if rk not in new_results_map:
                old_rk = (ek, dk, orig)
                base = results_by_key.get(old_rk, {
                    "event_key": ek, "discipline_key": dk, "placement": orig,
                    "score_text": "", "notes": "", "source": "",
                })
                new_result = dict(base)
                new_result["placement"] = str(next_place)
                new_results_map[rk] = new_result
            next_place += 1

    else:  # doubles
        # Group into teams of 2 (consecutive pairs), assign sequential placements
        next_place = 1
        i = 0
        while i < len(all_rows):
            pair = all_rows[i:i + 2]
            i += 2
            # If pair is short, pad with a ghost
            if len(pair) == 1:
                ghost = {
                    "event_key": ek, "discipline_key": dk,
                    "placement": str(next_place),
                    "participant_order": "2",
                    "display_name": "__UNKNOWN_PARTNER__",
                    "person_id": "", "notes": "auto:ghost_partner",
                }
                pair.append(ghost)
            orig = pair[0]["placement"]
            rk = (ek, dk, str(next_place))
            for order_idx, row in enumerate(pair, start=1):
                new_row = dict(row)
                new_row["placement"] = str(next_place)
                new_row["participant_order"] = str(order_idx)
                if orig != str(next_place):
                    note = f"seq_from:{orig}"
                    new_row["notes"] = (new_row["notes"] + ";" + note).lstrip(";")
                    seq_normalized += 1
                new_participants_6.append(new_row)
            if rk not in new_results_map:
                old_rk = (ek, dk, orig)
                base = results_by_key.get(old_rk, {
                    "event_key": ek, "discipline_key": dk, "placement": orig,
                    "score_text": "", "notes": "", "source": "",
                })
                new_result = dict(base)
                new_result["placement"] = str(next_place)
                new_results_map[rk] = new_result
            next_place += 1

participants = new_participants_6

# Rebuild results from normalized participants (one row per unique slot)
used_result_keys = {
    (r["event_key"], r["discipline_key"], r["placement"]) for r in participants
}
results = [v for k, v in new_results_map.items() if k in used_result_keys]

# Re-sort participants
participants.sort(key=lambda r: (
    r["event_key"],
    r["discipline_key"],
    int(r["placement"]) if r["placement"].isdigit() else 0,
    int(r["participant_order"]),
))
# Re-sort results
results.sort(key=lambda r: (
    r["event_key"],
    r["discipline_key"],
    int(r["placement"]) if r["placement"].isdigit() else 0,
))

print(f"  Participants renumbered: {seq_normalized:,}")
print(f"[Fix 6] placement-normalized groups: {normalized_groups}")
print(f"[Fix 6] placement-normalized participants: {normalized_participants}")

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
║ Fix 6  Participants renumbered (seq)     {seq_normalized:>6,} ║
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
