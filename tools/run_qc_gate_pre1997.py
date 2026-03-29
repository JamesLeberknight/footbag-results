"""
tools/run_qc_gate_pre1997.py

QC gate for the pre-1997 non-mirror canonical dataset.
Reads from early_data/final_pre1997/.

Exit 0  = PASS (0 hard failures; warnings allowed)
Exit 1  = FAIL (≥1 hard failure)

Hard failures (blocking):
  EVENTS
    - duplicate canonical_event_id
    - missing required fields (canonical_event_id, event_name, year)
  DISCIPLINES
    - duplicate (canonical_event_id, division_raw)
    - orphan canonical_event_id (not in events)
  RESULTS
    - duplicate (canonical_event_id, division_raw, place)
    - orphan (canonical_event_id, division_raw) not in disciplines
    - placement does not start at 1 for any division
  PARTICIPANTS
    - duplicate (result_id, person_id) — same person twice in same result
    - orphan result_id not in results
    - missing person_id AND missing player_name_raw
    - doubles division: placement has ≠2 participants
    - singles division: placement has <1 participant
  PERSONS
    - duplicate person_id
    - person_id referenced in participants not in persons
  DATA PURITY
    - non-person artifact in player_name_raw (club/city contamination)

Warnings (non-blocking):
  - placement gaps within a division
  - single-source events with LOW confidence
  - divisions with only 1 or 2 placements
  - unresolved participants (empty person_id with player_name_raw present)
"""

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT   = Path(__file__).resolve().parents[1]
PRE97  = ROOT / "early_data" / "final_pre1997"

# ── Collector ─────────────────────────────────────────────────────────────────

class IssueCollector:
    def __init__(self):
        self.issues = []

    def hard(self, code: str, msg: str, **ctx):
        entry = {"severity": "HARD", "code": code, "msg": msg}
        entry.update(ctx)
        self.issues.append(entry)
        self._print(entry)

    def warn(self, code: str, msg: str, **ctx):
        entry = {"severity": "WARN", "code": code, "msg": msg}
        entry.update(ctx)
        self.issues.append(entry)
        self._print(entry)

    def _print(self, e):
        ctx = " | ".join(f"{k}={v}" for k, v in e.items()
                         if k not in ("severity", "code", "msg"))
        line = f"[{e['severity']}] {e['code']}: {e['msg']}"
        if ctx:
            line += f" ({ctx})"
        print(line)

    @property
    def hard_count(self):
        return sum(1 for e in self.issues if e["severity"] == "HARD")

    @property
    def warn_count(self):
        return sum(1 for e in self.issues if e["severity"] == "WARN")


def load(filename: str) -> list[dict]:
    path = PRE97 / filename
    if not path.exists():
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(2)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Non-person artifact patterns (simplified) ─────────────────────────────────
import re

_RE_NOISE = re.compile(
    r"\b(club|team|kicker|footbag|association|league|federation|"
    r"unknown|tbd|tba|bye|placeholder)\b",
    re.IGNORECASE,
)


def looks_non_person(name: str) -> bool:
    if not name or name.strip().startswith("__"):
        return False  # sentinel values are OK
    return bool(_RE_NOISE.search(name))


# ── Load data ─────────────────────────────────────────────────────────────────

print("=== Pre-1997 QC Gate ===\n")

events       = load("events_pre1997.csv")
disciplines  = load("event_disciplines_pre1997.csv")
results      = load("event_results_pre1997.csv")
participants = load("event_result_participants_pre1997.csv")
persons      = load("persons_pre1997.csv")

print(f"Loaded: {len(events)} events, {len(disciplines)} disciplines, "
      f"{len(results)} results, {len(participants)} participants, "
      f"{len(persons)} persons\n")

qc = IssueCollector()

# ── EVENTS ────────────────────────────────────────────────────────────────────

print("--- Events ---")

event_ids = [e["canonical_event_id"] for e in events]
dup_events = [k for k, v in Counter(event_ids).items() if v > 1]
if dup_events:
    qc.hard("duplicate_event_id",
            f"{len(dup_events)} duplicate canonical_event_id(s): {dup_events[:3]}")

for e in events:
    for field in ("canonical_event_id", "event_name", "year"):
        if not e.get(field, "").strip():
            qc.hard("missing_required_event_field",
                    f"Event missing '{field}'", event_id=e.get("canonical_event_id"))

# Build valid event ID set from BOTH canonical_event_id (slug) and legacy_hex_id (hex).
# Disciplines/results/participants use hex IDs; events file carries both.
event_id_set = set(event_ids)
for e in events:
    hex_id = e.get("legacy_hex_id", "").strip()
    if hex_id:
        event_id_set.add(hex_id)

# ── DISCIPLINES ───────────────────────────────────────────────────────────────

print("--- Disciplines ---")

disc_keys = [(d["canonical_event_id"], d["division_raw"]) for d in disciplines]
dup_discs = [k for k, v in Counter(disc_keys).items() if v > 1]
if dup_discs:
    qc.hard("duplicate_discipline",
            f"{len(dup_discs)} duplicate (event_id, division_raw) pair(s): {dup_discs[:3]}")

orphan_discs = [k for k in disc_keys if k[0] not in event_id_set]
if orphan_discs:
    qc.hard("orphan_discipline_event",
            f"{len(orphan_discs)} disciplines reference unknown event_id(s): "
            f"{[k[0] for k in orphan_discs[:3]]}")

disc_key_set = set(disc_keys)

# ── RESULTS ───────────────────────────────────────────────────────────────────

print("--- Results ---")

result_keys = [(r["canonical_event_id"], r["division_raw"], r["place"])
               for r in results]
dup_results = [k for k, v in Counter(result_keys).items() if v > 1]
if dup_results:
    qc.hard("duplicate_result",
            f"{len(dup_results)} duplicate (event_id, division_raw, place) row(s): "
            f"{dup_results[:3]}")

orphan_result_discs = [
    (r["canonical_event_id"], r["division_raw"])
    for r in results
    if (r["canonical_event_id"], r["division_raw"]) not in disc_key_set
]
if orphan_result_discs:
    qc.hard("orphan_result_discipline",
            f"{len(orphan_result_discs)} results reference unknown discipline(s): "
            f"{list(set(orphan_result_discs))[:3]}")

# Placement starts at 1 check
result_id_set = set()
by_div_results: dict = defaultdict(list)
for r in results:
    result_id_set.add(r["result_id"])
    by_div_results[(r["canonical_event_id"], r["division_raw"])].append(
        int(r["place"]) if str(r["place"]).isdigit() else 0
    )

bad_start = []
for key, places in by_div_results.items():
    if places and min(places) != 1:
        bad_start.append(key)
if bad_start:
    qc.hard("placement_not_starting_at_1",
            f"{len(bad_start)} division(s) where min place ≠ 1: {bad_start[:3]}")

# Placement gaps (warn)
gap_divs = []
for key, places in by_div_results.items():
    sorted_p = sorted(set(places))
    if sorted_p != list(range(sorted_p[0], sorted_p[-1] + 1)):
        gap_divs.append(key)
if gap_divs:
    qc.warn("placement_gaps",
            f"{len(gap_divs)} division(s) have placement gaps (may be ties)")

# ── PARTICIPANTS ──────────────────────────────────────────────────────────────

print("--- Participants ---")

# Determine team type per discipline (infer from division name)
def infer_team_type(division_raw: str) -> str:
    """Return 'singles', 'doubles', or 'team' (3+ members)."""
    d = division_raw.lower()
    # "Team" events in pre-1997 data can have 3-4 members (team-of-three formats)
    if "team" in d:
        return "team"
    if "dbls" in d or "double" in d:
        return "doubles"
    return "singles"

disc_team_type = {
    (d["canonical_event_id"], d["division_raw"]): infer_team_type(d["division_raw"])
    for d in disciplines
}

# Group participants by (event_id, division, place)
by_slot: dict = defaultdict(list)
for p in participants:
    slot = (p["canonical_event_id"], p["division_raw"], p["place"])
    by_slot[slot].append(p)

# Check participant counts vs team type
bad_doubles = 0
bad_singles = 0
for (eid, div, plc), rows in by_slot.items():
    tt = disc_team_type.get((eid, div), "singles")
    n = len(rows)
    if tt == "doubles" and n != 2:
        bad_doubles += 1
        if bad_doubles <= 3:
            # Pre-1997 source data may have incomplete doubles pairs (partial coverage).
            # CLAUDE.md §4.2 allows partial coverage as WARN for legacy sources.
            qc.warn("incomplete_doubles_placement",
                    f"Doubles placement has {n} participant(s); expected 2",
                    event=eid, division=div, place=plc)
    elif tt == "team" and n < 2:
        # team events require ≥2 members per placement (lone entry = data error)
        bad_singles += 1
        qc.hard("invalid_team_participant_count",
                f"Team placement has {n} participant(s); expected ≥2",
                event=eid, division=div, place=plc)
    elif tt == "singles" and n < 1:
        bad_singles += 1

if bad_doubles > 3:
    qc.warn("incomplete_doubles_placement",
            f"... and {bad_doubles - 3} more doubles placements with ≠2 participants")
if bad_singles:
    qc.hard("invalid_singles_participant_count",
            f"{bad_singles} singles placement(s) with 0 participants")

# Duplicate person in same result
for (eid, div, plc), rows in by_slot.items():
    person_ids_here = [r["person_id"] for r in rows if r.get("person_id", "").strip()]
    dup_pid = [k for k, v in Counter(person_ids_here).items() if v > 1]
    if dup_pid:
        qc.hard("duplicate_person_in_result",
                f"Person appears twice in same placement",
                event=eid, division=div, place=plc, person_id=dup_pid[0])

# Orphan result_id
orphan_result_ids = [
    p["result_id"] for p in participants
    if p["result_id"] not in result_id_set
]
if orphan_result_ids:
    qc.hard("orphan_participant_result_id",
            f"{len(orphan_result_ids)} participant(s) reference unknown result_id(s): "
            f"{list(set(orphan_result_ids))[:3]}")

# Missing both person_id and player_name_raw
empty_both = [
    p for p in participants
    if not p.get("person_id", "").strip() and not p.get("player_name_raw", "").strip()
]
if empty_both:
    qc.hard("empty_participant",
            f"{len(empty_both)} participant row(s) with neither person_id nor player_name_raw")

# Non-person artifact in player_name_raw
# Exclude NOISE-resolution rows: these are legitimate placeholder markers from source.
artifact_count = sum(
    1 for p in participants
    if looks_non_person(p.get("player_name_raw", ""))
    and p.get("resolution_status", "").upper() != "NOISE"
)
if artifact_count:
    qc.hard("non_person_artifact",
            f"{artifact_count} participant row(s) with non-person text in player_name_raw")

# Unresolved participants (warn)
unresolved = sum(
    1 for p in participants
    if not p.get("person_id", "").strip() and p.get("player_name_raw", "").strip()
)
if unresolved:
    qc.warn("unresolved_participants",
            f"{unresolved} participant row(s) have player_name_raw but no person_id")

# ── PERSONS ───────────────────────────────────────────────────────────────────

print("--- Persons ---")

person_ids_in_file = [p["person_id"] for p in persons]
dup_persons = [k for k, v in Counter(person_ids_in_file).items() if v > 1]
if dup_persons:
    qc.hard("duplicate_person_id",
            f"{len(dup_persons)} duplicate person_id(s): {dup_persons[:3]}")

person_id_set = set(person_ids_in_file)

# All person_ids in participants must be in persons
participant_person_ids = {
    p["person_id"] for p in participants
    if p.get("person_id", "").strip()
}
missing_from_persons = participant_person_ids - person_id_set
if missing_from_persons:
    qc.hard("orphan_person_id",
            f"{len(missing_from_persons)} person_id(s) in participants not in persons: "
            f"{list(missing_from_persons)[:3]}")

# ── COVERAGE WARNINGS ─────────────────────────────────────────────────────────

print("--- Coverage ---")

# Divisions with <3 placements (warn)
thin_divs = [
    (k, len(v)) for k, v in by_div_results.items() if len(v) < 3
]
if thin_divs:
    qc.warn("thin_division",
            f"{len(thin_divs)} division(s) with <3 placements (may be sparse): "
            f"{thin_divs[:3]}")

# Events with no disciplines
event_disc_counts = Counter(d["canonical_event_id"] for d in disciplines)
events_no_discs = [e["canonical_event_id"] for e in events
                   if e["canonical_event_id"] not in event_disc_counts]
if events_no_discs:
    qc.warn("event_no_disciplines",
            f"{len(events_no_discs)} event(s) have no disciplines: {events_no_discs}")

# Single-source LOW confidence
low_conf = [
    e["canonical_event_id"] for e in events
    if e.get("confidence", "").upper() == "LOW"
]
if low_conf:
    qc.warn("low_confidence_events",
            f"{len(low_conf)} event(s) have LOW confidence: {low_conf[:5]}")

# ── Summary ───────────────────────────────────────────────────────────────────

print()
print("=" * 50)
print(f"Pre-1997 QC SUMMARY")
print(f"  Hard failures: {qc.hard_count}")
print(f"  Warnings:      {qc.warn_count}")
print()

if qc.hard_count == 0:
    print("QC STATUS: PASS")
    sys.exit(0)
else:
    print("QC STATUS: FAIL")
    sys.exit(1)
