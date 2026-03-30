#!/usr/bin/env python3
"""
tools/build_canonical_pf.py

Converts out/release_publication/*.csv (merged PRE1997 + POST1997 canonical)
into a Placements_Flat-compatible CSV (out/canonical_pf.csv) that
build_workbook_v14.py can consume directly.

Source schema (out/release_publication/ / canonical_all format):
  events:       event_id (slug), legacy_hex_id, event_name, year, status
  disciplines:  event_id, discipline (key), discipline_name, discipline_category,
                team_type, coverage_flag, total_placements
  results:      event_id, discipline, placement
  participants: event_id, discipline, placement, participant_order,
                display_name, person_id, team_person_key

Filtering rules (strict, applied at build time):
  - Events:      status in {'completed', 'historical'}
  - Disciplines: placement count >= 3  (sparse / summary-only excluded)
  - Events:      must have >= 1 qualifying discipline after the above

Doubles handling:
  - Participants are stored as individual rows (order 1 + 2) in canonical.
  - Adapter pairs them into a single team row with:
      competitor_type   = "team"
      team_person_key   = "pid1|pid2"
      team_display_name = "Name1 / Name2"
  - If a partner slot is absent or unknown, uses "__UNKNOWN__" / empty pid.

Output columns (PF-compatible):
  event_id, year, division_canon, division_category, place,
  competitor_type, person_id, team_person_key, person_canon,
  team_display_name, coverage_flag, person_unresolved, norm, division_raw
"""

import csv
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

ROOT   = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "out" / "release_publication"
OUT_PATH = ROOT / "out" / "canonical_pf.csv"

MIN_PLACEMENTS = 3   # hard floor — divisions below this are excluded

# Status values that count as publishable
VALID_STATUSES = {"completed", "historical"}

# ── Helpers ───────────────────────────────────────────────────────────────────

_SENTINEL_NAMES = {"__UNKNOWN_PARTNER__", "__NON_PERSON__", "[UNKNOWN PARTNER]",
                   "[UNKNOWN]", "Unknown", ""}


# ── Presentability check (mirrors 04_build_analytics.py) ────────────────────

_RE_ALLOWED_CHARS = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ\u0100-\u017F\x27\u2019 .-]+$")
_RE_BAD_TOKENS = re.compile(
    r"\b(usa|canada|germany|ger|fin|cz|victory|points?|scratch|results?|open|"
    r"place|position|playoff|rank|pixie|ducking|paradox|swirl|torque|and|with|plus)\b",
    re.IGNORECASE,
)
_RE_SEPARATORS = re.compile(r"[+/\\=]")
_RE_SINGLE_INITIAL = re.compile(r"^[A-Za-z]\.$")
_RE_MULTI_INITIAL = re.compile(r"^[A-Za-z](?:\.[A-Za-z])+\.$")
_PRESENTABLE_ALLOWLIST = frozenset({"Wally Victory", "Kendall KIC", "Greg RNH", "Toxic Tom B."})


def _is_presentable(s: str) -> bool:
    t = unicodedata.normalize("NFKC", s).strip()
    if not t:
        return False
    if t in _PRESENTABLE_ALLOWLIST:
        return True
    if _RE_SEPARATORS.search(t):
        return False
    if any(ch.isdigit() for ch in t):
        return False
    if not _RE_ALLOWED_CHARS.match(t):
        return False
    if _RE_BAD_TOKENS.search(t):
        return False
    parts = t.split()
    if not (2 <= len(parts) <= 4):
        return False
    last = len(parts) - 1
    for i, p in enumerate(parts):
        if len(p) == 1:
            return False
        if _RE_SINGLE_INITIAL.match(p) and i == last:
            return False
        if _RE_MULTI_INITIAL.match(p) and i == last:
            return False
        if p.isupper() and len(p) == 3:
            return False
    return True


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()
    return " ".join(s.lower().split())


def load(name: str) -> list[dict]:
    with open(SOURCE / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Load ──────────────────────────────────────────────────────────────────────

print("build_canonical_pf.py — building Placements_Flat from release_publication")
print(f"  Source:  {SOURCE}")
print(f"  Output:  {OUT_PATH}")
print(f"  Filter:  status in {VALID_STATUSES}, placements >= {MIN_PLACEMENTS}\n")

events_raw   = load("events.csv")
discs_raw    = load("event_disciplines.csv")
results_raw  = load("event_results.csv")
parts_raw    = load("event_result_participants.csv")
persons_raw  = load("persons.csv")

# Persons lookup: person_id → canonical name
# release_publication persons.csv has "person_name" or "person_canon" column
_pname_field = "person_name" if "person_name" in persons_raw[0] else "person_canon"
persons_map = {p["person_id"]: p[_pname_field] for p in persons_raw if p.get("person_id")}

# ── Step 1: filter events ─────────────────────────────────────────────────────

# event_id is the slug in release_publication schema
valid_event_keys = {e["event_id"] for e in events_raw
                    if e.get("status", "").lower() in VALID_STATUSES}
event_meta       = {e["event_id"]: e for e in events_raw}

excluded_events = len(events_raw) - len(valid_event_keys)
print(f"Events: {len(events_raw)} total, {excluded_events} excluded (no_results/incomplete)")

# ── Step 2: count placements per discipline, apply >= 3 filter ───────────────

# release_publication uses "discipline" (not "discipline_key") as the key field
plc_counts: dict[tuple, set] = defaultdict(set)
for r in results_raw:
    key = (r["event_id"], r["discipline"])
    plc_counts[key].add(r["placement"])

# Qualifying disciplines: valid status event + >= MIN_PLACEMENTS
valid_disc_keys: set[tuple] = set()
sparse_disc = 0
for d in discs_raw:
    ek = d["event_id"]
    dk = d["discipline"]
    if ek not in valid_event_keys:
        continue
    n_plc = len(plc_counts.get((ek, dk), set()))
    if n_plc >= MIN_PLACEMENTS:
        valid_disc_keys.add((ek, dk))
    else:
        sparse_disc += 1

print(f"Disciplines: {len(discs_raw)} total, {sparse_disc} excluded (< {MIN_PLACEMENTS} placements)")

disc_meta = {(d["event_id"], d["discipline"]): d for d in discs_raw}

# ── Step 3: filter to only events that still have >= 1 valid discipline ───────

events_with_valid_disc = {ek for (ek, _) in valid_disc_keys}
dead_events = valid_event_keys - events_with_valid_disc
if dead_events:
    print(f"Events removed (all disciplines sparse): {len(dead_events)}")
    for ek in sorted(dead_events):
        print(f"  {ek}")
valid_event_keys = events_with_valid_disc

# ── Step 4: build PF rows ─────────────────────────────────────────────────────

# Group participants by (event_id, discipline, placement)
parts_by_slot: dict[tuple, list[dict]] = defaultdict(list)
for p in parts_raw:
    slot = (p["event_id"], p["discipline"], p["placement"])
    parts_by_slot[slot].append(p)
for v in parts_by_slot.values():
    v.sort(key=lambda p: int(p.get("participant_order", 1)))

pf_rows: list[dict] = []

for (ek, dk) in sorted(valid_disc_keys):
    ev   = event_meta.get(ek, {})
    disc = disc_meta.get((ek, dk), {})
    year = ev.get("year", "")
    # Use event_id slug directly as PF event_id (consistent with year sheet loader)
    pf_event_id = ek

    disc_name = disc.get("discipline_name", dk)
    cat       = disc.get("discipline_category", "unknown")
    team_type = disc.get("team_type", "singles")
    cov_flag  = disc.get("coverage_flag", "complete")

    # Get all results for this discipline, sorted by placement
    disc_results = [r for r in results_raw
                    if r["event_id"] == ek and r["discipline"] == dk]
    disc_results.sort(key=lambda r: int(r["placement"]) if r["placement"].isdigit() else 999)

    for result in disc_results:
        plc  = result["placement"]
        slot = (ek, dk, plc)
        rows = parts_by_slot.get(slot, [])

        if team_type == "doubles":
            # Build one team row per placement from the two participant rows
            p1 = rows[0] if len(rows) > 0 else {}
            p2 = rows[1] if len(rows) > 1 else {}

            def _pname(p: dict) -> str:
                n = persons_map.get(p.get("person_id", ""), "") or p.get("display_name", "")
                return "" if n in _SENTINEL_NAMES else n

            n1 = _pname(p1)
            n2 = _pname(p2)
            pid1 = p1.get("person_id", "") if n1 else ""
            pid2 = p2.get("person_id", "") if n2 else ""

            # Skip if both partners are unknown/missing
            if not n1 and not n2:
                continue

            team_display = f"{n1 or 'Unknown'} / {n2 or 'Unknown'}"
            tpk = f"{pid1}|{pid2}" if (pid1 or pid2) else ""
            person_id = pid1 or pid2

            pf_rows.append({
                "event_id":          pf_event_id,
                "year":              year,
                "division_canon":    disc_name,
                "division_category": cat,
                "place":             plc,
                "competitor_type":   "team",
                "person_id":         person_id,
                "team_person_key":   tpk,
                "person_canon":      "__NON_PERSON__",
                "team_display_name": team_display,
                "coverage_flag":     cov_flag,
                "person_unresolved": "",
                "norm":              "",
                "division_raw":      dk,
            })

        else:
            # Singles / sideline / golf: one row per participant
            for p in rows:
                pid   = p.get("person_id", "")
                dname = p.get("display_name", "")
                canon = persons_map.get(pid, "") or dname

                if canon in _SENTINEL_NAMES or not canon:
                    continue

                unresolved = "1" if not pid else ""

                pf_rows.append({
                    "event_id":          pf_event_id,
                    "year":              year,
                    "division_canon":    disc_name,
                    "division_category": cat,
                    "place":             plc,
                    "competitor_type":   "player",
                    "person_id":         pid,
                    "team_person_key":   "",
                    "person_canon":      canon,
                    "team_display_name": "",
                    "coverage_flag":     cov_flag,
                    "person_unresolved": unresolved,
                    "norm":              _norm(canon),
                    "division_raw":      dk,
                })

# ── Write canonical_pf.csv ────────────────────────────────────────────────────

FIELDNAMES = [
    "event_id", "year", "division_canon", "division_category", "place",
    "competitor_type", "person_id", "team_person_key", "person_canon",
    "team_display_name", "coverage_flag", "person_unresolved", "norm", "division_raw",
]

with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
    w.writeheader()
    w.writerows(pf_rows)

# ── Write filtered release_publication/ CSVs ──────────────────────────────────
# release_publication/ must match the workbook exactly — no sparse/excluded data.

print(f"\nFiltering release_publication/ to match workbook...")

# events: only those with >= 1 qualifying discipline
_rel_events = [e for e in events_raw if e["event_id"] in valid_event_keys]

# disciplines: only qualifying ones
_rel_discs  = [d for d in discs_raw if (d["event_id"], d["discipline"]) in valid_disc_keys]

# results: only for qualifying (event, discipline) pairs
_rel_results = [r for r in results_raw if (r["event_id"], r["discipline"]) in valid_disc_keys]

# participants: only for qualifying (event, discipline) pairs
_rel_parts  = [p for p in parts_raw  if (p["event_id"], p["discipline"]) in valid_disc_keys]

# persons: keep all real persons — exclude sentinel/unknown placeholders
# (__UNKNOWN_PARTNER__, __NON_PERSON__, unknown, blank) which are internal
# pipeline artifacts for unresolved doubles partners.
_pname_field_rel = "person_name" if "person_name" in persons_raw[0] else "person_canon"
_rel_persons = [
    p for p in persons_raw
    if _is_presentable(p.get(_pname_field_rel, ""))
]

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

# Derive fieldnames from source files
_ev_fields   = list(events_raw[0].keys())   if events_raw   else []
_di_fields   = list(discs_raw[0].keys())    if discs_raw    else []
_re_fields   = list(results_raw[0].keys())  if results_raw  else []
_pa_fields   = list(parts_raw[0].keys())    if parts_raw    else []
_pe_fields   = list(persons_raw[0].keys())  if persons_raw  else []

_write_csv(SOURCE / "events.csv",                    _rel_events,  _ev_fields)
_write_csv(SOURCE / "event_disciplines.csv",         _rel_discs,   _di_fields)
_write_csv(SOURCE / "event_results.csv",             _rel_results, _re_fields)
_write_csv(SOURCE / "event_result_participants.csv", _rel_parts,   _pa_fields)
_write_csv(SOURCE / "persons.csv",                   _rel_persons, _pe_fields)

print(f"  release_publication/ updated:")
print(f"    events.csv:                    {len(_rel_events):,}")
print(f"    event_disciplines.csv:         {len(_rel_discs):,}")
print(f"    event_results.csv:             {len(_rel_results):,}")
print(f"    event_result_participants.csv: {len(_rel_parts):,}")
print(f"    persons.csv:                   {len(_rel_persons):,}")

print(f"\nOutput: {OUT_PATH.relative_to(ROOT)}")
print(f"  Events included:       {len(valid_event_keys):,}")
print(f"  Disciplines included:  {len(valid_disc_keys):,}  (excluded {sparse_disc} sparse)")
print(f"  PF rows written:       {len(pf_rows):,}")
