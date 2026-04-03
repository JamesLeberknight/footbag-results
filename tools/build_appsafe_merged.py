#!/usr/bin/env python3
"""
build_appsafe_merged.py

Reads the provenance-preserving union (canonical_all_union/) and produces the
official merged canonical dataset (canonical_all/) by suppressing POST1997 legacy
events that overlap with PRE1997 reconstructed events for the same real-world event.

Inputs (provenance-preserving union):
  out/canonical_all_union/events.csv
  out/canonical_all_union/event_disciplines.csv
  out/canonical_all_union/event_results.csv
  out/canonical_all_union/event_result_participants.csv
  out/canonical_all_union/persons.csv

Outputs (official merged canonical):
  out/canonical_all_union/early_overlap_candidates.csv  — overlap analysis table
  out/canonical_all/events.csv
  out/canonical_all/event_disciplines.csv
  out/canonical_all/event_results.csv
  out/canonical_all/event_result_participants.csv
  out/canonical_all/persons.csv
  out/canonical_all/validation_summary.txt
"""

import csv
import os
import re
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO = Path(__file__).parent.parent
# Input: provenance-preserving union (may use old _all names for backward compat)
UNION_DIR = REPO / "out" / "canonical_all_union"
# Fallback: old location with _all suffix names
_OLD_IN_DIR = REPO / "out" / "canonical_all"
# Determine actual input dir
if (UNION_DIR / "events.csv").exists():
    IN_DIR = UNION_DIR
    _USE_ALL_SUFFIX = False
elif (_OLD_IN_DIR / "events_all.csv").exists():
    IN_DIR = _OLD_IN_DIR
    _USE_ALL_SUFFIX = True
else:
    IN_DIR = UNION_DIR
    _USE_ALL_SUFFIX = False

OUT_DIR = REPO / "out" / "canonical_all"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _union_path(stem: str) -> Path:
    """Resolve union file path, supporting both naming conventions."""
    if _USE_ALL_SUFFIX:
        # old naming: events_all.csv, event_disciplines_all.csv, etc.
        suffix = "_all"
        name = stem.replace("event_result_participants", "event_result_participants_all") \
                   .replace("event_results", "event_results_all") \
                   .replace("event_disciplines", "event_disciplines_all") \
                   .replace("events", "events_all") \
                   .replace("persons", "persons_all")
        return IN_DIR / f"{name}.csv"
    else:
        return IN_DIR / f"{stem}.csv"

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

WORLDS_KEYWORDS = ["world footbag championships", "world championships", "worlds"]
NHSA_KEYWORDS = ["nhsa"]
WFA_NATIONALS_KEYWORDS = ["wfa national", "wfa nationals"]
WFA_WORLD_KEYWORDS = ["wfa world"]
IFAB_WORLD_KEYWORDS = ["ifab world"]
EURO_KEYWORDS = ["european footbag championships", "european open", "euro champ"]
US_NATIONALS_KEYWORDS = ["national footbag championships", "u.s. national", "us national"]
US_REGIONALS_KEYWORDS = ["western regional", "rocky mountain open"]

# PRE1997 normalized types that represent major championships
MAJOR_CHAMPIONSHIP_TYPES = {
    "WORLD_CHAMPIONSHIPS",
    "NHSA_NATIONALS",
    "WFA_NATIONALS",
    "WFA_WORLD_CHAMPIONSHIPS",
    "IFAB_WORLD_CHAMPIONSHIPS",
    "EURO_CHAMPIONSHIPS",
    "US_NATIONALS",
    "US_REGIONALS",
}

# These PRE1997 types are all "world championship" variants — a POST1997 generic
# "World Footbag Championships" event should match any of them in the same year.
WORLDS_FAMILY_TYPES = {
    "WORLD_CHAMPIONSHIPS",
    "WFA_WORLD_CHAMPIONSHIPS",
    "IFAB_WORLD_CHAMPIONSHIPS",
}


def normalize_name(name: str) -> str:
    return name.lower().strip()


def infer_normalized_type_from_post(event: dict) -> str | None:
    """
    Given a POST1997 early event, infer what normalized championship type it represents.
    Returns a string matching PRE1997 event_type values, or None if no match.

    Returns "WORLDS_FAMILY" for generic world championship events — callers must
    check against all WORLDS_FAMILY_TYPES when matching against PRE1997.
    """
    name = normalize_name(event.get("event_name", ""))
    etype = event.get("event_type", "").lower()

    if any(k in name for k in NHSA_KEYWORDS):
        return "NHSA_NATIONALS"
    if any(k in name for k in IFAB_WORLD_KEYWORDS):
        return "WORLDS_FAMILY"   # IFAB is a worlds-family member
    if any(k in name for k in WFA_WORLD_KEYWORDS):
        return "WORLDS_FAMILY"   # WFA Worlds is a worlds-family member
    if any(k in name for k in WFA_NATIONALS_KEYWORDS):
        return "WFA_NATIONALS"
    if any(k in name for k in EURO_KEYWORDS):
        return "EURO_CHAMPIONSHIPS"
    if any(k in name for k in US_NATIONALS_KEYWORDS):
        return "US_NATIONALS"
    if any(k in name for k in US_REGIONALS_KEYWORDS):
        return "US_REGIONALS"
    # Generic "world" events — treat as worlds family
    if any(k in name for k in WORLDS_KEYWORDS):
        return "WORLDS_FAMILY"
    # Also catch by event_type flag
    if etype == "worlds":
        return "WORLDS_FAMILY"

    return None


# ---------------------------------------------------------------------------
# Load all tables
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None):
    if not rows and fieldnames is None:
        path.write_text("")
        return
    fn = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fn, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


print(f"Loading union tables from {IN_DIR}...")
events_all = load_csv(_union_path("events"))
disciplines_all = load_csv(_union_path("event_disciplines"))
results_all = load_csv(_union_path("event_results"))
participants_all = load_csv(_union_path("event_result_participants"))
persons_all = load_csv(_union_path("persons"))

print(f"  events:       {len(events_all)}")
print(f"  disciplines:  {len(disciplines_all)}")
print(f"  results:      {len(results_all)}")
print(f"  participants: {len(participants_all)}")
print(f"  persons:      {len(persons_all)}")

# ---------------------------------------------------------------------------
# PART 1 — Identify early overlaps
# ---------------------------------------------------------------------------

print("\n--- PART 1: Identifying early overlaps ---")

# Partition events
pre_events = [e for e in events_all if e["data_source"] == "PRE1997"]
post_events = [e for e in events_all if e["data_source"] == "POST1997"]
post_early  = [e for e in post_events if e.get("year") and int(e["year"]) < 1997]
post_modern = [e for e in post_events if not (e.get("year") and int(e["year"]) < 1997)]

# Build PRE1997 lookup: (year, normalized_type) → list[event]
pre_by_year_type: dict[tuple, list] = defaultdict(list)
for e in pre_events:
    y = int(e["year"]) if e.get("year") else None
    t = e.get("event_type", "")
    if y and t:
        pre_by_year_type[(y, t)].append(e)

# For each POST1997 early event, attempt to find a PRE1997 match
# ---------------------------------------------------------------------------
# Manual overrides: POST1997 events whose data is richer than the PRE1997
# reconstruction.  These are force-kept even when a PRE1997 match exists.
# The corresponding PRE1997 event_id(s) listed in PRE1997_SUPPRESS will be
# excluded from the app-safe output to avoid duplicates.
# ---------------------------------------------------------------------------
POST1997_FORCE_KEEP: set[str] = {
    # prokicker.com source: 22 disciplines, 103 participants — supersedes the
    # 29-placement FBW magazine reconstruction in PRE1997
    "1996_worlds_montreal",
}

PRE1997_SUPPRESS: set[str] = {
    # Superseded by 1996_worlds_montreal (POST1997, prokicker.com)
    "1996_worlds",
}

overlap_candidates = []
post_event_flags: dict[str, str] = {}  # event_id → "SUPPRESS" | "KEEP"

for post_ev in post_early:
    year = int(post_ev["year"])
    inferred_type = infer_normalized_type_from_post(post_ev)

    # Find matching PRE1997 events
    # For WORLDS_FAMILY, check all worlds-family PRE1997 types in this year
    if inferred_type == "WORLDS_FAMILY":
        pre_matches = []
        for wtype in WORLDS_FAMILY_TYPES:
            pre_matches.extend(pre_by_year_type.get((year, wtype), []))
        matched_type = "WORLDS_FAMILY"
    elif inferred_type:
        pre_matches = pre_by_year_type.get((year, inferred_type), [])
        matched_type = inferred_type
    else:
        pre_matches = []
        matched_type = None

    if pre_matches:
        for pre_ev in pre_matches:
            overlap_candidates.append({
                "year": year,
                "normalized_event_type": matched_type,
                "pre1997_event_key": pre_ev["event_id"],
                "pre1997_event_name": pre_ev["event_name"],
                "post1997_event_key": post_ev["event_id"],
                "post1997_event_name": post_ev["event_name"],
                "overlap_reason": (
                    f"POST1997 name implies {matched_type}; "
                    f"PRE1997 has {pre_ev['event_type']} for year {year}"
                ),
                "resolution_action": "SUPPRESS_POST1997_FROM_APP_VIEW",
            })
        if post_ev["event_id"] in POST1997_FORCE_KEEP:
            post_event_flags[post_ev["event_id"]] = "KEEP"
        else:
            post_event_flags[post_ev["event_id"]] = "SUPPRESS"
    else:
        # No PRE1997 equivalent — keep it
        overlap_desc = inferred_type if inferred_type else "NO_CHAMPIONSHIP_TYPE_INFERRED"
        overlap_candidates.append({
            "year": year,
            "normalized_event_type": overlap_desc,
            "pre1997_event_key": "",
            "pre1997_event_name": "",
            "post1997_event_key": post_ev["event_id"],
            "post1997_event_name": post_ev["event_name"],
            "overlap_reason": (
                "POST1997 early event with no PRE1997 equivalent"
            ),
            "resolution_action": "KEEP_IN_APP_VIEW",
        })
        post_event_flags[post_ev["event_id"]] = "KEEP"

# Write overlap candidates to union dir (it's a union-layer artifact)
UNION_DIR.mkdir(parents=True, exist_ok=True)
overlap_path = UNION_DIR / "early_overlap_candidates.csv"
overlap_fields = [
    "year", "normalized_event_type",
    "pre1997_event_key", "pre1997_event_name",
    "post1997_event_key", "post1997_event_name",
    "overlap_reason", "resolution_action",
]
write_csv(overlap_path, overlap_candidates, overlap_fields)
print(f"  Wrote {len(overlap_candidates)} overlap candidate rows → {overlap_path}")

suppressed_ids = {eid for eid, flag in post_event_flags.items() if flag == "SUPPRESS"}
kept_early_ids = {eid for eid, flag in post_event_flags.items() if flag == "KEEP"}

print(f"  POST1997 early events suppressed: {len(suppressed_ids)}")
print(f"  POST1997 early events kept (no PRE1997 equivalent): {len(kept_early_ids)}")

# Print suppressed events for review
print("\n  Suppressed POST1997 events:")
for post_ev in sorted(post_early, key=lambda e: e["year"]):
    if post_ev["event_id"] in suppressed_ids:
        print(f"    {post_ev['year']} | {post_ev['event_id']} | {post_ev['event_name']}")

print("\n  Kept POST1997 early events (no PRE1997 match):")
for post_ev in sorted(post_early, key=lambda e: e["year"]):
    if post_ev["event_id"] in kept_early_ids:
        print(f"    {post_ev['year']} | {post_ev['event_id']} | {post_ev['event_name']}")

# ---------------------------------------------------------------------------
# PART 2 — Define the exclude_from_app_view flag
# ---------------------------------------------------------------------------

print("\n--- PART 2: Applying exclude_from_app_view flag ---")

# The set of event_ids to exclude from the app view
EXCLUDED_EVENT_IDS = suppressed_ids

# ---------------------------------------------------------------------------
# PART 3 — Build app-safe merged outputs
# ---------------------------------------------------------------------------

print("\n--- PART 3: Building app-safe outputs ---")

# App-safe events: all PRE1997 + POST1997 modern + POST1997 early without a PRE1997 match
# IMPORTANT: suppress only POST1997 events matching suppressed_ids — a PRE1997 event
# may share the same slug as a suppressed POST1997 event (same real-world event, two
# data sources). The PRE1997 record must always be kept.
appsafe_event_ids = set()
appsafe_events = []
for e in events_all:
    eid = e["event_id"]
    if eid in EXCLUDED_EVENT_IDS and e.get("data_source") == "POST1997":
        continue  # suppress POST1997 overlap only
    if eid in PRE1997_SUPPRESS:
        continue  # PRE1997 event superseded by a richer POST1997 reconstruction
    appsafe_event_ids.add(eid)
    appsafe_events.append(e)

# App-safe disciplines: keep only rows for included events
appsafe_disciplines = [r for r in disciplines_all if r["event_id"] in appsafe_event_ids]

# App-safe results
appsafe_results = [r for r in results_all if r["event_id"] in appsafe_event_ids]

# App-safe participants
appsafe_participants = [r for r in participants_all if r["event_id"] in appsafe_event_ids]

# App-safe persons: keep persons still referenced
referenced_person_ids = set()
for p in appsafe_participants:
    pid = p.get("person_id", "").strip()
    if pid:
        referenced_person_ids.add(pid)
# Also keep persons not referenced by participants but present (PRE1997_ONLY, etc.)
# — keep all persons that were already in the merged set unless they're ONLY
#   referenced by suppressed events. We'll keep all persons for safety.
appsafe_persons = persons_all  # persons table doesn't change with event filtering

# Write official merged outputs with clean names
write_csv(OUT_DIR / "events.csv", appsafe_events)
write_csv(OUT_DIR / "event_disciplines.csv", appsafe_disciplines)
write_csv(OUT_DIR / "event_results.csv", appsafe_results)
write_csv(OUT_DIR / "event_result_participants.csv", appsafe_participants)
write_csv(OUT_DIR / "persons.csv", appsafe_persons)

print(f"  events.csv:                    {len(appsafe_events)}")
print(f"  event_disciplines.csv:         {len(appsafe_disciplines)}")
print(f"  event_results.csv:             {len(appsafe_results)}")
print(f"  event_result_participants.csv: {len(appsafe_participants)}")
print(f"  persons.csv:                   {len(appsafe_persons)}")

# ---------------------------------------------------------------------------
# PART 4 — Validation
# ---------------------------------------------------------------------------

print("\n--- PART 4: Validation ---")

errors = []

# 1. Check for duplicate "worlds" events per year in app-safe.
#    Flag ONLY when multiple events of the SAME normalized type exist (real duplicates).
#    Multiple PRE1997 types per year (e.g. NHSA_NATIONALS + WORLD_CHAMPIONSHIPS in 1980)
#    are historically correct — different organizations ran separate championships.
worlds_types = {
    "WORLD_CHAMPIONSHIPS", "WFA_WORLD_CHAMPIONSHIPS",
    "IFAB_WORLD_CHAMPIONSHIPS", "NHSA_NATIONALS", "WFA_NATIONALS",
}
worlds_by_year: dict[int, list] = defaultdict(list)
for e in appsafe_events:
    year = int(e["year"]) if e.get("year") else None
    etype = e.get("event_type", "")
    if year and year < 1997 and etype in worlds_types:
        worlds_by_year[year].append(e)

# Detect true duplicates: same (year, event_type) with more than one entry
# Exception: two PRE1997 events of different org types in same year is expected.
duplicate_worlds_years = []
print("\n  Early championship events per year (app-safe):")
for year in sorted(worlds_by_year):
    evs = worlds_by_year[year]
    # Group by event_type within this year
    by_type: dict[str, list] = defaultdict(list)
    for ev in evs:
        by_type[ev["event_type"]].append(ev)
    # True duplicate = same type appears more than once
    true_dups = {t: es for t, es in by_type.items() if len(es) > 1}
    flag = " *** TRUE DUPLICATE" if true_dups else ""
    print(f"    {year}: {len(evs)} event(s){flag}")
    for ev in evs:
        print(f"      {ev['event_type']} | {ev['data_source']} | {ev['event_name']}")
    if true_dups:
        duplicate_worlds_years.append(year)

# 2. Referential integrity checks
appsafe_event_ids_check = {e["event_id"] for e in appsafe_events}

orphan_disciplines = [r for r in appsafe_disciplines if r["event_id"] not in appsafe_event_ids_check]
orphan_results     = [r for r in appsafe_results if r["event_id"] not in appsafe_event_ids_check]
orphan_participants = [r for r in appsafe_participants if r["event_id"] not in appsafe_event_ids_check]

person_ids_in_table = {p["person_id"] for p in appsafe_persons}
orphan_person_refs = [
    r for r in appsafe_participants
    if r.get("person_id", "").strip()
    and r["person_id"] not in person_ids_in_table
]

if orphan_disciplines:
    errors.append(f"Orphan discipline rows: {len(orphan_disciplines)}")
if orphan_results:
    errors.append(f"Orphan result rows: {len(orphan_results)}")
if orphan_participants:
    errors.append(f"Orphan participant rows: {len(orphan_participants)}")
if orphan_person_refs:
    errors.append(f"Orphan person_id references in participants: {len(orphan_person_refs)}")

print(f"\n  Referential integrity:")
print(f"    Orphan discipline rows:   {len(orphan_disciplines)}")
print(f"    Orphan result rows:       {len(orphan_results)}")
print(f"    Orphan participant rows:  {len(orphan_participants)}")
print(f"    Orphan person refs:       {len(orphan_person_refs)}")

# 3. Modern coverage unchanged
modern_orig = [e for e in events_all if e["data_source"] == "POST1997"
               and e.get("year") and int(e["year"]) >= 1997]
modern_safe = [e for e in appsafe_events if e["data_source"] == "POST1997"
               and e.get("year") and int(e["year"]) >= 1997]
if len(modern_orig) != len(modern_safe):
    errors.append(
        f"Modern coverage changed: {len(modern_orig)} → {len(modern_safe)}"
    )
print(f"\n  Modern (>=1997) POST1997 events: {len(modern_orig)} → {len(modern_safe)} (unchanged: {len(modern_orig) == len(modern_safe)})")

# 4. Count by data_source
from collections import Counter
ds_orig = Counter(e["data_source"] for e in events_all)
ds_safe = Counter(e["data_source"] for e in appsafe_events)
print(f"\n  Event counts by data_source:")
print(f"    {'Source':<15} {'Original':>10} {'App-safe':>10} {'Delta':>8}")
for src in sorted(set(list(ds_orig.keys()) + list(ds_safe.keys()))):
    orig_n = ds_orig.get(src, 0)
    safe_n = ds_safe.get(src, 0)
    delta = safe_n - orig_n
    print(f"    {src:<15} {orig_n:>10} {safe_n:>10} {delta:>+8}")

# ---------------------------------------------------------------------------
# Write validation summary
# ---------------------------------------------------------------------------

summary_lines = [
    "=" * 60,
    "APP-SAFE MERGED DATASET — VALIDATION SUMMARY",
    "=" * 60,
    "",
    "FILTER RULE",
    "-----------",
    "For year < 1997:",
    "  - POST1997 events whose name implies a known championship type",
    "    (WORLD_CHAMPIONSHIPS, NHSA_NATIONALS, WFA_WORLD_CHAMPIONSHIPS,",
    "     IFAB_WORLD_CHAMPIONSHIPS, WFA_NATIONALS, EURO_CHAMPIONSHIPS,",
    "     US_NATIONALS, US_REGIONALS) are SUPPRESSED when a PRE1997",
    "     reconstructed event exists for the same (year, type) pair.",
    "  - POST1997 events with no PRE1997 equivalent are KEPT.",
    "For year >= 1997: all POST1997 events are kept unchanged.",
    "",
    "EVENT COUNTS",
    "------------",
    f"  Original merged events:           {len(events_all)}",
    f"  App-safe events:                  {len(appsafe_events)}",
    f"  POST1997 early events suppressed: {len(suppressed_ids)}",
    f"  POST1997 early events kept:       {len(kept_early_ids)}",
    f"  PRE1997 events:                   {len(pre_events)}",
    f"  POST1997 modern (>=1997):         {len(modern_orig)}",
    "",
    "SUPPRESSED POST1997 EVENTS",
    "--------------------------",
]
for post_ev in sorted(post_early, key=lambda e: (e["year"], e["event_id"])):
    if post_ev["event_id"] in suppressed_ids:
        summary_lines.append(
            f"  {post_ev['year']} | {post_ev['event_id']:<35} | {post_ev['event_name']}"
        )

summary_lines += [
    "",
    "KEPT POST1997 EARLY EVENTS (no PRE1997 equivalent)",
    "---------------------------------------------------",
]
for post_ev in sorted(post_early, key=lambda e: (e["year"], e["event_id"])):
    if post_ev["event_id"] in kept_early_ids:
        summary_lines.append(
            f"  {post_ev['year']} | {post_ev['event_id']:<35} | {post_ev['event_name']}"
        )

summary_lines += [
    "",
    "REFERENTIAL INTEGRITY",
    "---------------------",
    f"  Orphan discipline rows:   {len(orphan_disciplines)}",
    f"  Orphan result rows:       {len(orphan_results)}",
    f"  Orphan participant rows:  {len(orphan_participants)}",
    f"  Orphan person refs:       {len(orphan_person_refs)}",
    "",
    "EARLY CHAMPIONSHIP OVERLAP CHECK (app-safe)",
    "--------------------------------------------",
    "  (Multiple entries per year indicate distinct organizations, not duplicates)",
]
for year in sorted(worlds_by_year):
    evs = worlds_by_year[year]
    for ev in evs:
        summary_lines.append(
            f"  {year} | {ev['event_type']:<30} | {ev['data_source']} | {ev['event_name']}"
        )

if duplicate_worlds_years:
    summary_lines += [
        "",
        "TRUE DUPLICATE CASES (same year + same type — need manual review)",
        "------------------------------------------------------------------",
    ]
    for year in duplicate_worlds_years:
        evs = worlds_by_year[year]
        for ev in evs:
            summary_lines.append(
                f"  {year} | {ev['event_type']} | {ev['data_source']} | {ev['event_id']} | {ev['event_name']}"
            )
else:
    summary_lines += [
        "",
        "No true duplicate championship overlaps remain in the app-safe view.",
        "(Multiple PRE1997 entries per year reflect distinct historical organizations.)",
    ]

summary_lines += [
    "",
    "VALIDATION STATUS",
    "-----------------",
]
if errors:
    summary_lines.append("  FAIL — errors found:")
    for e in errors:
        summary_lines.append(f"    - {e}")
else:
    summary_lines.append("  PASS — no integrity errors")

summary_lines += [
    "",
    "OUTPUT FILES (official merged canonical)",
    "----------------------------------------",
    f"  {OUT_DIR / 'events.csv'}",
    f"  {OUT_DIR / 'event_disciplines.csv'}",
    f"  {OUT_DIR / 'event_results.csv'}",
    f"  {OUT_DIR / 'event_result_participants.csv'}",
    f"  {OUT_DIR / 'persons.csv'}",
    "",
    "UNION (reference only)",
    "----------------------",
    f"  {UNION_DIR / 'early_overlap_candidates.csv'}",
    "",
]

summary_text = "\n".join(summary_lines)
summary_path = OUT_DIR / "validation_summary.txt"
summary_path.write_text(summary_text, encoding="utf-8")
print(f"\n  Wrote validation summary → {summary_path}")

# Note: README for canonical_all/ is maintained separately (not auto-generated)
# to preserve hand-written documentation.

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
print(summary_text)
