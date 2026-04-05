#!/usr/bin/env python3
"""
build_canonical_enrichment.py

Final Canonicalization and Publication Prep pass.

Reads out/canonical_all/*.csv and applies:
  Part 1 — worlds_classification column on events
  Part 2 — high-confidence location fixes (1983-1987)
  Part 3 — division_canonical column on event_disciplines
  Part 4 — ruleset column (ULTRA / ADVANCED)
  Part 5 — category_canonical column
  Part 6 — filtered release_publication CSVs (SPARSE excluded)
  Part 7 — consistency checks report

Outputs:
  out/canonical_all/events.csv            (+ worlds_classification; location fixes)
  out/canonical_all/event_disciplines.csv (+ division_canonical, ruleset, category_canonical)
  out/release_publication/                (filtered: SPARSE / NO RESULTS excluded)
  out/enrichment_report.txt
"""

import csv
import re
from pathlib import Path
from collections import Counter, defaultdict

ROOT    = Path(__file__).resolve().parent.parent
CA_DIR  = ROOT / "out" / "canonical_all"
RP_DIR  = ROOT / "out" / "release_publication"
RP_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_set_from_csv(path: Path, id_col: str = "event_type") -> set[str]:
    """Load a set of string values from a single CSV column. Fails loudly if file is missing."""
    if not path.exists():
        raise FileNotFoundError(f"Required override file missing: {path}")
    result: set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            val = row[id_col].strip()
            if val:
                result.add(val)
    return result

def _load_division_canonical_map(path: Path) -> dict[str, str]:
    """Load DIVISION_CANONICAL_MAP from CSV. Fails loudly if file is missing."""
    if not path.exists():
        raise FileNotFoundError(f"Required override file missing: {path}")
    result: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = row["raw_name"].strip()
            if not key:
                continue
            result[key] = row["canonical_name"].strip()
    return result

def _load_year_location_fixes(path: Path) -> dict[str, tuple[str, str, str]]:
    """Load YEAR_LOCATION_FIXES from CSV. Fails loudly if file is missing."""
    if not path.exists():
        raise FileNotFoundError(f"Required override file missing: {path}")
    result: dict[str, tuple[str, str, str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            year = row["year"].strip()
            if not year:
                continue
            result[year] = (row["city"].strip(), row["region"].strip(), row["country"].strip())
    return result

def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    if not rows:
        path.write_text("")
        return
    fn = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fn, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — worlds_classification
# ─────────────────────────────────────────────────────────────────────────────
# Event types that represent world championship competitions
# Managed in: overrides/worlds_family.csv
WORLDS_FAMILY = _load_set_from_csv(ROOT / "overrides" / "worlds_family.csv")
# Non-worlds types (national championships, regionals, etc.) — explicitly excluded
NON_WORLDS_TYPES = {
    "WFA_NATIONALS",
    "EURO_CHAMPIONSHIPS",
    "US_NATIONALS",
    "US_REGIONALS",
    "STATE_CHAMPIONSHIPS",
    "OTHER",
}

def worlds_classification(ev: dict) -> str:
    etype = ev.get("event_type", "").strip()
    yr    = ev.get("year", "").strip()
    yr_i  = int(yr) if yr.isdigit() else 9999

    # PRE1997 worlds-family events
    if ev.get("data_source") == "PRE1997":
        if etype in WORLDS_FAMILY:
            return "RETROACTIVE_WORLD_CHAMPIONSHIPS" if yr_i <= 1983 else "OFFICIAL_WORLD_CHAMPIONSHIPS"
        return ""

    # POST1997: event_type="worlds" flag
    if etype.lower() == "worlds":
        return "OFFICIAL_WORLD_CHAMPIONSHIPS"

    return ""

# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — location fixes (year-level, high-confidence)
# ─────────────────────────────────────────────────────────────────────────────
# Year-level rules apply to ALL events in that year.
# Format: year (str) → (city, region, country)
# Based on consolidated historical sources (confirmed by subject-matter expert).
# Managed in: overrides/year_location_fixes.csv
YEAR_LOCATION_FIXES: dict[str, tuple[str, str, str]] = _load_year_location_fixes(
    ROOT / "overrides" / "year_location_fixes.csv"
)

def apply_location_fix(ev: dict) -> dict:
    fix = YEAR_LOCATION_FIXES.get(ev.get("year", ""))
    if fix:
        city, region, country = fix
        ev = dict(ev)
        ev["city"]     = city
        ev["region"]   = region
        ev["country"]  = country
        ev["location"] = f"{city}, {region}, {country}"
    return ev

# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — division_canonical mapping table
# ─────────────────────────────────────────────────────────────────────────────
# Maps discipline_name → canonical division name.
# Keys are lowercased for matching.
# Managed in: overrides/division_canonical_map.csv
DIVISION_CANONICAL_MAP: dict[str, str] = _load_division_canonical_map(
    ROOT / "overrides" / "division_canonical_map.csv"
)

def get_division_canonical(disc: dict) -> str:
    name  = disc.get("discipline_name", "").strip()
    key   = name.lower()
    if key in DIVISION_CANONICAL_MAP:
        return DIVISION_CANONICAL_MAP[key]
    # Already canonical — return as-is
    return name

# ─────────────────────────────────────────────────────────────────────────────
# PART 4 — ruleset detection
# ─────────────────────────────────────────────────────────────────────────────

def get_ruleset(disc: dict) -> str:
    name = disc.get("discipline_name", "").lower()
    if "ultra" in name:
        return "ULTRA"
    if "advanced" in name:
        return "ADVANCED"
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# PART 5 — category_canonical
# ─────────────────────────────────────────────────────────────────────────────
# Use division_canonical (already normalized) for detection.
_FREESTYLE_KW  = re.compile(
    r"\b(freestyle|routines|shred|circle|sick|battle|combo|request|ironman)\b", re.I)
_CONSECUTIVE_KW = re.compile(r"\bconsecutive\b", re.I)
_GOLF_KW        = re.compile(r"\bgolf\b", re.I)
_DISTANCE_KW    = re.compile(r"\b(distance|one.pass)\b", re.I)
_OVERALL_KW     = re.compile(r"\boverall\b", re.I)
_ACCURACY_KW    = re.compile(r"\baccuracy\b", re.I)
_SIDELINE_KW    = re.compile(r"\b(sideline|side.out|rallye)\b", re.I)

def get_category_canonical(div_canonical: str, existing_category: str) -> str:
    n = div_canonical
    if _OVERALL_KW.search(n):     return "OVERALL"
    if _DISTANCE_KW.search(n):    return "DISTANCE"
    if _CONSECUTIVE_KW.search(n): return "CONSECUTIVE"
    if _GOLF_KW.search(n):        return "GOLF"
    if _ACCURACY_KW.search(n):    return "ACCURACY"
    if _FREESTYLE_KW.search(n):   return "FREESTYLE"
    if _SIDELINE_KW.search(n):    return "NET"
    # Net: contains Singles/Doubles/Mixed/Team without freestyle keywords
    if re.search(r"\b(singles|doubles|mixed|team)\b", n, re.I):
        return "NET"
    # Fall back to existing category (uppercased) if set
    if existing_category and existing_category.strip():
        return existing_category.strip().upper()
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# PART 6 — coverage computation for SPARSE filter
# ─────────────────────────────────────────────────────────────────────────────

def compute_coverage(events, results, discs, quarantine_ids: set) -> dict[str, str]:
    plc_count  = Counter(r["event_id"] for r in results)
    disc_count = Counter(d["event_id"] for d in discs)
    cov = {}
    for ev in events:
        eid    = ev["event_id"]
        np     = plc_count.get(eid, 0)
        nd     = disc_count.get(eid, 0)
        vs     = ev.get("validation_status", "")
        status = ev.get("status", "")
        if eid in quarantine_ids:
            cov[eid] = "QUARANTINED"
        elif status == "no_results":
            cov[eid] = "NO RESULTS"
        elif vs in ("CONFIRMED_MULTI_SOURCE", "VERIFIED") and np >= 3:
            cov[eid] = "FULL"
        elif np >= 20 and nd >= 3:
            cov[eid] = "FULL"
        elif np >= 10 or nd >= 2:
            cov[eid] = "PARTIAL"
        elif np > 0:
            cov[eid] = "SPARSE"
        else:
            cov[eid] = "NO RESULTS"
    return cov

INCLUDE_COVERAGE = {"FULL", "PARTIAL", "QUARANTINED"}

# ─────────────────────────────────────────────────────────────────────────────
# PART 7 — consistency checks
# ─────────────────────────────────────────────────────────────────────────────

def run_consistency_checks(results, discs, participants) -> list[str]:
    violations = []

    # Build disc type lookup: (event_id, discipline) → team_type / category
    disc_info: dict[tuple, dict] = {}
    for d in discs:
        key = (d["event_id"], d["discipline"])
        disc_info[key] = d

    # Check 1: Doubles-format rows (participant_order=2) inside singles disciplines.
    # Shared-place ties in singles have participant_order=1 for all — not a violation.
    part_per_slot: dict[tuple, list] = defaultdict(list)
    for p in participants:
        slot = (p["event_id"], p.get("discipline", ""), p.get("placement", ""))
        part_per_slot[slot].append(p)

    for slot, parts in part_per_slot.items():
        eid, disc, plc = slot
        dinfo = disc_info.get((eid, disc), {})
        team_type = dinfo.get("team_type", "").lower()
        has_order2 = any(p.get("participant_order", "1") == "2" for p in parts)
        if team_type == "singles" and has_order2:
            violations.append(
                f"CHECK1 DOUBLES_ORDER_IN_SINGLES | {eid} | {disc} | p{plc} | {len(parts)} participants"
            )

    # Check 2: Duplicate (event, discipline, place) keys
    slots_seen: Counter = Counter()
    for p in participants:
        slots_seen[(p["event_id"], p.get("discipline",""), p.get("placement",""))] += 1
    for slot, cnt in slots_seen.items():
        if cnt > 2:  # >2 is suspicious (doubles = 2 is normal)
            eid, disc, plc = slot
            violations.append(
                f"CHECK2 DUPLICATE_SLOT | {eid} | {disc} | p{plc} | {cnt} rows"
            )

    # Check 3: Ultra/Advanced left as division (not mapped)
    for d in discs:
        name = d.get("division_canonical", d.get("discipline_name",""))
        if re.search(r"\b(ultra|advanced)\b", name, re.I):
            violations.append(
                f"CHECK3 ULTRA_ADVANCED_NOT_MAPPED | {d['event_id']} | {name}"
            )

    # Check 4: category contamination — net division categorized as freestyle or vice versa
    for d in discs:
        cat = d.get("category_canonical", "")
        div = d.get("division_canonical", d.get("discipline_name", ""))
        if cat == "FREESTYLE" and re.search(r"\bnet\b", div, re.I):
            violations.append(
                f"CHECK4 NET_MARKED_FREESTYLE | {d['event_id']} | {div}"
            )
        if cat == "NET" and _FREESTYLE_KW.search(div):
            violations.append(
                f"CHECK4 FREESTYLE_MARKED_NET | {d['event_id']} | {div}"
            )

    return violations

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

print("Loading canonical_all…")
events       = load_csv(CA_DIR / "events.csv")
discs        = load_csv(CA_DIR / "event_disciplines.csv")
results      = load_csv(CA_DIR / "event_results.csv")
participants = load_csv(CA_DIR / "event_result_participants.csv")
persons      = load_csv(CA_DIR / "persons.csv")

print(f"  events:       {len(events)}")
print(f"  disciplines:  {len(discs)}")
print(f"  results:      {len(results)}")
print(f"  participants: {len(participants)}")
print(f"  persons:      {len(persons)}")

# ── Part 1 + 2: Enrich events ────────────────────────────────────────────────
print("\nPart 1+2 — events enrichment…")
events_out = []
wc_counts: Counter = Counter()
loc_fixed = 0

for ev in events:
    ev = dict(ev)

    # Part 2: location fix (mutates in place before classification)
    fixed_ev = apply_location_fix(ev)
    if fixed_ev is not ev:
        loc_fixed += 1
        ev = fixed_ev

    # Part 1: worlds_classification
    wc = worlds_classification(ev)
    ev["worlds_classification"] = wc
    if wc:
        wc_counts[wc] += 1

    # Normalize event_type: any worlds variant → "worlds"
    _etype = ev.get("event_type", "")
    if _etype in ("WORLD_CHAMPIONSHIPS", "WFA_WORLD_CHAMPIONSHIPS", "IFAB_WORLD_CHAMPIONSHIPS"):
        ev["event_type"] = "worlds"

    events_out.append(ev)

print(f"  worlds_classification applied:")
for k, v in sorted(wc_counts.items()):
    print(f"    {k}: {v}")
print(f"  Location fixes applied: {loc_fixed}")

# ── Parts 3-5: Enrich disciplines ────────────────────────────────────────────
print("\nParts 3-5 — discipline enrichment…")
discs_out = []
ruleset_counts: Counter = Counter()
cat_changes = 0
div_mapped = 0

for d in discs:
    d = dict(d)

    # Part 3: division_canonical
    div_can = get_division_canonical(d)
    d["division_canonical"] = div_can
    if div_can != d.get("discipline_name", ""):
        div_mapped += 1

    # Part 4: ruleset
    rs = get_ruleset(d)
    d["ruleset"] = rs
    if rs:
        ruleset_counts[rs] += 1

    # Part 5: category_canonical
    existing_cat = d.get("discipline_category", "")
    cat_can = get_category_canonical(div_can, existing_cat)
    d["category_canonical"] = cat_can
    if cat_can and cat_can != existing_cat.upper():
        cat_changes += 1

    discs_out.append(d)

print(f"  division_canonical remapped: {div_mapped}")
print(f"  ruleset counts: {dict(ruleset_counts)}")
print(f"  category_canonical changes: {cat_changes}")

# ── Part 6: coverage + filtered release_publication ──────────────────────────
print("\nPart 6 — publication filter…")
cov_map = compute_coverage(events_out, results, discs_out, quarantine_ids=set())
cov_counts = Counter(cov_map.values())
print(f"  Coverage distribution: {dict(cov_counts)}")

included_eids = {eid for eid, cov in cov_map.items() if cov in INCLUDE_COVERAGE}
excluded_eids = {eid for eid, cov in cov_map.items() if cov not in INCLUDE_COVERAGE}
print(f"  Included in publication: {len(included_eids)}")
print(f"  Excluded (SPARSE/NO RESULTS): {len(excluded_eids)}")

events_pub      = [e for e in events_out      if e["event_id"] in included_eids]
discs_pub       = [d for d in discs_out       if d["event_id"] in included_eids]
results_pub     = [r for r in results         if r["event_id"] in included_eids]
participants_pub= [p for p in participants     if p["event_id"] in included_eids]

# ── Part 7: consistency checks ───────────────────────────────────────────────
print("\nPart 7 — consistency checks…")
violations = run_consistency_checks(results_pub, discs_out, participants_pub)
print(f"  Violations found: {len(violations)}")
for v in violations[:20]:
    print(f"    {v}")
if len(violations) > 20:
    print(f"    … and {len(violations)-20} more (see enrichment_report.txt)")

# ── Write canonical_all (+ new columns) ──────────────────────────────────────
print("\nWriting canonical_all…")

# events: insert worlds_classification after event_type
ev_fields_orig = list(csv.DictReader(open(CA_DIR / "events.csv")).fieldnames or [])
if "worlds_classification" not in ev_fields_orig:
    idx = ev_fields_orig.index("event_type") + 1
    ev_fields_orig.insert(idx, "worlds_classification")
write_csv(CA_DIR / "events.csv", events_out, ev_fields_orig)
print(f"  events.csv: {len(events_out)} rows")

# disciplines: append new columns
disc_fields_orig = list(csv.DictReader(open(CA_DIR / "event_disciplines.csv")).fieldnames or [])
for col in ("division_canonical", "ruleset", "category_canonical"):
    if col not in disc_fields_orig:
        disc_fields_orig.append(col)
write_csv(CA_DIR / "event_disciplines.csv", discs_out, disc_fields_orig)
print(f"  event_disciplines.csv: {len(discs_out)} rows")

# ── Write filtered release_publication ───────────────────────────────────────
print("\nWriting release_publication (filtered)…")

# events
rp_ev_fields = [f for f in ev_fields_orig
                if f not in ("worlds_classification",)]  # keep platform schema clean
write_csv(RP_DIR / "events.csv", events_pub, rp_ev_fields)
print(f"  events.csv: {len(events_pub)} rows")

rp_disc_fields = [f for f in disc_fields_orig
                  if f not in ("division_canonical","ruleset","category_canonical")]
write_csv(RP_DIR / "event_disciplines.csv", discs_pub, rp_disc_fields)
print(f"  event_disciplines.csv: {len(discs_pub)} rows")

res_fields = list(csv.DictReader(open(CA_DIR / "event_results.csv")).fieldnames or [])
write_csv(RP_DIR / "event_results.csv", results_pub, res_fields)
print(f"  event_results.csv: {len(results_pub)} rows")

part_fields = list(csv.DictReader(open(CA_DIR / "event_result_participants.csv")).fieldnames or [])
write_csv(RP_DIR / "event_result_participants.csv", participants_pub, part_fields)
print(f"  event_result_participants.csv: {len(participants_pub)} rows")

write_csv(RP_DIR / "persons.csv", persons,
          list(csv.DictReader(open(CA_DIR / "persons.csv")).fieldnames or []))
print(f"  persons.csv: {len(persons)} rows")

# ── Enrichment report ─────────────────────────────────────────────────────────
report_lines = [
    "=" * 70,
    "FOOTBAG DATASET — FINAL CANONICALIZATION ENRICHMENT REPORT",
    "=" * 70,
    "",
    "PART 1 — WORLDS CLASSIFICATION",
    "-" * 40,
]
for k, v in sorted(wc_counts.items()):
    report_lines.append(f"  {k}: {v} event(s)")

wc_events = [(e["event_id"], e["event_name"], e["year"], e["worlds_classification"])
             for e in events_out if e.get("worlds_classification")]
report_lines += ["", "  Events classified:"]
for eid, name, yr, wc in sorted(wc_events, key=lambda x: x[2]):
    report_lines.append(f"    {yr}  {wc:<40}  {name}")

report_lines += [
    "",
    "PART 2 — LOCATION FIXES (year-level)",
    "-" * 40,
    f"  Events updated: {loc_fixed}",
]
for yr, (city, region, country) in sorted(YEAR_LOCATION_FIXES.items()):
    yr_events = [e for e in events_out if e.get("year") == yr]
    report_lines.append(f"  Year {yr} → {city}, {region}, {country}  ({len(yr_events)} events)")
    for e in sorted(yr_events, key=lambda x: x["event_id"]):
        report_lines.append(f"    {e['event_id']}")

report_lines += [
    "",
    "PART 3+4 — DIVISION CANONICALIZATION + RULESET",
    "-" * 40,
    f"  Divisions remapped: {div_mapped}",
    f"  ULTRA ruleset: {ruleset_counts.get('ULTRA',0)}",
    f"  ADVANCED ruleset: {ruleset_counts.get('ADVANCED',0)}",
    "",
    "  Ultra/Advanced mappings:",
]
for d in discs_out:
    if d.get("ruleset"):
        report_lines.append(
            f"    [{d['ruleset']:<8}] {d['event_id']} | {d['discipline_name']!r} → {d['division_canonical']!r}"
        )

report_lines += [
    "",
    "PART 5 — CATEGORY CANONICAL",
    "-" * 40,
    f"  category_canonical changes: {cat_changes}",
]
cat_dist = Counter(d.get("category_canonical","") for d in discs_out)
for k, v in sorted(cat_dist.items(), key=lambda x: -x[1]):
    report_lines.append(f"  {v:5d}  {k or '(blank)'}")

report_lines += [
    "",
    "PART 6 — PUBLICATION FILTER",
    "-" * 40,
]
for k, v in sorted(cov_counts.items(), key=lambda x: -x[1]):
    flag = "  ← EXCLUDED" if k not in INCLUDE_COVERAGE else ""
    report_lines.append(f"  {v:5d}  {k}{flag}")
report_lines.append(f"  Events in release_publication: {len(events_pub)}")
report_lines.append(f"  Events excluded: {len(excluded_eids)}")
if excluded_eids:
    report_lines.append("  Excluded event list:")
    excl_detail = [(e["year"], e["event_id"], e["event_name"])
                   for e in events_out if e["event_id"] in excluded_eids]
    for yr, eid, name in sorted(excl_detail):
        cov = cov_map.get(eid,"?")
        report_lines.append(f"    {yr}  [{cov:<10}]  {eid}  {name}")

report_lines += [
    "",
    "PART 7 — CONSISTENCY CHECKS",
    "-" * 40,
    f"  Total violations: {len(violations)}",
]
if violations:
    report_lines += ["  Violations:"] + [f"    {v}" for v in violations]
else:
    report_lines.append("  PASS — no violations found")

report_lines += ["", "=" * 70, "END OF REPORT", ""]
report_text = "\n".join(report_lines)
(ROOT / "out" / "enrichment_report.txt").write_text(report_text, encoding="utf-8")

print("\nEnrichment report → out/enrichment_report.txt")
print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
