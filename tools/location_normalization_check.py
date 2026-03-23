#!/usr/bin/env python3
"""
location_normalization_check.py
Part 3: Location Normalization Check

Checks specific event IDs for location quality, scans for common issues:
- "Basque Country" in country/region fields
- Venue names in city/country fields
- Duplicated city/region strings
- Missing country field

Does NOT modify any canonical data - read-only analysis.
"""

import csv
import json
import os
import re
import sys

csv.field_size_limit(sys.maxsize)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CANONICAL_EVENTS_CSV = os.path.join(BASE_DIR, "out", "canonical", "events.csv")
STAGE2_CSV = os.path.join(BASE_DIR, "out", "stage2_canonical_events.csv")

# ── Specific events to check ─────────────────────────────────────────────────
TARGET_EVENT_IDS = [
    "1018925821", "1070400528", "984694623", "1313351184", "1038895913",
    "1623054449", "937727262", "1265745512", "1043370312", "1096695238",
    "1739036206",   # Bulgaria 2025
]

# ── Load canonical events ─────────────────────────────────────────────────────
print("Loading canonical events.csv...")
canonical_events = {}
with open(CANONICAL_EVENTS_CSV, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        eid = row.get("legacy_event_id", "") or row.get("event_key", "")
        canonical_events[eid] = dict(row)
        # Also index by event_key
        canonical_events[row.get("event_key", "")] = dict(row)

print(f"  {len(canonical_events)} entries loaded (including key aliases)")

# ── Load stage2 for cross-reference ──────────────────────────────────────────
print("Loading stage2 events...")
stage2_events = {}
with open(STAGE2_CSV, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        eid = row.get("event_id", "")
        stage2_events[eid] = {
            "event_id": eid,
            "event_name": row.get("event_name", ""),
            "year": row.get("year", ""),
            "location": row.get("location", ""),
        }

print(f"  {len(stage2_events)} events in stage2")

# ── Helper: find event by legacy_event_id ────────────────────────────────────
def find_canonical(eid):
    """Find canonical event row by legacy_event_id."""
    for key, row in canonical_events.items():
        if row.get("legacy_event_id") == eid:
            return row
    return None

# ── Part 1: Check specific events ────────────────────────────────────────────
print("\n" + "="*60)
print("SPECIFIC EVENT CHECKS")
print("="*60)

issues = []

for eid in TARGET_EVENT_IDS:
    canon = find_canonical(eid)
    stage2 = stage2_events.get(eid, {})

    name = (canon or stage2).get("event_name", "") or stage2.get("event_name", "")
    year = (canon or stage2).get("year", "") or stage2.get("year", "")

    print(f"\n[{eid}] {year} {name}")

    if canon:
        city = canon.get("city", "")
        region = canon.get("region", "")
        country = canon.get("country", "")
        print(f"  Canonical: city={city!r} region={region!r} country={country!r}")

        # Check issues
        # 1. Missing country
        if not country:
            issues.append({"event_id": eid, "issue": "MISSING_COUNTRY", "field": "country",
                          "value": "", "note": f"{year} {name}"})
            print(f"  ISSUE: Missing country")

        # 2. Duplicated city/region
        if city and region and city.lower() == region.lower():
            issues.append({"event_id": eid, "issue": "DUPLICATE_CITY_REGION",
                          "field": "city/region", "value": f"{city}/{region}",
                          "note": f"{year} {name}"})
            print(f"  ISSUE: Duplicate city/region: {city!r} = {region!r}")

        # 3. Venue name in city
        venue_patterns = [r'\bRIT\b', r'\bUniversity\b', r'\bHall\b', r'\bPark\b.*arena',
                          r'\bCenter\b', r'\bArena\b', r'\bField\b', r'\bGym\b']
        for pat in venue_patterns:
            if re.search(pat, city, re.I):
                issues.append({"event_id": eid, "issue": "VENUE_IN_CITY",
                              "field": "city", "value": city,
                              "note": f"{year} {name}"})
                print(f"  ISSUE: Possible venue in city field: {city!r}")
                break

        # 4. Basque Country check
        if "basque" in str(region).lower() or "basque" in str(country).lower():
            issues.append({"event_id": eid, "issue": "BASQUE_COUNTRY_CHECK",
                          "field": "region", "value": region,
                          "note": f"Check if province should be Biscay/Gipuzkoa for {year} {name}"})
            print(f"  NOTE: Basque Country in region: {region!r}")
    else:
        print(f"  NOT in canonical events.csv")
        s2 = stage2_events.get(eid, {})
        if s2:
            print(f"  Stage2 location: {s2.get('location','')!r}")

# ── Part 2: Full scan of all canonical events ─────────────────────────────────
print("\n" + "="*60)
print("FULL SCAN: ALL CANONICAL EVENTS")
print("="*60)

all_issues = {
    "basque_country": [],
    "venue_in_city": [],
    "duplicate_city_region": [],
    "missing_country": [],
    "other": [],
}

# Get unique events (canonical_events may have duplicate key aliases)
seen_eids = set()
event_rows = []
for key, row in canonical_events.items():
    eid = row.get("legacy_event_id", "")
    if eid and eid not in seen_eids:
        seen_eids.add(eid)
        event_rows.append(row)

print(f"Scanning {len(event_rows)} unique events...")

for row in event_rows:
    eid = row.get("legacy_event_id", "")
    year = row.get("year", "")
    name = row.get("event_name", "")
    city = row.get("city", "") or ""
    region = row.get("region", "") or ""
    country = row.get("country", "") or ""
    location_str = f"{city}, {region}, {country}"

    # 1. Basque Country check
    if "basque country" in region.lower() or "basque country" in city.lower():
        all_issues["basque_country"].append({
            "event_id": eid, "year": year, "event_name": name,
            "city": city, "region": region, "country": country,
            "note": "Region says 'Basque Country' — should specify Biscay or Gipuzkoa if province-level"
        })

    # 2. Venue name in city (common patterns)
    venue_suspects = [
        r'\bRIT\b',           # Rochester Institute of Technology
        r'\bUniversity\b',    # University campus
        r'\bAcademy\b',
        r'\bCollege\b',
        r'\bHigh School\b',
        r'\bArena\b(?!\s*$)', # Arena (not standalone)
        r'\bAyazmo\b',        # Bulgarian venue
        r'\bstadium\b',
        r'\bcomplex\b',
    ]
    for pat in venue_suspects:
        if re.search(pat, city, re.I):
            all_issues["venue_in_city"].append({
                "event_id": eid, "year": year, "event_name": name,
                "city": city, "region": region, "country": country,
                "matched_pattern": pat,
                "note": f"City field may contain venue name: {city!r}"
            })
            break

    # 3. Duplicate city/region
    if city and region and city.strip().lower() == region.strip().lower():
        all_issues["duplicate_city_region"].append({
            "event_id": eid, "year": year, "event_name": name,
            "city": city, "region": region, "country": country,
            "note": f"City and region are identical: {city!r}"
        })

    # 4. Missing country
    if not country.strip():
        all_issues["missing_country"].append({
            "event_id": eid, "year": year, "event_name": name,
            "city": city, "region": region, "country": country,
            "note": "Missing country field"
        })

# ── Report ────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("SCAN RESULTS")
print("="*60)

print(f"\nBasque Country in region/city: {len(all_issues['basque_country'])} events")
for r in all_issues['basque_country']:
    print(f"  [{r['event_id']}] {r['year']} {r['event_name'][:50]}")
    print(f"    city={r['city']!r} region={r['region']!r} country={r['country']!r}")

print(f"\nVenue name in city field: {len(all_issues['venue_in_city'])} events")
for r in all_issues['venue_in_city']:
    print(f"  [{r['event_id']}] {r['year']} {r['event_name'][:50]}")
    print(f"    city={r['city']!r} region={r['region']!r}")

print(f"\nDuplicate city/region: {len(all_issues['duplicate_city_region'])} events")
for r in all_issues['duplicate_city_region']:
    print(f"  [{r['event_id']}] {r['year']} {r['event_name'][:50]}")
    print(f"    city={r['city']!r} region={r['region']!r}")

print(f"\nMissing country: {len(all_issues['missing_country'])} events")
for r in all_issues['missing_country']:
    print(f"  [{r['event_id']}] {r['year']} {r['event_name'][:50]}")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
total = sum(len(v) for v in all_issues.values())
print(f"Total location issues found: {total}")
print(f"  Basque Country:       {len(all_issues['basque_country'])}")
print(f"  Venue in city:        {len(all_issues['venue_in_city'])}")
print(f"  Duplicate city/reg:   {len(all_issues['duplicate_city_region'])}")
print(f"  Missing country:      {len(all_issues['missing_country'])}")

# ── Note on Bulgaria 2025 ─────────────────────────────────────────────────────
print("\n" + "="*60)
print("BULGARIA 2025 LOCATION STATUS")
print("="*60)
canon_bg = find_canonical("1739036206")
if canon_bg:
    city = canon_bg.get("city", "")
    region = canon_bg.get("region", "")
    country = canon_bg.get("country", "")
    print(f"  city={city!r}, region={region!r}, country={country!r}")
    full = f"{city}, {region}, {country}" if region and region != city else f"{city}, {country}"
    print(f"  Display: {full}")
    if city == "Stara Zagora" and country == "Bulgaria":
        print("  STATUS: CORRECT — city=Stara Zagora, country=Bulgaria")
        if region == city:
            print("  NOTE: region is duplicate of city (Stara Zagora = Stara Zagora)")
            print("  RECOMMENDATION: For display, use 'Stara Zagora, Bulgaria' (omit duplicate region)")
    else:
        print("  STATUS: NEEDS FIX")
else:
    print("  Event not found in canonical events")

print("\nNOTE: No canonical data modifications made. This is a read-only analysis.")
print("NOTE: The workbook build_final_workbook_v3.py should use 'City, Country' format")
print("      (skipping region when it duplicates city) for EVENT INDEX display.")
