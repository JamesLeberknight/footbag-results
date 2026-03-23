#!/usr/bin/env python3
"""
compute_statistics.py
Compute all statistics for the STATISTICS sheet rebuild.

Sections:
  1 - Dataset Overview
  2 - Events by Year
  3 - Discipline History
  4 - Geographic Distribution (by Country, Top Host Cities)
"""

import csv
import os
import sys
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EVENTS_NORMALIZED  = os.path.join(BASE_DIR, "out", "canonical", "events_normalized.csv")
EVENT_DISCIPLINES  = os.path.join(BASE_DIR, "out", "canonical", "event_disciplines.csv")
PLACEMENTS_FLAT    = os.path.join(BASE_DIR, "out", "Placements_Flat.csv")
PERSONS_TRUTH      = os.path.join(BASE_DIR, "out", "Persons_Truth.csv")
QUARANTINE_CSV     = os.path.join(BASE_DIR, "inputs", "review_quarantine_events.csv")


# ── Load quarantine list ──────────────────────────────────────────────────────

def load_quarantine_ids():
    ids = set()
    with open(QUARANTINE_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ids.add(str(row["event_id"]))
    return ids


# ── Load events ───────────────────────────────────────────────────────────────

def load_events(quarantine_ids):
    """Return list of non-quarantined event dicts."""
    events = []
    with open(EVENTS_NORMALIZED, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            legacy_id = str(row.get("legacy_event_id", "")).strip()
            if legacy_id in quarantine_ids:
                continue
            events.append(row)
    return events


# ── Section 1: Dataset Overview ───────────────────────────────────────────────

def compute_overview(events):
    years = []
    countries = set()
    cities = set()

    for ev in events:
        y = ev.get("year", "").strip()
        if y and y.isdigit():
            years.append(int(y))
        c = ev.get("country", "").strip()
        if c:
            countries.add(c)
        city = ev.get("city", "").strip()
        if city:
            cities.add(city)

    events_documented = len(events)
    years_covered = f"{min(years)}–{max(years)}" if years else "N/A"
    num_countries = len(countries)
    num_cities = len(cities)

    # Total placements
    total_placements = 0
    with open(PLACEMENTS_FLAT, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for _ in reader:
            total_placements += 1

    # Unique players (exclude COVERAGE_CLOSURE)
    unique_players = 0
    with open(PERSONS_TRUTH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            excl = row.get("exclusion_reason", "").strip()
            if not excl:
                unique_players += 1

    return {
        "events_documented": events_documented,
        "years_covered": years_covered,
        "countries": num_countries,
        "cities": num_cities,
        "total_placements": total_placements,
        "unique_players": unique_players,
    }


# ── Section 2: Events by Year ─────────────────────────────────────────────────

def compute_events_by_year(events):
    counts = defaultdict(int)
    for ev in events:
        y = ev.get("year", "").strip()
        if y and y.isdigit():
            counts[int(y)] += 1
    return sorted(counts.items())  # list of (year, count)


# ── Section 3: Discipline History ────────────────────────────────────────────

def compute_discipline_history(events):
    """
    For each discipline category, find first year and event count.
    Uses event_disciplines.csv joined to non-quarantined events.
    """
    # Map event_key -> year from events list
    quarantine_ids_by_key = set()  # we already filtered events list
    event_key_to_year = {}
    for ev in events:
        key = ev.get("event_key", "").strip()
        y = ev.get("year", "").strip()
        if key and y and y.isdigit():
            event_key_to_year[key] = int(y)

    # Discipline display name mapping
    DISC_DISPLAY = {
        "net":      "Net (Rallye / Side-Out)",
        "freestyle": "Freestyle / Shred / Circles",
        "golf":     "Footbag Golf",
        "sideline": "Consecutive / Sideline",
        "overall":  "Overall / Combined",
        "unknown":  "Unknown / Unclassified",
    }

    disc_events = defaultdict(set)  # category -> set of event_keys

    with open(EVENT_DISCIPLINES, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = row.get("event_key", "").strip()
            cat = row.get("discipline_category", "").strip()
            if key in event_key_to_year and cat:
                disc_events[cat].add(key)

    results = []
    for cat, event_keys in disc_events.items():
        years_for_disc = [event_key_to_year[k] for k in event_keys if k in event_key_to_year]
        first_year = min(years_for_disc) if years_for_disc else None
        display = DISC_DISPLAY.get(cat, cat.title())
        results.append((display, first_year, len(event_keys), cat))

    # Sort by first_year ascending, then by category name
    results.sort(key=lambda x: (x[1] or 9999, x[0]))

    # Return without internal cat field
    return [(display, first_year, count) for display, first_year, count, _ in results]


# ── Section 4: Geographic Distribution ───────────────────────────────────────

def compute_geography(events):
    country_counts = defaultdict(int)
    city_counts = defaultdict(int)  # key: (city, country)

    for ev in events:
        country = ev.get("country", "").strip()
        city    = ev.get("city", "").strip()

        if country:
            country_counts[country] += 1
        if city and country:
            city_counts[(city, country)] += 1

    by_country = sorted(country_counts.items(), key=lambda x: -x[1])
    by_city    = sorted(city_counts.items(), key=lambda x: -x[1])

    return by_country, by_city[:30]  # top 30 cities


# ── Main ──────────────────────────────────────────────────────────────────────

def compute_all():
    print("Loading quarantine list...")
    quarantine_ids = load_quarantine_ids()
    print(f"  {len(quarantine_ids)} quarantined events")

    print("Loading events (non-quarantined)...")
    events = load_events(quarantine_ids)
    print(f"  {len(events)} non-quarantined events")

    print("\n=== SECTION 1: DATASET OVERVIEW ===")
    overview = compute_overview(events)
    rows_overview = [
        ("Events documented",  overview["events_documented"]),
        ("Years covered",      overview["years_covered"]),
        ("Countries",          overview["countries"]),
        ("Cities",             overview["cities"]),
        ("Total placements",   overview["total_placements"]),
        ("Unique players",     overview["unique_players"]),
    ]
    for label, val in rows_overview:
        print(f"  {label:<25} {val}")

    print("\n=== SECTION 2: EVENTS BY YEAR ===")
    events_by_year = compute_events_by_year(events)
    for year, count in events_by_year:
        print(f"  {year}: {count}")

    print("\n=== SECTION 3: DISCIPLINE HISTORY ===")
    disc_history = compute_discipline_history(events)
    print(f"  {'Discipline':<35} {'First Year':>10} {'Events':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*8}")
    for display, first_year, count in disc_history:
        print(f"  {display:<35} {first_year!s:>10} {count:>8}")

    print("\n=== SECTION 4: GEOGRAPHIC DISTRIBUTION ===")
    by_country, by_city = compute_geography(events)

    print("\n  Events by Country (top 20):")
    for country, count in by_country[:20]:
        print(f"    {country:<30} {count}")

    print(f"\n  Top Host Cities (top 30):")
    for (city, country), count in by_city[:30]:
        print(f"    {city:<25} {country:<20} {count}")

    return {
        "overview":       rows_overview,
        "events_by_year": events_by_year,
        "disc_history":   disc_history,
        "by_country":     by_country,
        "by_city":        by_city,
    }


if __name__ == "__main__":
    compute_all()
