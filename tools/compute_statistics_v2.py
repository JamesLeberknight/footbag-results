#!/usr/bin/env python3
"""
compute_statistics_v2.py
Compute all statistics for the STATISTICS sheet of the community workbook.

Definitions:
  Events documented  = total rows in events_normalized.csv (all statuses, incl. quarantined)
  Events with results= non-quarantined events with status='completed'
  Countries          = distinct non-empty country values (excluding 'Global') from non-quarantined events
  Cities             = distinct accent-normalised (city, country) pairs from non-quarantined events
  Total placements   = len(Placements_Flat.csv) — all rows
  Canonical players  = len(Persons_Truth.csv)
  Players in results = distinct non-__NON_PERSON__ person_ids in Placements_Flat
"""

import csv
import os
import sys
import unicodedata
from collections import Counter, defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EVENTS_CSV    = os.path.join(BASE_DIR, "out", "canonical", "events_normalized.csv")
QUARANTINE_CSV= os.path.join(BASE_DIR, "inputs", "review_quarantine_events.csv")
PF_CSV        = os.path.join(BASE_DIR, "out", "Placements_Flat.csv")
PT_CSV        = os.path.join(BASE_DIR, "out", "Persons_Truth.csv")


def normalize_city(c: str) -> str:
    return unicodedata.normalize("NFD", c).encode("ascii", "ignore").decode().lower().strip()


def compute():
    # ── Load data ────────────────────────────────────────────────────────────────
    with open(QUARANTINE_CSV, encoding="utf-8") as f:
        quarantine = {str(r["event_id"]) for r in csv.DictReader(f)}

    with open(EVENTS_CSV, encoding="utf-8") as f:
        all_events = list(csv.DictReader(f))

    with open(PF_CSV, encoding="utf-8") as f:
        csv.field_size_limit(sys.maxsize)
        pf_rows = list(csv.DictReader(f))

    with open(PT_CSV, encoding="utf-8") as f:
        pt_rows = list(csv.DictReader(f))

    # ── Dataset overview ─────────────────────────────────────────────────────────
    total_events = len(all_events)
    quarantined_count = sum(
        1 for r in all_events if str(r.get("legacy_event_id", "")) in quarantine
    )

    non_q = [r for r in all_events if str(r.get("legacy_event_id", "")) not in quarantine]
    events_with_results = sum(1 for r in non_q if r.get("status", "") == "completed")

    years = sorted(set(int(r["year"]) for r in all_events if r.get("year", "").strip()))
    year_range = f"{years[0]}–{years[-1]}"

    # Countries: non-quarantined events, exclude 'Global' and empty
    real_countries = sorted(
        set(
            r.get("country", "").strip()
            for r in non_q
            if r.get("country", "").strip() and r.get("country", "").strip() != "Global"
        )
    )
    country_count = len(real_countries)

    # Cities: accent-normalised (city, country) pairs from non-quarantined events
    city_keys = set()
    for r in non_q:
        city = r.get("city", "").strip()
        country = r.get("country", "").strip()
        if city:
            key = (normalize_city(city), country)
            city_keys.add(key)
    city_count = len(city_keys)

    total_placements = len(pf_rows)
    canonical_players = len(pt_rows)

    pf_person_ids = set(
        r.get("person_id", "")
        for r in pf_rows
        if r.get("person_id", "") not in ("", "__NON_PERSON__")
    )
    players_in_results = len(pf_person_ids)

    print("=" * 60)
    print("DATASET OVERVIEW")
    print("=" * 60)
    print(f"  Events documented (total in registry):        {total_events}")
    print(f"    of which quarantined:                       {quarantined_count}")
    print(f"  Events with results (non-quarantined, completed): {events_with_results}")
    print(f"  Years covered:                                {year_range}")
    print(f"  Countries (excl. Global):                     {country_count}")
    print(f"  Cities (accent-normalised):                   {city_count}")
    print(f"  Total placements (all PF rows):               {total_placements:,}")
    print(f"  Canonical players (registry):                 {canonical_players:,}")
    print(f"  Players appearing in results:                 {players_in_results:,}")

    # ── Events by year (all events, incl. quarantined) ──────────────────────────
    year_counts = Counter(int(r["year"]) for r in all_events if r.get("year", "").strip())
    events_by_year = [(y, year_counts[y]) for y in sorted(year_counts)]

    print("\n" + "=" * 60)
    print("EVENTS BY YEAR (all events incl. quarantined)")
    print("=" * 60)
    for y, n in events_by_year:
        print(f"  {y}: {n}")
    print(f"  TOTAL: {sum(n for _, n in events_by_year)}")

    # ── Discipline history (from Placements_Flat, non-quarantined) ──────────────
    # Build event_id → year map
    eid_to_year = {
        r.get("legacy_event_id", ""): int(r["year"])
        for r in all_events
        if r.get("year", "").strip() and r.get("legacy_event_id", "")
    }

    disc_events = defaultdict(set)
    disc_first_year = defaultdict(lambda: 9999)

    for row in pf_rows:
        eid = row.get("event_id", "")
        if eid in quarantine:
            continue
        cat = row.get("division_category", "").strip()
        if not cat:
            continue
        year = eid_to_year.get(eid)
        if year:
            disc_events[cat].add(eid)
            if year < disc_first_year[cat]:
                disc_first_year[cat] = year

    DISC_LABELS = {
        "net":       "Net (Rallye / Side-Out)",
        "freestyle": "Freestyle / Shred / Circles",
        "golf":      "Footbag Golf",
        "sideline":  "Consecutive / Sideline",
        "unknown":   "Unknown / Unclassified",
    }

    disciplines = sorted(disc_first_year.items(), key=lambda x: x[1])

    print("\n" + "=" * 60)
    print("DISCIPLINE HISTORY (non-quarantined events)")
    print("=" * 60)
    for cat, first_year in disciplines:
        label = DISC_LABELS.get(cat, cat.title())
        n_events = len(disc_events[cat])
        print(f"  {label}: first={first_year}, events={n_events}")

    # ── Events by country ────────────────────────────────────────────────────────
    country_counts = Counter(
        r.get("country", "").strip()
        for r in non_q
        if r.get("country", "").strip()
    )
    global_count = country_counts.pop("Global", 0)
    ranked_countries = [(c, n) for c, n in country_counts.most_common()]

    print("\n" + "=" * 60)
    print("EVENTS BY COUNTRY (non-quarantined, excl. Global)")
    print("=" * 60)
    for c, n in ranked_countries:
        print(f"  {c}: {n}")
    if global_count:
        print(f"  Multi-country / Online: {global_count}")

    # ── Top host cities ──────────────────────────────────────────────────────────
    city_counter = Counter()
    city_canonical = defaultdict(Counter)
    for r in non_q:
        city = r.get("city", "").strip()
        country = r.get("country", "").strip()
        if city:
            norm = normalize_city(city)
            key = (norm, country)
            city_counter[key] += 1
            city_canonical[key][city] += 1

    print("\n" + "=" * 60)
    print("TOP HOST CITIES (non-quarantined)")
    print("=" * 60)
    for i, ((norm, country), count) in enumerate(city_counter.most_common(20), 1):
        canonical = city_canonical[(norm, country)].most_common(1)[0][0]
        print(f"  {i:2}. {canonical!r} ({country}): {count}")

    # Check for Montreal/Montréal merge
    print("\nCity merge check (accent variants):")
    for (norm, country), count in city_counter.items():
        spellings = city_canonical[(norm, country)]
        if len(spellings) > 1:
            print(f"  MERGED: {dict(spellings)} → norm={norm!r}")

    # ── Return structured data for use by workbook builder ──────────────────────
    return {
        "total_events":       total_events,
        "quarantined_count":  quarantined_count,
        "events_with_results": events_with_results,
        "year_range":         year_range,
        "country_count":      country_count,
        "city_count":         city_count,
        "total_placements":   total_placements,
        "canonical_players":  canonical_players,
        "players_in_results": players_in_results,
        "events_by_year":     events_by_year,
        "disciplines":        [
            (DISC_LABELS.get(cat, cat.title()), first_year, len(disc_events[cat]))
            for cat, first_year in disciplines
        ],
        "ranked_countries":   ranked_countries,
        "global_count":       global_count,
        "top_cities":         [
            (city_canonical[k].most_common(1)[0][0], k[1], count)
            for k, count in city_counter.most_common(20)
        ],
    }


if __name__ == "__main__":
    compute()
