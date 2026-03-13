#!/usr/bin/env python3
"""
location_audit.py — Audit event location fields for normalization issues.
Reads out/canonical/events.csv, flags problems, saves report.
"""
import csv
import os
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_CSV = os.path.join(BASE_DIR, "out", "canonical", "events.csv")
OUT_DIR = os.path.join(BASE_DIR, "out", "final_verification")
OUT_CSV = os.path.join(OUT_DIR, "location_audit_pre_fix.csv")

os.makedirs(OUT_DIR, exist_ok=True)

# Abbreviation sets
US_STATE_ABBREVS = {
    "CA", "NY", "OR", "WA", "TX", "CO", "MA", "PA", "OH", "FL", "IL",
    "MN", "WI", "MO", "AZ", "NV", "NJ", "UT", "NC", "SC", "GA", "VA",
    "MD", "DC", "AL", "ID", "MT", "KS", "TN", "ME",
}
CA_PROVINCE_ABBREVS = {"BC", "B.C.", "QC", "ON", "AB", "MB", "SK"}
AU_STATE_ABBREVS = {"NSW", "VIC", "QLD"}
ALL_ABBREVS = US_STATE_ABBREVS | CA_PROVINCE_ABBREVS | AU_STATE_ABBREVS

COUNTRY_ABBREVS = {"USA", "U.S.A.", "U.S.", "US", "UK", "GB"}

STREET_SUFFIXES = re.compile(r'\b(Rd|St|Ave|Blvd|Drive|Lane|Way|Road|Street|Boulevard)\b\.?', re.IGNORECASE)
DIGIT_PATTERN = re.compile(r'\d')

VENUE_PATTERNS = [
    re.compile(r'\bAyazmo\b', re.IGNORECASE),
    re.compile(r'\bRIT\b'),
    re.compile(r'^\d+\s+\w', re.IGNORECASE),  # starts with street number
]


def load_events():
    with open(EVENTS_CSV, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def flag_issues(row):
    issues = []
    city = (row.get("city") or "").strip()
    region = (row.get("region") or "").strip()
    country = (row.get("country") or "").strip()

    # COUNTRY_ABBREV
    if country in COUNTRY_ABBREVS:
        issues.append("COUNTRY_ABBREV")

    # BAD_REGION_ABBREV
    if region in ALL_ABBREVS:
        issues.append("BAD_REGION_ABBREV")

    # DUPLICATE_CITY_REGION (city == region, not in allowed-double-up countries)
    if city and region and city.lower() == region.lower():
        issues.append("DUPLICATE_CITY_REGION")

    # BASQUE_COUNTRY
    if "basque" in region.lower():
        issues.append("BASQUE_COUNTRY")

    # DIGITS_IN_LOCATION
    if DIGIT_PATTERN.search(city) or DIGIT_PATTERN.search(country):
        issues.append("DIGITS_IN_LOCATION")

    # STREET_SUFFIX
    for field in (city, region, country):
        if STREET_SUFFIXES.search(field):
            issues.append("STREET_SUFFIX")
            break

    # VENUE_IN_CITY
    for pat in VENUE_PATTERNS:
        if pat.search(city):
            issues.append("VENUE_IN_CITY")
            break

    # STREET_IN_COUNTRY
    if STREET_SUFFIXES.search(country) or (DIGIT_PATTERN.search(country) and len(country) > 4):
        issues.append("STREET_IN_COUNTRY")

    # TOO_MANY_SEGMENTS (combined location string)
    combined = ", ".join(p for p in [city, region, country] if p)
    if combined.count(",") > 2:
        issues.append("TOO_MANY_SEGMENTS")

    # EMPTY_COUNTRY
    if not country:
        issues.append("EMPTY_COUNTRY")

    # DUPLICATE_QUEBEC (Québec vs Quebec inconsistency - flag both spellings present)
    if region in ("Québec", "Quebec"):
        issues.append("QUEBEC_ACCENT_CHECK")

    return issues


def main():
    rows = load_events()
    print(f"Loaded {len(rows)} events from {EVENTS_CSV}")

    flagged = []
    issue_counts = {}

    for row in rows:
        issues = flag_issues(row)
        if issues:
            city = (row.get("city") or "").strip()
            region = (row.get("region") or "").strip()
            country = (row.get("country") or "").strip()
            flagged.append({
                "event_key": row.get("event_key", ""),
                "legacy_event_id": row.get("legacy_event_id", ""),
                "year": row.get("year", ""),
                "event_name": row.get("event_name", ""),
                "city": city,
                "region": region,
                "country": country,
                "issues": "|".join(issues),
            })
            for iss in issues:
                issue_counts[iss] = issue_counts.get(iss, 0) + 1

    print(f"\nFlagged {len(flagged)} events with issues")
    print("\nIssue counts:")
    for k, v in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    # Print details of each issue type
    print("\n=== COUNTRY_ABBREV details ===")
    for r in flagged:
        if "COUNTRY_ABBREV" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | country={r['country']!r}")

    print("\n=== BAD_REGION_ABBREV details ===")
    for r in flagged:
        if "BAD_REGION_ABBREV" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | region={r['region']!r} country={r['country']!r}")

    print("\n=== DUPLICATE_CITY_REGION details ===")
    for r in flagged:
        if "DUPLICATE_CITY_REGION" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | city={r['city']!r} region={r['region']!r} country={r['country']!r}")

    print("\n=== BASQUE_COUNTRY details ===")
    for r in flagged:
        if "BASQUE_COUNTRY" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | city={r['city']!r} region={r['region']!r}")

    print("\n=== DIGITS_IN_LOCATION / STREET details ===")
    for r in flagged:
        if "DIGITS_IN_LOCATION" in r["issues"] or "STREET_SUFFIX" in r["issues"] or "STREET_IN_COUNTRY" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | city={r['city']!r} country={r['country']!r}")

    print("\n=== EMPTY_COUNTRY details ===")
    for r in flagged:
        if "EMPTY_COUNTRY" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | city={r['city']!r} region={r['region']!r}")

    print("\n=== VENUE_IN_CITY details ===")
    for r in flagged:
        if "VENUE_IN_CITY" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | city={r['city']!r}")

    print("\n=== QUEBEC_ACCENT_CHECK details ===")
    for r in flagged:
        if "QUEBEC_ACCENT_CHECK" in r["issues"]:
            print(f"  [{r['year']}] {r['event_name'][:50]} | region={r['region']!r} city={r['city']!r}")

    # Save
    fieldnames = ["event_key", "legacy_event_id", "year", "event_name", "city", "region", "country", "issues"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(flagged)
    print(f"\nSaved {len(flagged)} flagged rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
