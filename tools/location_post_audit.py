#!/usr/bin/env python3
"""
location_post_audit.py — Re-run audit on normalized events.csv to show remaining issues.
"""
import csv
import os
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_CSV = os.path.join(BASE_DIR, "out", "canonical", "events_normalized.csv")
OUT_DIR = os.path.join(BASE_DIR, "out", "final_verification")
OUT_CSV = os.path.join(OUT_DIR, "location_audit_post_fix.csv")

os.makedirs(OUT_DIR, exist_ok=True)

COUNTRY_ABBREVS = {"USA", "U.S.A.", "U.S.", "US", "UK", "GB"}
US_STATE_ABBREVS = {"CA", "NY", "OR", "WA", "TX", "CO", "MA", "PA", "OH", "FL", "IL",
    "MN", "WI", "MO", "AZ", "NV", "NJ", "UT", "NC", "SC", "GA", "VA",
    "MD", "DC", "AL", "ID", "MT", "KS", "TN", "ME"}
CA_PROVINCE_ABBREVS = {"BC", "B.C.", "QC", "ON", "AB", "MB", "SK"}
AU_STATE_ABBREVS = {"NSW", "VIC", "QLD"}
ALL_ABBREVS = US_STATE_ABBREVS | CA_PROVINCE_ABBREVS | AU_STATE_ABBREVS

STREET_SUFFIXES = re.compile(r'\b(Rd|St|Ave|Blvd|Drive|Lane|Way|Road|Street|Boulevard)\b\.?', re.IGNORECASE)
DIGIT_PATTERN = re.compile(r'\d')
VENUE_PATTERNS = [
    re.compile(r'\bAyazmo\b', re.IGNORECASE),
    re.compile(r'\bRIT\b'),
    re.compile(r'^\d+\s+\w', re.IGNORECASE),
]


def flag_issues(row):
    issues = []
    city = (row.get("city") or "").strip()
    region = (row.get("region") or "").strip()
    country = (row.get("country") or "").strip()
    if country in COUNTRY_ABBREVS:
        issues.append("COUNTRY_ABBREV")
    if region in ALL_ABBREVS:
        issues.append("BAD_REGION_ABBREV")
    if city and region and city.lower() == region.lower():
        issues.append("DUPLICATE_CITY_REGION")
    if "basque" in region.lower():
        issues.append("BASQUE_COUNTRY")
    if DIGIT_PATTERN.search(city) or DIGIT_PATTERN.search(country):
        issues.append("DIGITS_IN_LOCATION")
    for field in (city, region, country):
        if STREET_SUFFIXES.search(field):
            issues.append("STREET_SUFFIX")
            break
    for pat in VENUE_PATTERNS:
        if pat.search(city):
            issues.append("VENUE_IN_CITY")
            break
    if STREET_SUFFIXES.search(country) or (DIGIT_PATTERN.search(country) and len(country) > 4):
        issues.append("STREET_IN_COUNTRY")
    combined = ", ".join(p for p in [city, region, country] if p)
    if combined.count(",") > 2:
        issues.append("TOO_MANY_SEGMENTS")
    if not country:
        issues.append("EMPTY_COUNTRY")
    return issues


def main():
    with open(EVENTS_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} events from {EVENTS_CSV}")

    flagged = []
    issue_counts = {}
    for row in rows:
        issues = flag_issues(row)
        if issues:
            flagged.append({
                "event_key": row.get("event_key", ""),
                "legacy_event_id": row.get("legacy_event_id", ""),
                "year": row.get("year", ""),
                "event_name": row.get("event_name", ""),
                "city": (row.get("city") or "").strip(),
                "region": (row.get("region") or "").strip(),
                "country": (row.get("country") or "").strip(),
                "issues": "|".join(issues),
            })
            for iss in issues:
                issue_counts[iss] = issue_counts.get(iss, 0) + 1

    if flagged:
        print(f"\nRemaining flagged issues: {len(flagged)} events")
        print("\nIssue counts:")
        for k, v in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}")
        for r in flagged:
            print(f"  [{r['year']}] {r['event_name'][:50]} | {r['city']!r}, {r['region']!r}, {r['country']!r} | {r['issues']}")
    else:
        print("\nAll location issues resolved — no remaining flags!")

    fieldnames = ["event_key", "legacy_event_id", "year", "event_name", "city", "region", "country", "issues"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(flagged)
    print(f"\nSaved {len(flagged)} flagged rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
