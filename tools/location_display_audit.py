#!/usr/bin/env python3
"""
location_display_audit.py
Audits location display for all events using the two-column display rules.

Display Rules:
  US/Canada: left = "City, State/Province", right = full country name
  All others: left = City only, right = full country name

Saves: out/final_verification/location_display_audit.csv
"""

import csv
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CANONICAL_EVENTS_CSV = os.path.join(BASE_DIR, "out", "canonical", "events_normalized.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "out", "final_verification", "location_display_audit.csv")

# Known bad abbreviations in right column
ABBREV_COUNTRIES = {"USA", "US", "U.S.", "U.S.A.", "UK", "U.K.", "CAN"}

# Street address indicators
STREET_INDICATORS = ["Rd.", "St.", "Ave.", "Blvd", "adjacent", "UCSF", "RIT "]


def build_location_pair(eid, canonical_locations):
    """
    Returns (left_display, right_display) for the two-column EVENT INDEX.

    US/Canada: left = "City, State/Province", right = full country name
    All others: left = City only, right = full country name
    """
    if eid not in canonical_locations:
        return ("", "")

    city, region, country = canonical_locations[eid]

    if country in ("United States", "Canada"):
        if city and region:
            left = f"{city}, {region}"
        elif city:
            left = city
        else:
            left = ""
        right = country
    else:
        left = city if city else ""
        right = country

    return (left, right)


def build_year_sheet_location(eid, canonical_locations):
    """
    Returns a single location string for year sheet display.

    US/Canada: "City, State/Province, Country"
    All others: "City, Country"
    """
    if eid not in canonical_locations:
        return None

    city, region, country = canonical_locations[eid]

    if country in ("United States", "Canada"):
        if city and region:
            return f"{city}, {region}, {country}"
        elif city:
            return f"{city}, {country}"
        else:
            return country
    else:
        if city:
            return f"{city}, {country}"
        else:
            return country


def validate_display(left, right, country=None):
    """
    Returns (is_valid, reason) for a display pair.
    """
    if right in ABBREV_COUNTRIES:
        return (False, f"ABBREV_COUNTRY: right={right!r}")

    if left:
        for indicator in STREET_INDICATORS:
            if indicator in left:
                return (False, f"STREET_ADDR: left contains {indicator!r}")

        # Check for digits in left column
        if any(c.isdigit() for c in left):
            return (False, f"DIGITS_IN_LEFT: left={left!r}")

        # Two commas in left = too many parts (allowed: one comma for US/CA)
        comma_count = left.count(",")
        if comma_count > 1:
            return (False, f"TOO_MANY_COMMAS: {comma_count} commas in left={left!r}")

    return (True, "OK")


def main():
    # Load canonical locations
    canonical_locations = {}
    with open(CANONICAL_EVENTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = row.get("legacy_event_id", "")
            if eid:
                city = row.get("city", "") or ""
                region = row.get("region", "") or ""
                country = row.get("country", "") or ""
                canonical_locations[eid] = (city, region, country)

    print(f"Loaded {len(canonical_locations)} canonical locations")

    # Audit each event
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    issues = []
    all_rows = []

    for eid, (city, region, country) in canonical_locations.items():
        left, right = build_location_pair(eid, canonical_locations)
        valid, reason = validate_display(left, right, country)
        year_loc = build_year_sheet_location(eid, canonical_locations)

        row = {
            "event_id": eid,
            "city": city,
            "region": region,
            "country": country,
            "left_display": left,
            "right_display": right,
            "year_sheet_location": year_loc,
            "valid": "Y" if valid else "N",
            "issue": reason if not valid else "",
        }
        all_rows.append(row)
        if not valid:
            issues.append(row)

    # Write output
    fieldnames = ["event_id", "city", "region", "country", "left_display", "right_display",
                  "year_sheet_location", "valid", "issue"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nAudit complete: {len(all_rows)} events")
    print(f"Issues found: {len(issues)}")

    if issues:
        print("\nISSUES:")
        for row in issues:
            print(f"  [{row['event_id']}] {row['issue']}")
            print(f"    city={row['city']!r} region={row['region']!r} country={row['country']!r}")
            print(f"    left={row['left_display']!r} right={row['right_display']!r}")
    else:
        print("  All events PASS display validation.")

    print(f"\nSaved: {OUTPUT_CSV}")

    # Spot-check examples
    print("\nSpot checks:")
    check_pairs = [
        ("Rochester", "New York", "United States"),
        ("Montreal", "Quebec", "Canada"),
        ("Vienna", "Vienna", "Austria"),
        ("Bilbao", "Biscay", "Spain"),
        ("Stara Zagora", "Stara Zagora", "Bulgaria"),
        ("Tokyo", "", "Japan"),
    ]
    for city, region, country in check_pairs:
        eid = "TEST"
        canonical_locations["TEST"] = (city, region, country)
        left, right = build_location_pair("TEST", canonical_locations)
        print(f"  {city!r} / {region!r} / {country!r}")
        print(f"    → left={left!r}  right={right!r}")

    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
