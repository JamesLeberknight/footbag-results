#!/usr/bin/env python3
"""
location_normalize.py — Apply canonical normalization to event location fields.
Reads out/canonical/events.csv, writes out/canonical/events_normalized.csv.
Does NOT overwrite the original.
"""
import csv
import os
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_CSV = os.path.join(BASE_DIR, "out", "canonical", "events.csv")
OUT_CSV = os.path.join(BASE_DIR, "out", "canonical", "events_normalized.csv")
REPORT_DIR = os.path.join(BASE_DIR, "out", "final_verification")
REPORT_CSV = os.path.join(REPORT_DIR, "location_normalization_report.csv")
POST_AUDIT_CSV = os.path.join(REPORT_DIR, "location_audit_post_fix.csv")

os.makedirs(REPORT_DIR, exist_ok=True)

# ── Rule A: Country abbreviations ─────────────────────────────────────────────
COUNTRY_ABBREV_MAP = {
    "USA": "United States",
    "U.S.A.": "United States",
    "U.S.": "United States",
    "US": "United States",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
}

# ── Rule B: Region abbreviations ──────────────────────────────────────────────
# US states
US_STATE_MAP = {
    "CA": "California",
    "NY": "New York",
    "OR": "Oregon",
    "WA": "Washington",
    "TX": "Texas",
    "CO": "Colorado",
    "MA": "Massachusetts",
    "PA": "Pennsylvania",
    "OH": "Ohio",
    "FL": "Florida",
    "IL": "Illinois",
    "MN": "Minnesota",
    "WI": "Wisconsin",
    "MO": "Missouri",
    "AZ": "Arizona",
    "NV": "Nevada",
    "NJ": "New Jersey",
    "UT": "Utah",
    "NC": "North Carolina",
    "SC": "South Carolina",
    "VA": "Virginia",
    "MD": "Maryland",
    "DC": "District of Columbia",
    "AL": "Alabama",
    "ID": "Idaho",
    "MT": "Montana",
    "KS": "Kansas",
    "TN": "Tennessee",
    "ME": "Maine",
}
# GA: Georgia only if US (also a country)
GA_US_ONLY = {"GA": "Georgia"}

# Canadian provinces
CA_PROVINCE_MAP = {
    "BC": "British Columbia",
    "B.C.": "British Columbia",
    "QC": "Quebec",
    "ON": "Ontario",
    "AB": "Alberta",
    "MB": "Manitoba",
    "SK": "Saskatchewan",
}

# Australian states
AU_STATE_MAP = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
}

# Combined — used after country-context check
ALL_REGION_MAPS = {
    "United States": {**US_STATE_MAP, **GA_US_ONLY},
    "Canada": CA_PROVINCE_MAP,
    "Australia": AU_STATE_MAP,
}
# Also apply US states regardless of country for ambiguous cases
UNAMBIGUOUS_REGION_MAP = US_STATE_MAP  # These don't conflict with country names

# Countries where city==region is valid European convention (federal states)
CITY_REGION_DOUBLE_ALLOWED = {
    "Austria", "Germany", "Switzerland", "Russia", "Hungary",
    "Czech Republic", "Slovakia", "Bulgaria", "New Zealand", "Poland",
}


def normalize_country(country: str) -> tuple[str, str]:
    """Returns (normalized_country, rule_applied)"""
    if country in COUNTRY_ABBREV_MAP:
        return COUNTRY_ABBREV_MAP[country], "RULE_A_COUNTRY_ABBREV"
    return country, ""


def normalize_region(region: str, country: str) -> tuple[str, str]:
    """Returns (normalized_region, rule_applied)"""
    if not region:
        return region, ""

    # Check country-specific maps first
    country_map = ALL_REGION_MAPS.get(country, {})
    if region in country_map:
        return country_map[region], "RULE_B_REGION_ABBREV"

    # Check unambiguous US state abbreviations (even if country isn't set right yet)
    if region in UNAMBIGUOUS_REGION_MAP and country in ("United States", "USA", "US", "U.S.", "U.S.A.", ""):
        return UNAMBIGUOUS_REGION_MAP[region], "RULE_B_REGION_ABBREV"

    # B.C. → British Columbia (unambiguous)
    if region == "B.C.":
        return "British Columbia", "RULE_B_REGION_ABBREV"

    return region, ""


def normalize_basque(city: str, region: str) -> tuple[str, str]:
    """Rule C: Basque region → specific province.
    All known footbag Basque events are in Bizkaia (Bilbao + Larrabetzu)."""
    if "basque" not in region.lower():
        return region, ""
    # Both Bilbao and Larrabetzu are in Bizkaia province
    return "Bizkaia", "RULE_C_BASQUE"


def clear_duplicate_city_region(city: str, region: str, country: str) -> tuple[str, str]:
    """Rule D: If city == region exactly, clear region unless in allowed-double countries."""
    if not city or not region:
        return region, ""
    if city.strip().lower() != region.strip().lower():
        return region, ""
    if country in CITY_REGION_DOUBLE_ALLOWED:
        # In these countries, city==region is meaningful (e.g. Berlin state)
        # BUT it's still redundant for display — clear it for cleaner output
        return "", "RULE_D_DUPLICATE_CITY_REGION"
    return "", "RULE_D_DUPLICATE_CITY_REGION"


def fix_venue_in_city(city: str) -> tuple[str, str]:
    """Rule E: Remove venue prefix from city."""
    # RIT Rochester → Rochester
    if city.startswith("RIT "):
        return city[4:].strip(), "RULE_E_VENUE_IN_CITY"
    return city, ""


def normalize_quebec(region: str) -> tuple[str, str]:
    """Normalize Quebec/Québec to consistent 'Quebec' (no accent)."""
    if region == "Québec":
        return "Quebec", "RULE_QUEBEC_NORMALIZE"
    return region, ""


def normalize_event(row: dict) -> tuple[dict, list[str]]:
    """Apply all normalization rules to a single event row.
    Returns (normalized_row, list_of_rules_applied)."""
    city = (row.get("city") or "").strip()
    region = (row.get("region") or "").strip()
    country = (row.get("country") or "").strip()

    rules_applied = []

    # Rule A: Country abbreviation
    country, rule = normalize_country(country)
    if rule:
        rules_applied.append(rule)

    # Rule B: Region abbreviation (after country is normalized)
    region, rule = normalize_region(region, country)
    if rule:
        rules_applied.append(rule)

    # Rule C: Basque region
    region, rule = normalize_basque(city, region)
    if rule:
        rules_applied.append(rule)

    # Rule D: Duplicate city/region
    region, rule = clear_duplicate_city_region(city, region, country)
    if rule:
        rules_applied.append(rule)

    # Rule E: Venue in city
    city, rule = fix_venue_in_city(city)
    if rule:
        rules_applied.append(rule)

    # Quebec normalization
    region, rule = normalize_quebec(region)
    if rule:
        rules_applied.append(rule)

    new_row = dict(row)
    new_row["city"] = city
    new_row["region"] = region
    new_row["country"] = country

    return new_row, rules_applied


def main():
    # Load
    with open(EVENTS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows)} events from {EVENTS_CSV}")

    # Normalize
    normalized_rows = []
    report_rows = []
    rule_counts = {}
    changed_count = 0

    for row in rows:
        orig_city = (row.get("city") or "").strip()
        orig_region = (row.get("region") or "").strip()
        orig_country = (row.get("country") or "").strip()

        new_row, rules = normalize_event(row)

        new_city = new_row["city"]
        new_region = new_row["region"]
        new_country = new_row["country"]

        changed = (orig_city != new_city or orig_region != new_region or orig_country != new_country)

        if changed:
            changed_count += 1

        for r in rules:
            rule_counts[r] = rule_counts.get(r, 0) + 1

        normalized_rows.append(new_row)

        report_rows.append({
            "event_key": row.get("event_key", ""),
            "legacy_event_id": row.get("legacy_event_id", ""),
            "year": row.get("year", ""),
            "event_name": row.get("event_name", ""),
            "orig_city": orig_city,
            "orig_region": orig_region,
            "orig_country": orig_country,
            "new_city": new_city,
            "new_region": new_region,
            "new_country": new_country,
            "changed": "YES" if changed else "no",
            "rules_applied": "|".join(rules),
        })

    # Save normalized events
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_rows)
    print(f"Saved normalized events to {OUT_CSV}")

    # Save report
    report_fields = [
        "event_key", "legacy_event_id", "year", "event_name",
        "orig_city", "orig_region", "orig_country",
        "new_city", "new_region", "new_country",
        "changed", "rules_applied",
    ]
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=report_fields)
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"Saved normalization report to {REPORT_CSV}")

    # Summary
    print(f"\nTotal events changed: {changed_count}")
    print("\nRules applied (counts are events affected):")
    for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
        print(f"  {rule}: {count}")

    # Show all changed rows
    print("\n=== CHANGED ROWS ===")
    for r in report_rows:
        if r["changed"] == "YES":
            print(f"  [{r['year']}] {r['event_name'][:45]}")
            if r["orig_city"] != r["new_city"]:
                print(f"    city:    {r['orig_city']!r} → {r['new_city']!r}")
            if r["orig_region"] != r["new_region"]:
                print(f"    region:  {r['orig_region']!r} → {r['new_region']!r}")
            if r["orig_country"] != r["new_country"]:
                print(f"    country: {r['orig_country']!r} → {r['new_country']!r}")
            print(f"    rules: {r['rules_applied']}")


if __name__ == "__main__":
    main()
