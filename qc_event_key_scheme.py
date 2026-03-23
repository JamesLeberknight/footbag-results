#!/usr/bin/env python3
"""
qc_event_key_scheme.py

Test proposed canonical event ID scheme:
    {year}-{descriptor}-{city}-{country}

against out/stage2_canonical_events.csv
"""

from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict, Counter
from pathlib import Path
csv.field_size_limit(sys.maxsize)
OUT = Path("out")
INPUT = OUT / "stage2_canonical_events.csv"


def slugify(text: str) -> str:
    s = (text or "").strip().lower()
    s = re.sub(r"['\u2019\u2018\u201c\u201d]", "", s)
    s = re.sub(r"&", " and ", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def parse_location(location: str) -> tuple[str, str, str]:
    """
    Best-effort parse to (city, region, country)
    """
    if not location:
        return "", "", ""

    location = location.split("/")[0].strip()
    parts = [p.strip() for p in location.split(",") if p.strip()]

    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return parts[0], "", parts[1]
    if len(parts) == 1:
        return "", "", parts[0]
    return "", "", ""


def normalize_country(country: str) -> str:
    c = slugify(country)
    mapping = {
        "united-states": "usa",
        "usa": "usa",
        "u-s-a": "usa",
        "us": "usa",
        "u-s": "usa",
        "united-kingdom": "uk",
        "great-britain": "uk",
    }
    return mapping.get(c, c or "unknown")


def normalize_descriptor(event_name: str) -> str:
    n = (event_name or "").lower().strip()

    # broad priority mapping
    rules = [
        (r"\b(world|worlds|world championship|world championships|ifpa world)\b", "worlds"),
        (r"\b(us open|u\.s\. open|u s open)\b", "us-open"),
        (r"\beuropean championship|\beuropean championships|\beuros?\b", "euros"),
        (r"\basian championship|\basian championships|\basia\b", "asia"),
        (r"\bcanadian championship|\bcanadian championships|\bcanada\b", "canada"),
        (r"\bnational championship|\bnational championships|\bnationals?\b", "nationals"),
        (r"\bfootbag open\b|\bopen\b", "open"),
        (r"\bchampionship\b|\bchampionships\b", "championships"),
    ]
    for pattern, label in rules:
        if re.search(pattern, n):
            return label

    # fallback: first 3 useful words of slugified name
    words = [w for w in slugify(event_name).split("-") if w]
    stop = {"the", "and", "of", "footbag", "freestyle", "net", "championship", "championships"}
    useful = [w for w in words if w not in stop]
    if useful:
        return "-".join(useful[:3])
    return "event"


def make_candidate_id(year: str, event_name: str, city: str, country: str) -> str:
    year_slug = slugify(year) or "unknown-year"
    descriptor = normalize_descriptor(event_name)
    city_slug = slugify(city) or "unknown-city"
    country_slug = normalize_country(country)
    return f"{year_slug}-{descriptor}-{city_slug}-{country_slug}"


def main() -> None:
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing input: {INPUT}")

    rows = []
    with INPUT.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            event_id = row.get("event_id", "")
            year = row.get("year", "")
            event_name = row.get("event_name", "")
            location = row.get("location", "")
            city, region, country = parse_location(location)
            candidate = make_candidate_id(year, event_name, city, country)

            rows.append({
                "event_id": event_id,
                "year": year,
                "event_name": event_name,
                "location": location,
                "city": city,
                "region": region,
                "country": country,
                "candidate_id": candidate,
                "descriptor": normalize_descriptor(event_name),
            })

    groups = defaultdict(list)
    for r in rows:
        groups[r["candidate_id"]].append(r)

    collision_groups = {k: v for k, v in groups.items() if len(v) > 1}

    print(f"Total events: {len(rows):,}")
    print(f"Unique candidate IDs: {len(groups):,}")
    print(f"Collision groups: {len(collision_groups):,}")
    print(f"Events involved in collisions: {sum(len(v) for v in collision_groups.values()):,}")
    print()

    if collision_groups:
        print("Top collision groups:")
        for cid, items in sorted(collision_groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:20]:
            print(f"\n{cid}  -> {len(items)} events")
            for r in items:
                print(f"  {r['event_id']:>12} | {r['year']} | {r['event_name']} | {r['location']}")

    missing_city = [r for r in rows if not r["city"].strip()]
    missing_country = [r for r in rows if not r["country"].strip()]
    unknown_city_ids = [r for r in rows if "unknown-city" in r["candidate_id"]]
    unknown_country_ids = [r for r in rows if "unknown" == normalize_country(r["country"])]

    print("\nCoverage diagnostics:")
    print(f"  Missing city: {len(missing_city):,}")
    print(f"  Missing country: {len(missing_country):,}")
    print(f"  Candidate IDs with unknown-city: {len(unknown_city_ids):,}")
    print(f"  Candidate IDs with unknown country: {len(unknown_country_ids):,}")

    print("\nMost common descriptors:")
    for desc, count in Counter(r["descriptor"] for r in rows).most_common(20):
        print(f"  {desc:20} {count:>5}")

    if unknown_city_ids:
        print("\nExamples with missing city:")
        for r in unknown_city_ids[:20]:
            print(f"  {r['event_id']:>12} | {r['year']} | {r['event_name']} | {r['location']}")

    # Optional recommendation
    if collision_groups:
        print("\nRecommendation:")
        print("  Use {year}-{descriptor}-{city}-{country} as the primary pattern,")
        print("  and append -{legacy_event_id} only for collision cases.")
    else:
        print("\nRecommendation:")
        print("  The proposed scheme appears collision-free on the current dataset.")


if __name__ == "__main__":
    main()
