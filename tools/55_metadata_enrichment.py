"""
tools/55_metadata_enrichment.py
────────────────────────────────
Fetch missing event metadata (host_club, date) from live footbag.org event pages.
Produces overrides/event_metadata_overrides.csv for use by 04B and 05.

Only fills fields that are blank in stage2_canonical_events.csv.
Never overwrites non-empty values.

Usage:
    .venv/bin/python tools/55_metadata_enrichment.py [--limit N]
"""

from __future__ import annotations

import csv, json, re, sys, time, argparse
from pathlib import Path

csv.field_size_limit(sys.maxsize)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT     = Path(__file__).resolve().parent.parent
STAGE2   = ROOT / "out" / "stage2_canonical_events.csv"
OUT_FILE = ROOT / "overrides" / "event_metadata_overrides.csv"

MEMBER_ID = "11985"
MEMBER_PW = "fb5XPirIXHzxA"
RATE_LIMIT = 0.35


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "footbag-archive-research/1.0"})
    # authenticate
    s.post("http://www.footbag.org/members/list", data={
        "MemberID": MEMBER_ID, "MemberPassword": MEMBER_PW,
        "SearchText": "", "Submit": "Search"
    }, timeout=10)
    return s


def fetch_event_page(session: requests.Session, event_id: str) -> str | None:
    try:
        r = session.get(f"http://www.footbag.org/events/show/{event_id}", timeout=12)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


def parse_metadata(html: str) -> dict:
    """Extract structured fields from an event page."""
    result = {}

    # Host club: look for text inside eventsHostClubInner
    m = re.search(r'eventsHostClubInner[^>]*>\s*(?:<a[^>]*>)?([^<\n]{3,120})', html)
    if m:
        hc = m.group(1).strip()
        if hc and not hc.startswith('</'):
            result['host_club'] = hc

    # Date: eventsDateHeader or eventsDate
    m = re.search(r'eventsDate(?:Header)?[^>]*>\s*\n?\s*([A-Z][a-zA-Z]+ \d{1,2}[-–,][^\n<]{3,40})', html)
    if m:
        result['date'] = m.group(1).strip()

    # Location: eventsLocationInner
    m = re.search(r'eventsLocationInner[^>]*>\s*([^<\n]{4,120}(?:\n[^<\n]{0,60})?)', html)
    if m:
        loc = re.sub(r'\s+', ' ', m.group(1)).strip()
        if loc:
            result['location'] = loc

    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    with open(STAGE2) as f:
        events = list(csv.DictReader(f))

    # Find events missing host_club or date
    to_enrich = []
    for ev in events:
        needs = []
        if not ev.get('host_club', '').strip():
            needs.append('host_club')
        if not ev.get('date', '').strip():
            needs.append('date')
        if needs:
            to_enrich.append((ev['event_id'], ev['year'], ev['event_name'], needs))

    if args.limit:
        to_enrich = to_enrich[:args.limit]

    print(f"Events needing enrichment: {len(to_enrich)}")

    # Load existing overrides to avoid re-fetching
    existing: dict[str, dict] = {}
    if OUT_FILE.exists():
        with open(OUT_FILE) as f:
            for r in csv.DictReader(f):
                existing[r['event_id']] = r

    session = make_session()
    new_rows = []
    fetched = skipped = 0

    for eid, year, name, needs in to_enrich:
        if eid in existing:
            skipped += 1
            continue

        html = fetch_event_page(session, eid)
        time.sleep(RATE_LIMIT)
        fetched += 1

        if not html:
            continue

        meta = parse_metadata(html)
        if not meta:
            continue

        row = {'event_id': eid, 'year': year, 'event_name': name}
        for field in ('host_club', 'date', 'location'):
            row[field] = meta.get(field, '')

        if any(row[f] for f in ('host_club', 'date', 'location')):
            new_rows.append(row)
            print(f"  {eid} | {name[:40]} → host_club={row['host_club']!r:.30} date={row['date']!r:.25}")

    # Merge with existing and write
    all_rows = list(existing.values()) + new_rows
    with open(OUT_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['event_id','year','event_name','host_club','date','location'])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n=== DONE ===")
    print(f"  Fetched: {fetched}  |  Skipped (cached): {skipped}  |  New enrichments: {len(new_rows)}")
    print(f"  Written: {OUT_FILE} ({len(all_rows)} total rows)")


if __name__ == "__main__":
    main()
