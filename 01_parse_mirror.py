#!/usr/bin/env python3
"""
01_parse_mirror.py — Stage 1: Extract raw facts from HTML mirror

This script:
- Reads local offline mirror under ./mirror
- Extracts raw event data from HTML (no semantic cleaning)
- Outputs: out/stage1_raw_events.csv

Input: ./mirror/www.footbag.org/events/show/*/index.html
Output: out/stage1_raw_events.csv
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable, Optional

from bs4 import BeautifulSoup


# CSV safety: remove control chars that could cause issues
_ILLEGAL_CSV_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def sanitize_csv_string(s: str) -> str:
    """Remove control characters for CSV safety."""
    if not isinstance(s, str):
        return s
    return _ILLEGAL_CSV_RE.sub("", s)


# ------------------------------------------------------------
# Mirror discovery
# ------------------------------------------------------------
def find_events_show_dir(mirror_dir: Path) -> Path:
    """Find the events/show directory in the mirror."""
    mirror_dir = mirror_dir.resolve()
    candidates = [
        mirror_dir / "www.footbag.org" / "events" / "show",
        mirror_dir / "events" / "show",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(f"No events/show directory found under {mirror_dir}")


def iter_event_html_files(events_show: Path) -> Iterable[Path]:
    """Iterate over event HTML files in the mirror."""
    for subdir in sorted(events_show.iterdir()):
        if not subdir.is_dir():
            continue
        if not subdir.name.isdigit():
            continue

        html_file = subdir / "index.html"
        if not html_file.exists():
            html_file = subdir / f"{subdir.name}.html"

        if html_file.exists():
            yield html_file.resolve()


# ------------------------------------------------------------
# HTML extraction helpers
# ------------------------------------------------------------
def _text_or_none(node) -> Optional[str]:
    """Extract text from a BeautifulSoup node."""
    if not node:
        return None
    txt = node.get_text(" ", strip=True)
    return txt.strip() if txt else None


def extract_by_bold_label(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    Extract value following bold label like:
      <b>Host Club:</b> VALUE
    Best-effort only.
    """
    b = soup.find("b", string=re.compile(rf"^{label}\s*:?\s*$", re.I))
    if not b:
        return None

    sib = b.find_next_sibling()
    if sib:
        v = _text_or_none(sib)
        if v:
            return v

    parent = b.parent
    if parent:
        full = parent.get_text(" ", strip=True)
        full = re.sub(rf"^{label}\s*:?\s*", "", full, flags=re.I).strip()
        return full or None

    return None


def extract_event_record(html: str, source_path: str, source_url: str) -> dict:
    """
    Extract raw event data from HTML.
    Returns dict with raw fields and parse notes/warnings.
    """
    soup = BeautifulSoup(html, "html.parser")

    parse_notes = []
    warnings = []

    # event_id from URL path
    parts = source_url.split("/")
    event_id = next((p for p in reversed(parts) if p.isdigit()), None)
    if not event_id:
        warnings.append("event_id: not found in path")

    # event name from title
    event_name_raw = None
    if soup.title and soup.title.string:
        event_name_raw = soup.title.string.strip()
        parse_notes.append("event_name: <title> tag")
    else:
        warnings.append("event_name: <title> tag missing")

    # Date from DOM block
    date_raw = None
    date_node = soup.select_one("div.eventsDateHeader")
    if date_node:
        date_raw = _text_or_none(date_node)
        if date_raw:
            date_raw = re.sub(r"\(\s*concluded\s*\)$", "", date_raw, flags=re.I).strip()
            parse_notes.append("date: div.eventsDateHeader")
    if not date_raw:
        warnings.append("date: div.eventsDateHeader missing")

    # Location from DOM block
    location_raw = None
    location_node = soup.select_one("div.eventsLocationInner")
    if location_node:
        location_raw = _text_or_none(location_node)
        parse_notes.append("location: div.eventsLocationInner")
    if not location_raw:
        warnings.append("location: div.eventsLocationInner missing")

    # Host Club - try DOM first, then bold label
    host_club_raw = None
    host_club_node = soup.select_one("div.eventsHostClubInner")
    if host_club_node:
        host_club_raw = _text_or_none(host_club_node)
        parse_notes.append("host_club: div.eventsHostClubInner")
    if not host_club_raw:
        host_club_raw = extract_by_bold_label(soup, "Host Club") or extract_by_bold_label(soup, "Host")
        if host_club_raw:
            parse_notes.append("host_club: bold label")
    if not host_club_raw:
        warnings.append("host_club: not found")

    # Event Type from bold label
    event_type_raw = extract_by_bold_label(soup, "Event Type") or extract_by_bold_label(soup, "Type")
    if event_type_raw:
        parse_notes.append("event_type: bold label")
    else:
        warnings.append("event_type: not found")

    # Year detection from date or title
    year = None
    for source in (date_raw or "", event_name_raw or ""):
        m = re.search(r"\b(19\d{2}|20\d{2})\b", source)
        if m:
            year = int(m.group(1))
            break
    if not year:
        warnings.append("year: not found in date or title")

    # Raw results blob - look specifically in eventsResults div for actual results
    # This div may contain:
    #   1. Structured results in <h2> headers with <br> separated entries (preferred)
    #   2. "Manually Entered Results" in a <pre> block (fallback)
    results_block_raw = None
    results_div = soup.select_one("div.eventsResults")
    if results_div:
        # PREFERRED: Try extracting structured results from <h2> division headers first
        # These have proper division names like "Open Singles Net:" with <br>-separated entries
        # Note: Mixed <br> and <br/> in HTML causes BeautifulSoup issues, so we extract
        # the full text and parse it line by line instead of walking the DOM
        h2_tags = results_div.find_all("h2")
        division_headers = [h2.get_text(strip=True) for h2 in h2_tags
                           if h2.get_text(strip=True) and "manually" not in h2.get_text(strip=True).lower()]

        if division_headers:
            # Get full text of results div and parse it
            full_text = results_div.get_text("\n", strip=False)
            lines = full_text.splitlines()
            structured_results = []
            in_structured_section = False
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Check if this line is a division header we found
                if line in division_headers or line.rstrip(":") in division_headers:
                    in_structured_section = True
                    structured_results.append(line)
                    continue
                # Stop at "Manually Entered Results" or other non-result sections
                if "manually entered" in line.lower() or line.startswith("Related Photos"):
                    in_structured_section = False
                    continue
                # Collect numbered entries
                if in_structured_section and re.match(r'^\d+\.?\s+\S', line):
                    structured_results.append(line)

            if structured_results and len(structured_results) > len(division_headers):
                results_block_raw = "\n".join(structured_results)
                parse_notes.append("results: div.eventsResults > h2 + structured")

        # FALLBACK: If no structured results, look for <pre> tags with placements
        if not results_block_raw:
            all_pres = results_div.find_all("pre")
            for pre in all_pres:
                pre_text = pre.get_text("\n", strip=False)
                # Check if this pre contains actual results (numbered placements)
                if re.search(r'^\s*[1-9]\d?\s*[.)\-:]\s*\S', pre_text, re.MULTILINE):
                    results_block_raw = pre_text
                    parse_notes.append("results: div.eventsResults > pre (with placements)")
                    break

        # Final fallback: any pre.eventsPre in eventsResults
        if not results_block_raw:
            results_pre = results_div.select_one("pre.eventsPre")
            if results_pre:
                results_block_raw = results_pre.get_text("\n", strip=False)
                parse_notes.append("results: div.eventsResults > pre.eventsPre")

    # Fallback to first pre.eventsPre if no eventsResults div
    if not results_block_raw:
        pre = soup.select_one("pre.eventsPre")
        if pre:
            results_block_raw = pre.get_text("\n", strip=False)
            parse_notes.append("results: pre.eventsPre (fallback)")

    if not results_block_raw:
        warnings.append("results: no results found in HTML")

    return {
        "event_id": event_id,
        "year": year,
        "source_path": source_path,
        "source_url": source_url,
        "event_name_raw": sanitize_csv_string(event_name_raw) if event_name_raw else None,
        "date_raw": sanitize_csv_string(date_raw) if date_raw else None,
        "location_raw": sanitize_csv_string(location_raw) if location_raw else None,
        "host_club_raw": sanitize_csv_string(host_club_raw) if host_club_raw else None,
        "event_type_raw": sanitize_csv_string(event_type_raw) if event_type_raw else None,
        "results_block_raw": sanitize_csv_string(results_block_raw) if results_block_raw else None,
        "html_parse_notes": "; ".join(parse_notes),
        "html_warnings": "; ".join(warnings),
    }


def parse_mirror(mirror_dir: Path) -> list[dict]:
    """Parse all event HTML files from the mirror."""
    events_show = find_events_show_dir(mirror_dir)
    records = []

    for html_file in iter_event_html_files(events_show):
        html = html_file.read_text(encoding="utf-8", errors="replace")
        source_path = str(html_file)
        source_url = "file://" + source_path.replace("\\", "/")

        rec = extract_event_record(html, source_path, source_url)
        records.append(rec)

    return records


def write_stage1_csv(records: list[dict], out_path: Path) -> None:
    """Write records to stage1 CSV file."""
    if not records:
        print("No records to write!")
        return

    fieldnames = [
        "event_id",
        "year",
        "source_path",
        "source_url",
        "event_name_raw",
        "date_raw",
        "location_raw",
        "host_club_raw",
        "event_type_raw",
        "results_block_raw",
        "html_parse_notes",
        "html_warnings",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def print_verification_stats(records: list[dict]) -> None:
    """Print verification gate statistics."""
    total = len(records)
    print(f"\n{'='*60}")
    print("VERIFICATION GATE: Stage 1 (HTML Parsing)")
    print(f"{'='*60}")
    print(f"Total events parsed: {total}")

    if total == 0:
        return

    # Calculate % missing per field
    fields = [
        "event_id", "year", "event_name_raw", "date_raw",
        "location_raw", "host_club_raw", "event_type_raw", "results_block_raw"
    ]

    print("\nField coverage:")
    for field in fields:
        missing = sum(1 for r in records if not r.get(field))
        pct_present = ((total - missing) / total) * 100
        print(f"  {field:20s}: {pct_present:5.1f}% present ({total - missing}/{total})")

    # Year distribution
    years = [r["year"] for r in records if r.get("year")]
    if years:
        print(f"\nYear range: {min(years)} - {max(years)}")
        print(f"Events with year: {len(years)}/{total}")

    # Sample output (first 3 events)
    print("\nSample events (first 3):")
    for i, rec in enumerate(records[:3]):
        print(f"  [{i+1}] event_id={rec.get('event_id')}, "
              f"year={rec.get('year')}, "
              f"name={str(rec.get('event_name_raw', ''))[:40]}...")

    # Count events with warnings
    with_warnings = sum(1 for r in records if r.get("html_warnings"))
    print(f"\nEvents with parse warnings: {with_warnings}/{total}")

    print(f"{'='*60}\n")


def main():
    """
    Parse HTML mirror and output stage1_raw_events.csv
    """
    repo_dir = Path(__file__).resolve().parent
    # Mirror is in the repo directory
    mirror_dir = repo_dir / "mirror"
    out_dir = repo_dir / "out"
    out_csv = out_dir / "stage1_raw_events.csv"

    # Ensure output directory exists
    out_dir.mkdir(exist_ok=True)

    print(f"Parsing mirror at: {mirror_dir}")
    records = parse_mirror(mirror_dir)

    print(f"Writing to: {out_csv}")
    write_stage1_csv(records, out_csv)

    print_verification_stats(records)
    print(f"Wrote: {out_csv}")


if __name__ == "__main__":
    main()
