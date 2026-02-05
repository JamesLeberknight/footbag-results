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
import json
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


def extract_event_record(html: str, source_path: str, source_url: str, soup: BeautifulSoup = None) -> dict:
    """
    Extract raw event data from HTML.
    Returns dict with raw fields and parse notes/warnings.
    """
    if soup is None:
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
        "_html": html,  # Store for QC checks
        "_soup": soup,  # Store parsed soup for QC checks
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


# ------------------------------------------------------------
# Stage 1 QC System
# ------------------------------------------------------------
def check_results_extraction(rec: dict) -> list[dict]:
    """Check if results were properly extracted."""
    issues = []
    event_id = rec.get("event_id", "")
    location = rec.get("location_raw", "")
    date = rec.get("date_raw", "")
    results = rec.get("results_block_raw", "")
    html = rec.get("_html", "")

    # s1_results_empty: Event has location/date but no results
    if (location or date) and not results:
        issues.append({
            "check_id": "s1_results_empty",
            "severity": "WARN",
            "event_id": event_id,
            "field": "results_block_raw",
            "message": "Event has location/date but no results_block_raw"
        })

    # s1_results_short: results_block_raw < 50 chars (may be incomplete)
    if results and len(results) < 50:
        issues.append({
            "check_id": "s1_results_short",
            "severity": "INFO",
            "event_id": event_id,
            "field": "results_block_raw",
            "message": f"results_block_raw is short ({len(results)} chars, may be incomplete)",
            "example_value": results[:50]
        })

    # s1_results_has_patterns_but_empty: HTML has numbered entries but extraction failed
    if not results and html:
        # Look for placement patterns in HTML
        has_placement_pattern = bool(re.search(r'^\s*[1-9]\d?\s*[.):\-]\s+[A-Z]', html, re.MULTILINE))
        if has_placement_pattern:
            issues.append({
                "check_id": "s1_results_has_patterns_but_empty",
                "severity": "ERROR",
                "event_id": event_id,
                "field": "results_block_raw",
                "message": "HTML contains numbered entries but extraction failed"
            })

    return issues


def check_html_structure(rec: dict) -> list[dict]:
    """Check if expected HTML structure elements were found."""
    issues = []
    event_id = rec.get("event_id", "")
    soup = rec.get("_soup")

    if not soup:
        return issues

    # s1_html_no_events_results_div: Could not find div.eventsResults
    results_div = soup.select_one("div.eventsResults")
    if not results_div:
        issues.append({
            "check_id": "s1_html_no_events_results_div",
            "severity": "WARN",
            "event_id": event_id,
            "field": "html_structure",
            "message": "Could not find div.eventsResults in HTML"
        })

    # s1_html_no_pre_block: No pre.eventsPre found
    pre_block = soup.select_one("pre.eventsPre")
    if not pre_block:
        issues.append({
            "check_id": "s1_html_no_pre_block",
            "severity": "INFO",
            "event_id": event_id,
            "field": "html_structure",
            "message": "No pre.eventsPre found in HTML"
        })

    return issues


def check_field_extraction(rec: dict) -> list[dict]:
    """Check if core fields were extracted."""
    issues = []
    event_id = rec.get("event_id", "")

    # Known broken source events (SQL errors) - don't error on these
    KNOWN_BROKEN = {
        "1023993464", "1030642331", "1099545007", "1151949245",
        "1278991986", "1299244521", "860082052", "941066992", "959094047"
    }
    is_known_broken = str(event_id) in KNOWN_BROKEN

    # s1_location_missing: location_raw empty (not a known broken source)
    if not rec.get("location_raw") and not is_known_broken:
        issues.append({
            "check_id": "s1_location_missing",
            "severity": "ERROR",
            "event_id": event_id,
            "field": "location_raw",
            "message": "location_raw is empty"
        })

    # s1_date_missing: date_raw empty
    if not rec.get("date_raw"):
        issues.append({
            "check_id": "s1_date_missing",
            "severity": "WARN",
            "event_id": event_id,
            "field": "date_raw",
            "message": "date_raw is empty"
        })

    # s1_year_not_found: No year in date or title
    if not rec.get("year"):
        issues.append({
            "check_id": "s1_year_not_found",
            "severity": "WARN",
            "event_id": event_id,
            "field": "year",
            "message": "No year found in date or title"
        })

    # s1_event_name_missing: No event name extracted
    if not rec.get("event_name_raw"):
        issues.append({
            "check_id": "s1_event_name_missing",
            "severity": "ERROR",
            "event_id": event_id,
            "field": "event_name_raw",
            "message": "No event name extracted"
        })

    return issues


def run_stage1_qc(records: list[dict]) -> tuple[dict, list[dict]]:
    """
    Run all Stage 1 QC checks.
    Returns (summary_dict, issues_list).
    """
    all_issues = []

    # Run checks on each record
    for rec in records:
        all_issues.extend(check_results_extraction(rec))
        all_issues.extend(check_html_structure(rec))
        all_issues.extend(check_field_extraction(rec))

    # Build summary
    from collections import defaultdict
    counts_by_check = defaultdict(lambda: {"ERROR": 0, "WARN": 0, "INFO": 0})
    for issue in all_issues:
        counts_by_check[issue["check_id"]][issue["severity"]] += 1

    total_errors = sum(1 for i in all_issues if i["severity"] == "ERROR")
    total_warnings = sum(1 for i in all_issues if i["severity"] == "WARN")
    total_info = sum(1 for i in all_issues if i["severity"] == "INFO")

    # Field coverage stats
    field_coverage = {}
    for field in ["event_id", "event_name_raw", "date_raw", "location_raw", "year", "results_block_raw"]:
        non_empty = sum(1 for r in records if r.get(field) not in [None, ""])
        field_coverage[field] = {
            "present": non_empty,
            "total": len(records),
            "percent": round(100 * non_empty / len(records), 1) if records else 0,
        }

    summary = {
        "total_records": len(records),
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "total_info": total_info,
        "counts_by_check": dict(counts_by_check),
        "field_coverage": field_coverage,
    }

    return summary, all_issues


def write_stage1_qc_outputs(summary: dict, issues: list[dict], out_dir: Path) -> None:
    """Write Stage 1 QC summary and issues to output files."""
    # Write summary JSON
    summary_path = out_dir / "stage1_qc_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Wrote: {summary_path}")

    # Write issues JSONL
    issues_path = out_dir / "stage1_qc_issues.jsonl"
    with open(issues_path, "w", encoding="utf-8") as f:
        for issue in issues:
            f.write(json.dumps(issue, ensure_ascii=False) + "\n")
    print(f"Wrote: {issues_path} ({len(issues)} issues)")


def print_stage1_qc_summary(summary: dict) -> None:
    """Print Stage 1 QC summary to console."""
    print(f"\n{'='*60}")
    print("STAGE 1 QC SUMMARY")
    print(f"{'='*60}")
    print(f"Total records: {summary['total_records']}")
    print(f"Total errors:  {summary['total_errors']}")
    print(f"Total warnings: {summary['total_warnings']}")
    print(f"Total info:    {summary['total_info']}")

    print("\nField coverage:")
    for field, stats in summary.get("field_coverage", {}).items():
        print(f"  {field:20s}: {stats['present']:4d}/{stats['total']:4d} ({stats['percent']:5.1f}%)")

    print("\nIssues by check:")
    for check_id, counts in sorted(summary.get("counts_by_check", {}).items()):
        err = counts.get("ERROR", 0)
        warn = counts.get("WARN", 0)
        info = counts.get("INFO", 0)
        parts = []
        if err > 0:
            parts.append(f"{err} ERROR")
        if warn > 0:
            parts.append(f"{warn} WARN")
        if info > 0:
            parts.append(f"{info} INFO")
        if parts:
            print(f"  {check_id}: {', '.join(parts)}")

    print(f"{'='*60}\n")


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

    # Run Stage 1 QC checks
    print("\nRunning Stage 1 QC checks...")
    qc_summary, qc_issues = run_stage1_qc(records)
    write_stage1_qc_outputs(qc_summary, qc_issues, out_dir)
    print_stage1_qc_summary(qc_summary)


if __name__ == "__main__":
    main()
