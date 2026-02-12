#!/usr/bin/env python3
"""
04_import_old_results_secondary.py

Parse OLD_RESULTS.txt into a SECONDARY-EVIDENCE placements table.
This does NOT overwrite or “merge” anything. It just produces clean, traceable
rows you can later use to suggest name-spelling fixes, etc.

Input:
  OLD_RESULTS.txt   (or any path you pass)

Outputs:
  out/secondary_evidence/old_results__placements_raw.csv
  out/secondary_evidence/old_results__events_raw.csv
  out/secondary_evidence/import_report__old_results.json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict


EVENT_HEADER_RE = re.compile(r"^\s*(\d{4})\s+(NHSA|WFA)\s*:\s*$", re.IGNORECASE)
DIV_HEADING_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9 '\/&.\-]+)\s*:\s*$")
PLACEMENT_RE = re.compile(r"^\s*(\d+)(st|nd|rd|th)\s*-\s*(.+?)\s*$", re.IGNORECASE)
INLINE_DIV_PLACEMENT_RE = re.compile(
    r"^\s*(.+?)\s*-\s*(\d+)(st|nd|rd|th)\s*-\s*(.+?)\s*$", re.IGNORECASE
)

# Treat lines that are just replacement characters / junk as blank.
JUNK_LINE_RE = re.compile(r"^[\s�]+$")


def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())


def norm_for_match(s: str) -> str:
    """Lightweight normalization for matching (secondary-only)."""
    s = s.strip().lower()
    s = re.sub(r"[^\w\s/]+", "", s)  # keep slash for teams
    s = re.sub(r"\s+", " ", s)
    return s


def split_members(competitor_raw: str) -> List[str]:
    # Secondary matching helper only: split on "/" but keep original elsewhere
    parts = [norm_spaces(p) for p in competitor_raw.split("/") if norm_spaces(p)]
    return parts


@dataclass
class PlacementRow:
    sec_source: str
    line_no: int
    sec_event_key: str
    year: int
    org: str
    division_raw: str
    place_raw: int
    competitor_raw: str
    notes_raw: str
    competitor_norm: str
    member1_raw: str
    member2_raw: str
    member3_raw: str
    member4_raw: str


@dataclass
class EventRow:
    sec_source: str
    sec_event_key: str
    year: int
    org: str
    event_name_raw: str
    date_precision: str


def parse_old_results(text: str, sec_source: str) -> tuple[List[EventRow], List[PlacementRow], Dict]:
    events: Dict[str, EventRow] = {}
    placements: List[PlacementRow] = []

    current_year: Optional[int] = None
    current_org: Optional[str] = None
    current_event_key: Optional[str] = None
    current_division: Optional[str] = None

    stats = {
        "events_seen": 0,
        "placements_parsed": 0,
        "inline_div_placements": 0,
        "heading_divisions_seen": 0,
        "placements_without_division": 0,
        "lines_total": 0,
        "lines_junk_or_blank": 0,
        "lines_ignored": 0,
    }

    lines = text.splitlines()
    stats["lines_total"] = len(lines)

    for idx, raw in enumerate(lines, start=1):
        line = raw.strip()

        # Skip junk/blank lines
        if not line or JUNK_LINE_RE.match(line):
            stats["lines_junk_or_blank"] += 1
            continue

        # Event header
        m = EVENT_HEADER_RE.match(line)
        if m:
            current_year = int(m.group(1))
            current_org = m.group(2).upper()
            current_event_key = f"{current_org}_{current_year}"
            current_division = None
            stats["events_seen"] += 1

            if current_event_key not in events:
                events[current_event_key] = EventRow(
                    sec_source=sec_source,
                    sec_event_key=current_event_key,
                    year=current_year,
                    org=current_org,
                    event_name_raw="World Championships (OLD_RESULTS)",
                    date_precision="year",
                )
            continue

        # If we haven't seen an event header yet, ignore
        if current_event_key is None or current_year is None or current_org is None:
            stats["lines_ignored"] += 1
            continue

        # Division heading (e.g., "Singles:")
        mh = DIV_HEADING_RE.match(line)
        if mh:
            div = norm_spaces(mh.group(1))
            # Avoid treating event headers like "1985 WFA:" as division headings (already handled)
            # Also avoid catching "Results 1982-1986" type headers; they won't match due to digits
            current_division = div
            stats["heading_divisions_seen"] += 1
            continue

        # Inline: "Division - 1st - Name"
        mi = INLINE_DIV_PLACEMENT_RE.match(line)
        if mi:
            division_raw = norm_spaces(mi.group(1))
            place_raw = int(mi.group(2))
            competitor_raw = norm_spaces(mi.group(4))
            notes_raw = ""

            members = split_members(competitor_raw)
            members += ["", "", "", ""]
            placements.append(
                PlacementRow(
                    sec_source=sec_source,
                    line_no=idx,
                    sec_event_key=current_event_key,
                    year=current_year,
                    org=current_org,
                    division_raw=division_raw,
                    place_raw=place_raw,
                    competitor_raw=competitor_raw,
                    notes_raw=notes_raw,
                    competitor_norm=norm_for_match(competitor_raw),
                    member1_raw=members[0],
                    member2_raw=members[1],
                    member3_raw=members[2],
                    member4_raw=members[3],
                )
            )
            stats["placements_parsed"] += 1
            stats["inline_div_placements"] += 1
            continue

        # Standard placement: "1st - Name"
        mp = PLACEMENT_RE.match(line)
        if mp:
            place_raw = int(mp.group(1))
            competitor_raw = norm_spaces(mp.group(3))
            notes_raw = ""

            division_raw = current_division if current_division else "unknown"
            if division_raw == "unknown":
                stats["placements_without_division"] += 1

            members = split_members(competitor_raw)
            members += ["", "", "", ""]
            placements.append(
                PlacementRow(
                    sec_source=sec_source,
                    line_no=idx,
                    sec_event_key=current_event_key,
                    year=current_year,
                    org=current_org,
                    division_raw=division_raw,
                    place_raw=place_raw,
                    competitor_raw=competitor_raw,
                    notes_raw=notes_raw,
                    competitor_norm=norm_for_match(competitor_raw),
                    member1_raw=members[0],
                    member2_raw=members[1],
                    member3_raw=members[2],
                    member4_raw=members[3],
                )
            )
            stats["placements_parsed"] += 1
            continue

        # Otherwise: ignore (narrative headers, etc.)
        stats["lines_ignored"] += 1

    report = {
        "sec_source": sec_source,
        "events_count": len(events),
        "placements_count": len(placements),
        "stats": stats,
        "event_keys": sorted(events.keys()),
    }
    return list(events.values()), placements, report


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--infile",
        type=Path,
        default=Path("OLD_RESULTS.txt"),
        help="Path to OLD_RESULTS.txt (default: OLD_RESULTS.txt)",
    )
    ap.add_argument(
        "--outdir",
        type=Path,
        default=Path("out/secondary_evidence"),
        help="Output directory (default: out/secondary_evidence)",
    )
    args = ap.parse_args()

    infile = args.infile.expanduser().resolve()
    outdir = args.outdir.expanduser().resolve()

    if not infile.exists():
        raise FileNotFoundError(f"Input file not found: {infile}")

    text = infile.read_text(encoding="utf-8", errors="replace")
    events, placements, report = parse_old_results(text, sec_source="OLD_RESULTS.txt")

    events_path = outdir / "old_results__events_raw.csv"
    placements_path = outdir / "old_results__placements_raw.csv"
    report_path = outdir / "import_report__old_results.json"

    write_csv(
        events_path,
        [e.__dict__ for e in events],
        fieldnames=[
            "sec_source",
            "sec_event_key",
            "year",
            "org",
            "event_name_raw",
            "date_precision",
        ],
    )

    write_csv(
        placements_path,
        [p.__dict__ for p in placements],
        fieldnames=[
            "sec_source",
            "line_no",
            "sec_event_key",
            "year",
            "org",
            "division_raw",
            "place_raw",
            "competitor_raw",
            "notes_raw",
            "competitor_norm",
            "member1_raw",
            "member2_raw",
            "member3_raw",
            "member4_raw",
        ],
    )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote: {events_path} (rows={len(events)})")
    print(f"Wrote: {placements_path} (rows={len(placements)})")
    print(f"Wrote: {report_path}")
    print("\nReport summary:")
    print(json.dumps({k: report[k] for k in ["events_count", "placements_count", "stats"]}, indent=2))


if __name__ == "__main__":
    main()
