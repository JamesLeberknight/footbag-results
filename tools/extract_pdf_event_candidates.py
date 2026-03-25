#!/usr/bin/env python3
"""
tools/extract_pdf_event_candidates.py

Extract event-level metadata from the footbag.org results PDF archive.

PDF structure (confirmed via coordinate analysis):
  Pages 1-7:  annual index/TOC pages (TOURNAMENT RESULTS FOR YYYY)
  Pages 8+:   event pages, each starting with "FOOTBAG WORLDWIDE : EVENTS"

Each event page has a two-column layout:
  LEFT column (x0 < 370):  event name, date, Location:, Host Club:, Events Offered:, results
  MIDDLE (x0 370-700):     contact info (excluded)
  RIGHT (x0 >= 700):       navigation sidebar (excluded)

Block sequence in the left column (after FBW_HEADER):
  1. "Tournament Results"
  2. <Event Name>           ← may be multi-line block
  3. <Date string>
  4. "Location:"
  5. <city/country>
  6. "Host Club:"
  7. <club name>
  8. "Events Offered:"
  9. <divisions>
  10. results...

Output: out/pdf_compare/pdf_event_candidates.csv
"""

import csv
import re
import sys
import uuid
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: pymupdf not installed. Run: pip install pymupdf")
    sys.exit(1)

ROOT     = Path(__file__).resolve().parents[1]
PDF_PATH = ROOT / "out" / "pdf_compare" / "tmp" / "results_926p.pdf"
OUT_DIR  = ROOT / "out" / "pdf_compare"
OUT_CSV  = OUT_DIR / "pdf_event_candidates.csv"
PDF_NAME = "926-pages-results-footbag.org-2021-02-14.pdf"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Regex helpers ─────────────────────────────────────────────────────────────

DATE_RE = re.compile(
    r'(?:'
    r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
    r'[\s\-]+\d{1,2}(?:\s*[-–—]\s*\d{1,2})?,?\s*\d{4}'
    r'|'
    r'\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{4}'
    r'|'
    r'\d{4}-\d{2}-\d{2}'
    r')',
    re.IGNORECASE,
)

YEAR_RE     = re.compile(r'\b(19[89]\d|20[0-2]\d)\b')
YEAR_HDR_RE = re.compile(r'TOURNAMENT RESULTS FOR (\d{4})', re.IGNORECASE)
TOC_PAGE_RE = re.compile(r'^-page(\d+)\s*$')

FBW_HEADER  = "FOOTBAG WORLDWIDE : EVENTS"
TOURN_HDR   = "Tournament Results"

LOCATION_RE = re.compile(r'^Location\s*:\s*(.*)', re.IGNORECASE)
HOST_RE     = re.compile(r'^Host\s*Club\s*:\s*(.*)', re.IGNORECASE)
HOME_RE     = re.compile(r'^Home\s*Page\s*:\s*(.*)', re.IGNORECASE)
EVENTS_RE   = re.compile(r'^Events?\s*Offered\s*:\s*(.*)', re.IGNORECASE)

SKIP_RE = re.compile(
    r'^(?:add\s+this\s+event|subscribe\s+to\s+all|'
    r'Manually\s+Entered|Related\s+(?:Photos|Videos)|'
    r'Copyright\s+©|\*Copyright|CREATED\s+\w+|LAST\s+UPDAT)',
    re.IGNORECASE,
)

# Location value noise: phrases that appear as first line of location block but aren't the city
LOCATION_NOISE_RE = re.compile(
    r'^(?:See\s+\w+\s+website|Site\(s\)\s*TBA|https?://|www\.|TBA\s*$)',
    re.IGNORECASE,
)

PLACEMENT_RE = re.compile(r'^\d+\.?\s+\S')

# Maximum x-coordinate for main event content column
# Contact info typically starts at x0 ≥ 380 on most pages; use 370 to be safe
CONTENT_MAX_X = 370


# ── Page readers ──────────────────────────────────────────────────────────────

def get_content_blocks_after_fbw(page):
    """
    Return (y_fbw, lines) where y_fbw is the y-coordinate of FBW_HEADER,
    and lines are the left-column content lines AFTER FBW_HEADER.
    Returns (None, []) if FBW_HEADER not found.
    """
    blocks = page.get_text("blocks", sort=True)
    fbw_y = None

    # First pass: find FBW_HEADER y-position
    for b in blocks:
        x0, y0, x1, y1, text, *_ = b
        if FBW_HEADER in text:
            fbw_y = y0
            break

    if fbw_y is None:
        return None, []

    # Second pass: collect left-column lines AFTER fbw_y
    lines = []
    for b in blocks:
        x0, y0, x1, y1, text, *_ = b
        if y0 <= fbw_y:          # skip blocks at or above FBW_HEADER
            continue
        if x0 >= CONTENT_MAX_X:  # skip contact info and nav
            continue
        for raw in text.split("\n"):
            s = raw.strip()
            if not s:
                continue
            if SKIP_RE.match(s):
                continue
            lines.append(s)

    return fbw_y, lines


def is_event_header_page(page) -> bool:
    text = page.get_text("text")
    return FBW_HEADER in text


def extract_year(text: str) -> str:
    m = YEAR_RE.search(text)
    return m.group(1) if m else ""


def clean_location_line(s: str) -> str:
    s = re.sub(r"Site\(s\)\s*TBA\s*", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


# ── Event page extraction ─────────────────────────────────────────────────────

def extract_event_from_page(page_num: int, page) -> dict | None:
    """
    Extract structured event metadata from one event page.
    Returns a dict or None if the page is a year summary or has no useful content.
    """
    fbw_y, lines = get_content_blocks_after_fbw(page)
    if fbw_y is None or not lines:
        return None

    # Detect year-summary pages (no individual events)
    for line in lines[:5]:
        if YEAR_HDR_RE.search(line):
            return None
        if "No Events Yet" in line:
            return None

    rec: dict = {
        "source_pdf":            PDF_NAME,
        "source_page":           str(page_num),
        "event_name_raw":        "",
        "date_raw":              "",
        "year":                  "",
        "location_raw":          "",
        "host_club_raw":         "",
        "event_type_raw":        "",
        "divisions_raw":         "",
        "results_block_raw":     "",
        "extraction_confidence": "low",
        "notes":                 "event_page",
    }

    event_name = ""
    date_raw   = ""
    location   = ""
    host_club  = ""
    divisions  = ""
    result_lines: list[str] = []

    # States: pre_name → in_name → seeking_date → fields → results
    state = "pre_name"

    for line in lines:
        # ── Global field label checks (take priority over state) ───────────
        m = LOCATION_RE.match(line)
        if m:
            part = clean_location_line(m.group(1))
            if part and not location:
                location = part
            state = "after_location"
            continue

        m = HOST_RE.match(line)
        if m:
            part = m.group(1).strip()
            if part and part.lower() not in ("none", "n/a", "tba"):
                host_club = part
            state = "after_host"
            continue

        m = EVENTS_RE.match(line)
        if m:
            divisions = m.group(1).strip()
            state = "results"
            continue

        if HOME_RE.match(line):
            state = "fields"
            continue

        # ── State-specific handling ─────────────────────────────────────────

        if state == "results":
            if PLACEMENT_RE.match(line) and len(line) < 150:
                result_lines.append(line)
            continue

        if state == "after_location":
            # Continuation line(s) after "Location:" label
            cleaned = clean_location_line(line)
            if cleaned and not LOCATION_NOISE_RE.match(cleaned):
                location = cleaned
                state = "fields"
            # else: noise line (Site(s) TBA, See Worlds website, URL) — stay in after_location
            continue

        if state == "after_host":
            # Next content line after "Host Club:" is the club name
            if line.lower() not in ("none", "n/a", "tba"):
                host_club = line
            state = "fields"
            continue

        if state == "fields":
            # Already parsed main metadata; skip other lines
            continue

        if state in ("pre_name", "in_name", "seeking_date"):
            # Skip navigation header lines
            if line == TOURN_HDR or FBW_HEADER in line:
                continue
            if re.match(r'^(?:PREVIOUS|NEXT|English)\b', line, re.I):
                continue

            # Check for date first (might appear on same line as name or after)
            dates = DATE_RE.findall(line)
            if dates and state in ("in_name", "seeking_date"):
                date_raw = "; ".join(dates)
                state = "fields"
                continue

            # Line is not a date → it's the event name or subtitle
            if state == "pre_name":
                if len(line) > 5:
                    event_name = line
                    state = "seeking_date"
                continue

            if state == "seeking_date":
                # Multi-line event name block or subtitle — check if it's a date
                if dates:
                    date_raw = "; ".join(dates)
                    state = "fields"
                else:
                    # Non-date line while seeking date: it's a subtitle or continuation
                    # Append to name if it doesn't look like an ad/noise
                    if not re.match(r'^(?:https?://|www\.)', line, re.I):
                        event_name = event_name  # keep original; skip subtitle
                    # Stay in seeking_date — the actual date line is coming
                continue

    # ── Cleanup ───────────────────────────────────────────────────────────────
    if event_name:
        event_name = re.sub(r'\s{2,}', ' ', event_name).strip()
        # Strip trailing date fragments that got appended
        event_name = DATE_RE.sub("", event_name).strip().rstrip("-– ,").strip()

    if not event_name and not date_raw:
        return None

    # Filter obvious non-event pages (ads that slipped through)
    if event_name and not any(
        kw in event_name.lower() for kw in [
            "footbag", "championship", "tournament", "open", "cup", "jam",
            "challenge", "circuit", "classic", "national", "regional", "world",
            "annual", "masters", "invitational", "polish", "finnish", "austrian",
            "basque", "czech", "slovak", "german", "french", "spanish", "colombian",
            "venezuelan", "bulgarian", "hungarian", "slovenian", "swiss",
        ]
    ):
        if not date_raw and not location:
            return None  # Likely an ad page

    year = extract_year(date_raw) or extract_year(event_name)

    # Location multi-line: fitz sometimes puts city on separate block line separated by "|"
    location = re.sub(r'\s{2,}', ', ', location).strip()

    rec["event_name_raw"]    = event_name
    rec["date_raw"]          = date_raw
    rec["year"]              = year
    rec["location_raw"]      = location
    rec["host_club_raw"]     = host_club
    rec["divisions_raw"]     = divisions
    rec["results_block_raw"] = " | ".join(result_lines[:30])

    score = 0
    if event_name:    score += 3
    if date_raw:      score += 2
    if year:          score += 1
    if location:      score += 2
    if host_club:     score += 1
    if result_lines:  score += 1

    if score >= 8:    rec["extraction_confidence"] = "high"
    elif score >= 5:  rec["extraction_confidence"] = "medium"
    else:             rec["extraction_confidence"] = "low"

    return rec


# ── TOC page extraction ───────────────────────────────────────────────────────

def is_toc_page(page) -> bool:
    return bool(YEAR_HDR_RE.search(page.get_text("text")))


def get_toc_lines(page) -> list[str]:
    """Get all left-column lines from a TOC page (no y-offset filtering)."""
    blocks = page.get_text("blocks", sort=True)
    lines = []
    for b in blocks:
        x0, y0, x1, y1, text, *_ = b
        if x0 >= CONTENT_MAX_X:
            continue
        for raw in text.split("\n"):
            s = raw.strip()
            if s and not SKIP_RE.match(s):
                lines.append(s)
    return lines


def extract_events_from_toc(page_num: int, page) -> list[dict]:
    lines = get_toc_lines(page)
    events = []
    current_year = ""
    i = 0

    while i < len(lines):
        line = lines[i]

        m = YEAR_HDR_RE.search(line)
        if m:
            current_year = m.group(1)
            i += 1
            continue

        if TOC_PAGE_RE.match(line):
            block = []
            j = i + 1
            while j < len(lines):
                nxt = lines[j]
                if TOC_PAGE_RE.match(nxt) or YEAR_HDR_RE.search(nxt):
                    break
                block.append(nxt)
                j += 1
            if block:
                rec = _parse_toc_block(block, current_year, page_num)
                if rec:
                    events.append(rec)
            i = j
            continue

        i += 1

    return events


def _parse_toc_block(lines: list[str], default_year: str, page_num: int) -> dict | None:
    name = ""; date_raw = ""; loc = ""; host = ""

    for line in lines:
        m = LOCATION_RE.match(line)
        if m:
            loc = m.group(1).strip()
            continue
        m = HOST_RE.match(line)
        if m:
            host = m.group(1).strip()
            continue
        dates = DATE_RE.findall(line)
        if dates:
            date_raw = "; ".join(dates)
            cleaned = DATE_RE.sub("", line).strip().rstrip("-– ,")
            if cleaned and not name:
                name = cleaned
            continue
        if not name and len(line) > 5:
            name = line

    if not name:
        return None

    name = re.sub(r'\s+', ' ', name).strip().rstrip("-– ,").strip()
    year = extract_year(date_raw) or default_year

    return {
        "source_pdf":            PDF_NAME,
        "source_page":           str(page_num),
        "event_name_raw":        name,
        "date_raw":              date_raw,
        "year":                  year,
        "location_raw":          re.sub(r"\s{2,}", " ", loc),
        "host_club_raw":         host if host.lower() not in ("none", "", "n/a") else "",
        "event_type_raw":        "",
        "divisions_raw":         "",
        "results_block_raw":     "",
        "extraction_confidence": "medium" if (name and date_raw) else "low",
        "notes":                 "toc_entry",
    }


# ── ID and normalisation ──────────────────────────────────────────────────────

_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def assign_id(event_name: str, year: str, page: str) -> str:
    key = f"pdf::{year}::{event_name.lower().strip()}::{page}"
    return str(uuid.uuid5(_NS, key))[:12]


def normalize_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bchampionships?\b", "championships", s)
    s = re.sub(r"\bworld championships?\b", "worlds", s)
    s = re.sub(r"\bintl\b", "international", s)
    s = re.sub(r'\s+\d{4}$', '', s).strip()
    return s


def normalize_location(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"Site\(s\)\s*TBA\s*", "", s, flags=re.I)
    s = s.lower()
    s = re.sub(r"[^\w\s,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── Main ──────────────────────────────────────────────────────────────────────

FIELDNAMES = [
    "pdf_event_id", "source_pdf", "source_page",
    "event_name_raw", "date_raw", "year",
    "location_raw", "host_club_raw", "event_type_raw",
    "divisions_raw", "results_block_raw",
    "extraction_confidence", "notes",
    "normalized_event_name", "normalized_location",
]


def main():
    if not PDF_PATH.exists():
        print(f"ERROR: PDF not found at {PDF_PATH}")
        sys.exit(1)

    print(f"Opening {PDF_PATH.name} ...")
    doc = fitz.open(str(PDF_PATH))
    n   = len(doc)
    print(f"  {n} pages")

    all_events: list[dict] = []

    # ── Pass 1: full event pages ──────────────────────────────────────────
    print("Pass 1: extracting full event pages ...")
    for pn in range(n):
        page = doc[pn]
        if not is_event_header_page(page):
            continue
        rec = extract_event_from_page(pn + 1, page)
        if rec:
            all_events.append(rec)

    print(f"  Found {len(all_events)} event pages")

    # ── Pass 2: TOC entries (first ~20 pages) ─────────────────────────────
    print("Pass 2: extracting TOC entries ...")
    toc_events: list[dict] = []
    for pn in range(min(20, n)):
        page = doc[pn]
        if is_toc_page(page):
            toc_events.extend(extract_events_from_toc(pn + 1, page))

    print(f"  Found {len(toc_events)} TOC entries")
    doc.close()

    # ── Merge TOC-only entries ────────────────────────────────────────────
    ep_index = {(normalize_name(e["event_name_raw"]), e["year"]) for e in all_events}
    added = 0
    for te in toc_events:
        key = (normalize_name(te["event_name_raw"]), te["year"])
        if key not in ep_index:
            all_events.append(te)
            ep_index.add(key)
            added += 1

    print(f"  Added {added} TOC-only entries (not covered by event pages)")

    # ── Assign IDs and normalise ──────────────────────────────────────────
    for rec in all_events:
        rec["pdf_event_id"]          = assign_id(rec["event_name_raw"], rec["year"], rec["source_page"])
        rec["normalized_event_name"] = normalize_name(rec["event_name_raw"])
        rec["normalized_location"]   = normalize_location(rec["location_raw"])

    # ── Write output ─────────────────────────────────────────────────────
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_events)

    print(f"\nWrote {len(all_events)} candidates → {OUT_CSV.relative_to(ROOT)}")

    # Stats
    from collections import Counter
    by_year = Counter(e["year"] for e in all_events)
    by_conf = Counter(e["extraction_confidence"] for e in all_events)
    no_name = sum(1 for e in all_events if not e["event_name_raw"])
    no_year = sum(1 for e in all_events if not e["year"])

    print(f"\nExtraction confidence: {dict(by_conf)}")
    print(f"Missing name: {no_name}  |  Missing year: {no_year}")
    years_with_data = sorted(y for y in by_year if y)
    if years_with_data:
        print(f"Year range: {years_with_data[0]} – {years_with_data[-1]}")
    print(f"\nEvents per year:")
    for y in years_with_data:
        print(f"  {y}: {by_year[y]}")
    if "" in by_year:
        print(f"  (no year): {by_year['']}")


if __name__ == "__main__":
    main()
