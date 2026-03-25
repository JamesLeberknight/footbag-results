#!/usr/bin/env python3
"""
tools/compare_pdf_events_to_current.py

Compare PDF-extracted event candidates against the canonical events dataset.

Matching strategy (multi-pass):
  1. MATCHED_STRONG  — strong name + year match
  2. MATCHED_POSSIBLE — year match + fuzzy name similarity ≥ 0.75
  3. NEW_EVENT_CANDIDATE — no match found in canonical dataset
  4. RICHER_EXISTING_EVENT — matched event where PDF has metadata not in canonical

Outputs:
  out/pdf_compare/pdf_vs_current_event_comparison.csv  — all PDF events with match status
  out/pdf_compare/new_event_candidates.csv             — unmatched events
  out/pdf_compare/richer_existing_events.csv           — matched events with new metadata
  out/pdf_compare/comparison_summary.md                — narrative summary

Usage:
  python tools/compare_pdf_events_to_current.py
"""

import csv
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

ROOT          = Path(__file__).resolve().parents[1]
PDF_CSV       = ROOT / "out" / "pdf_compare" / "pdf_event_candidates.csv"
CANONICAL_CSV = ROOT / "out" / "canonical" / "events.csv"
OUT_DIR       = ROOT / "out" / "pdf_compare"

COMPARISON_CSV = OUT_DIR / "pdf_vs_current_event_comparison.csv"
NEW_EVENTS_CSV = OUT_DIR / "new_event_candidates.csv"
RICHER_CSV     = OUT_DIR / "richer_existing_events.csv"
SUMMARY_MD     = OUT_DIR / "comparison_summary.md"


# ── Name normalisation ────────────────────────────────────────────────────────

# Noise words to strip for matching
NOISE_WORDS = re.compile(
    r'\b(?:footbag|the|of|de|du|la|le|les|and|&|for|a|an|'
    r'championship|championships|annual|ifpa|open|cup|jam|'
    r'tournament|challenge|circuit|classic|invitational)\b',
    re.IGNORECASE,
)

ORDINAL_RE = re.compile(r'\b\d+(?:st|nd|rd|th)?\b')


def norm(s: str) -> str:
    """Normalise event name for comparison."""
    s = s.lower()
    # Remove non-alphanumeric except spaces
    s = re.sub(r"[^\w\s]", " ", s)
    # Remove ordinals
    s = ORDINAL_RE.sub("", s)
    # Remove noise words
    s = NOISE_WORDS.sub(" ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_location(s: str) -> str:
    """Normalise location string."""
    s = s.lower()
    s = re.sub(r"[^\w\s,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio on normalised names."""
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


# ── Known event name aliases ──────────────────────────────────────────────────
# Some PDF names differ significantly from canonical names in known ways

PDF_ALIASES = {
    # PDF name fragment → canonical search term
    "world footbag championships": "worlds",
    "ifpa world footbag championships": "worlds",
    "32nd ifpa world footbag championships": "worlds",
    "world footbag":  "worlds",
    "european footbag championships": "european championships",
    "euro footbag championships": "european championships",
    "south american footbag championships": "south american championships",
    "us open footbag championships": "us open",
    "east coast footbag championships": "east coast championships",
}


def resolve_alias(name: str) -> str:
    """Apply known aliases for better matching."""
    nl = name.lower()
    for k, v in PDF_ALIASES.items():
        if k in nl:
            return v
    return name


# ── Load data ─────────────────────────────────────────────────────────────────

def load_pdf_events(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # Filter to high-confidence event pages (skip TOC and ad pages)
    return [r for r in rows if r.get("extraction_confidence") == "high"]


def load_canonical_events(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_canonical_index(canonical: list[dict]) -> dict:
    """
    Build a lookup: year → list of canonical events.
    Also builds: (year, norm_name) → canonical event for exact-ish matches.
    """
    by_year: dict[str, list[dict]] = defaultdict(list)
    for ev in canonical:
        yr = str(ev.get("year", "")).strip()
        if yr:
            by_year[yr].append(ev)
    return by_year


# ── Matching ──────────────────────────────────────────────────────────────────

STRONG_THRESHOLD  = 0.85
POSSIBLE_THRESHOLD = 0.60


def match_event(pdf_ev: dict, by_year: dict) -> tuple[str, dict | None, float]:
    """
    Try to match a PDF event to a canonical event.
    Returns (match_type, best_canonical, score)
    match_type: MATCHED_STRONG | MATCHED_POSSIBLE | NEW_EVENT_CANDIDATE
    """
    year = pdf_ev.get("year", "").strip()
    pdf_name = pdf_ev.get("event_name_raw", "").strip()
    pdf_name_resolved = resolve_alias(pdf_name)

    candidates = by_year.get(year, [])

    if not candidates:
        return "NEW_EVENT_CANDIDATE", None, 0.0

    best_score = 0.0
    best_match = None

    for canon in candidates:
        canon_name = canon.get("event_name", "")
        score = max(
            similarity(pdf_name, canon_name),
            similarity(pdf_name_resolved, canon_name),
        )
        if score > best_score:
            best_score = score
            best_match = canon

    if best_score >= STRONG_THRESHOLD:
        return "MATCHED_STRONG", best_match, best_score
    elif best_score >= POSSIBLE_THRESHOLD:
        return "MATCHED_POSSIBLE", best_match, best_score
    else:
        return "NEW_EVENT_CANDIDATE", None, best_score


# ── Richer-metadata detection ─────────────────────────────────────────────────

def pdf_has_richer_metadata(pdf_ev: dict, canon_ev: dict) -> list[str]:
    """
    Return list of enrichments where PDF has data not in or more specific than canonical.
    """
    enrichments = []

    # Host club missing in canonical
    pdf_host = pdf_ev.get("host_club_raw", "").strip()
    canon_host = canon_ev.get("host_club", "").strip()
    if pdf_host and not canon_host:
        enrichments.append(f"host_club: {pdf_host}")

    # Start date missing in canonical
    pdf_date = pdf_ev.get("date_raw", "").strip()
    canon_date = canon_ev.get("start_date", "").strip()
    if pdf_date and not canon_date:
        enrichments.append(f"date: {pdf_date}")

    # Venue-level location (PDF has venue/address, canonical only has city)
    pdf_loc = pdf_ev.get("location_raw", "").strip()
    canon_city = canon_ev.get("city", "").strip()
    if pdf_loc and canon_city:
        # Check if PDF location is MORE specific than city (not just the city name)
        pdf_loc_lower = pdf_loc.lower()
        canon_city_lower = canon_city.lower()
        # PDF is more specific if it contains venue-type words OR the city is only
        # a substring of a longer PDF location string with extra detail
        is_venue = bool(re.search(
            r'\b(?:park|gym|gymnasium|hall|arena|school|centre|center|university|'
            r'stadio|stade|cancha|halle|palais|salle|sportif|sportska|sportshall|'
            r'parc|parque|complex|plaza|street|avenue|rue|calle|blvd|dr\.?|'
            r'str\.|strasse|str\b)\b',
            pdf_loc_lower
        ))
        is_more_specific = (len(pdf_loc) > len(canon_city) + 5 and
                            not pdf_loc_lower.startswith("see ") and
                            not pdf_loc_lower.startswith("t.b") and
                            not pdf_loc_lower.startswith("tba"))
        if is_venue or is_more_specific:
            enrichments.append(f"venue: {pdf_loc[:100]}")

    return enrichments


# ── Main ──────────────────────────────────────────────────────────────────────

COMPARISON_FIELDS = [
    "pdf_event_id", "source_page", "year",
    "event_name_raw", "date_raw", "location_raw", "host_club_raw",
    "divisions_raw", "extraction_confidence",
    "match_type", "match_score",
    "matched_event_key", "matched_event_name", "matched_year",
    "matched_city", "matched_country", "matched_host_club", "matched_date",
    "enrichments",
]

NEW_EVENTS_FIELDS = [
    "pdf_event_id", "source_page", "year",
    "event_name_raw", "date_raw", "location_raw", "host_club_raw",
    "divisions_raw", "extraction_confidence", "best_score",
    "notes",
]

RICHER_FIELDS = [
    "pdf_event_id", "source_page", "year",
    "event_name_raw", "date_raw", "location_raw", "host_club_raw",
    "match_type", "match_score",
    "matched_event_key", "matched_event_name",
    "matched_city", "matched_country", "matched_host_club", "matched_date",
    "enrichments",
]


def main():
    for p in [PDF_CSV, CANONICAL_CSV]:
        if not p.exists():
            print(f"ERROR: not found: {p}")
            sys.exit(1)

    print("Loading data ...")
    pdf_events  = load_pdf_events(PDF_CSV)
    canonical   = load_canonical_events(CANONICAL_CSV)
    by_year     = build_canonical_index(canonical)

    print(f"  PDF events (high-confidence): {len(pdf_events)}")
    print(f"  Canonical events: {len(canonical)}")
    print(f"  Canonical year range: {min(e['year'] for e in canonical if e['year'])} – {max(e['year'] for e in canonical if e['year'])}")

    # ── Match every PDF event ─────────────────────────────────────────────
    print("\nMatching PDF events to canonical ...")
    comparison_rows = []
    new_events      = []
    richer_events   = []

    counts = {"MATCHED_STRONG": 0, "MATCHED_POSSIBLE": 0, "NEW_EVENT_CANDIDATE": 0}

    for pdf_ev in pdf_events:
        match_type, best_canon, score = match_event(pdf_ev, by_year)
        counts[match_type] += 1

        row = {
            "pdf_event_id":     pdf_ev["pdf_event_id"],
            "source_page":      pdf_ev["source_page"],
            "year":             pdf_ev["year"],
            "event_name_raw":   pdf_ev["event_name_raw"],
            "date_raw":         pdf_ev["date_raw"],
            "location_raw":     pdf_ev["location_raw"],
            "host_club_raw":    pdf_ev["host_club_raw"],
            "divisions_raw":    pdf_ev["divisions_raw"],
            "extraction_confidence": pdf_ev["extraction_confidence"],
            "match_type":       match_type,
            "match_score":      f"{score:.3f}",
            "matched_event_key":   best_canon["event_key"]   if best_canon else "",
            "matched_event_name":  best_canon["event_name"]  if best_canon else "",
            "matched_year":        best_canon["year"]         if best_canon else "",
            "matched_city":        best_canon.get("city","")  if best_canon else "",
            "matched_country":     best_canon.get("country","") if best_canon else "",
            "matched_host_club":   best_canon.get("host_club","") if best_canon else "",
            "matched_date":        best_canon.get("start_date","") if best_canon else "",
            "enrichments":         "",
        }

        if match_type != "NEW_EVENT_CANDIDATE" and best_canon:
            enrichments = pdf_has_richer_metadata(pdf_ev, best_canon)
            row["enrichments"] = " | ".join(enrichments)
            if enrichments:
                richer_events.append(dict(row))

        if match_type == "NEW_EVENT_CANDIDATE":
            new_events.append({
                "pdf_event_id":   pdf_ev["pdf_event_id"],
                "source_page":    pdf_ev["source_page"],
                "year":           pdf_ev["year"],
                "event_name_raw": pdf_ev["event_name_raw"],
                "date_raw":       pdf_ev["date_raw"],
                "location_raw":   pdf_ev["location_raw"],
                "host_club_raw":  pdf_ev["host_club_raw"],
                "divisions_raw":  pdf_ev["divisions_raw"],
                "extraction_confidence": pdf_ev["extraction_confidence"],
                "best_score":     f"{score:.3f}",
                "notes":          "",
            })

        comparison_rows.append(row)

    # ── Write outputs ─────────────────────────────────────────────────────
    def write_csv(path: Path, fields: list, rows: list):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"  Wrote {len(rows)} rows → {path.relative_to(ROOT)}")

    print("\nWriting outputs ...")
    write_csv(COMPARISON_CSV, COMPARISON_FIELDS, comparison_rows)
    write_csv(NEW_EVENTS_CSV, NEW_EVENTS_FIELDS, new_events)
    write_csv(RICHER_CSV, RICHER_FIELDS, richer_events)

    # ── Markdown summary ──────────────────────────────────────────────────
    write_summary(pdf_events, canonical, comparison_rows, new_events, richer_events, counts)

    # ── Console summary ───────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"PDF COMPARISON SUMMARY")
    print(f"{'='*60}")
    print(f"PDF events analysed:     {len(pdf_events)}")
    print(f"Canonical events:        {len(canonical)}")
    print()
    print(f"MATCHED_STRONG:          {counts['MATCHED_STRONG']}")
    print(f"MATCHED_POSSIBLE:        {counts['MATCHED_POSSIBLE']}")
    print(f"NEW_EVENT_CANDIDATE:     {counts['NEW_EVENT_CANDIDATE']}")
    print()
    print(f"Events with richer PDF metadata: {len(richer_events)}")
    print()

    if new_events:
        print("NEW EVENT CANDIDATES (not in canonical):")
        by_year_new = defaultdict(list)
        for e in new_events:
            by_year_new[e["year"]].append(e)
        for yr in sorted(by_year_new):
            print(f"\n  {yr}:")
            for e in by_year_new[yr]:
                print(f"    [{e['best_score']}] {e['event_name_raw']}")
                if e["location_raw"]:
                    print(f"         loc: {e['location_raw']}")
                if e["host_club_raw"]:
                    print(f"         host: {e['host_club_raw']}")

    if richer_events:
        print("\nEVENTS WITH RICHER PDF METADATA (sample — top 20):")
        for e in richer_events[:20]:
            print(f"  {e['matched_event_key']} ← PDF p{e['source_page']}")
            print(f"    enrichments: {e['enrichments'][:100]}")


def write_summary(pdf_events, canonical, comparison_rows, new_events, richer_events, counts):
    from collections import Counter

    new_by_year = Counter(e["year"] for e in new_events)
    richer_by_field = Counter()
    for e in richer_events:
        for part in e["enrichments"].split(" | "):
            if part:
                field = part.split(":")[0].strip()
                richer_by_field[field] += 1

    lines = [
        "# PDF vs Canonical Event Comparison",
        "",
        f"**PDF archive:** 926-pages-results-footbag.org-2021-02-14.pdf",
        f"**PDF events extracted (high-confidence):** {len(pdf_events)}",
        f"**Canonical events:** {len(canonical)}",
        "",
        "## Match Results",
        "",
        f"| Status | Count |",
        f"|--------|-------|",
        f"| MATCHED_STRONG (≥0.85) | {counts['MATCHED_STRONG']} |",
        f"| MATCHED_POSSIBLE (0.60–0.85) | {counts['MATCHED_POSSIBLE']} |",
        f"| NEW_EVENT_CANDIDATE | {counts['NEW_EVENT_CANDIDATE']} |",
        f"| **Total** | **{len(pdf_events)}** |",
        "",
        "## New Event Candidates",
        "",
        f"**{len(new_events)} PDF events not found in canonical dataset.**",
        "",
    ]

    if new_events:
        lines.append("| Year | Event Name | Location | Host Club |")
        lines.append("|------|-----------|----------|-----------|")
        for e in sorted(new_events, key=lambda x: (x["year"], x["event_name_raw"])):
            lines.append(f"| {e['year']} | {e['event_name_raw']} | {e['location_raw']} | {e['host_club_raw']} |")
    else:
        lines.append("All PDF events matched to canonical dataset.")

    lines += [
        "",
        "## Events with Richer PDF Metadata",
        "",
        f"**{len(richer_events)} matched events where PDF has metadata not in canonical.**",
        "",
    ]

    if richer_by_field:
        lines.append("**Enrichment breakdown:**")
        lines.append("")
        for field, count in richer_by_field.most_common():
            lines.append(f"- {field}: {count} events")

    if richer_events:
        lines += [
            "",
            "**Top enrichment opportunities (first 30):**",
            "",
            "| Canonical Event | PDF Data Available |",
            "|-----------------|-------------------|",
        ]
        for e in richer_events[:30]:
            lines.append(f"| {e['matched_event_key']} | {e['enrichments'][:120]} |")

    lines += [
        "",
        "## Possible Match Review",
        "",
        f"**{counts['MATCHED_POSSIBLE']} events matched with moderate confidence.** Review recommended.",
        "",
    ]

    possible = [r for r in comparison_rows if r["match_type"] == "MATCHED_POSSIBLE"]
    if possible:
        lines.append("| Score | PDF Name | PDF Year | Canonical Match |")
        lines.append("|-------|---------|----------|----------------|")
        for r in sorted(possible, key=lambda x: -float(x["match_score"])):
            lines.append(f"| {r['match_score']} | {r['event_name_raw']} | {r['year']} | {r['matched_event_name']} |")

    lines += [
        "",
        "## Data Files",
        "",
        "| File | Description |",
        "|------|-------------|",
        "| `pdf_event_candidates.csv` | All extracted PDF events with metadata |",
        "| `pdf_vs_current_event_comparison.csv` | All PDF events with match results |",
        "| `new_event_candidates.csv` | Unmatched events (potential new data) |",
        "| `richer_existing_events.csv` | Matched events with additional PDF metadata |",
        "",
    ]

    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Wrote summary → {SUMMARY_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
