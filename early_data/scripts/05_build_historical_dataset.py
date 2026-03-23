#!/usr/bin/env python3
"""
05_build_historical_dataset.py — Build multi-layer historical dataset.

Reads:
  early_data/review/gemini_batch_*.json
  inputs/OLD_RESULTS.txt

Produces (8 outputs):
  early_data/event_blocks/event_blocks.csv              (Gemini, in-scope)
  early_data/placements/placements_flat.csv             (Gemini, in-scope)
  early_data/old_results/old_results_event_blocks.csv
  early_data/old_results/old_results_placements_flat.csv
  early_data/canonical/event_groups.csv
  early_data/canonical/canonical_events.csv
  early_data/canonical/event_id_mapping.csv
  early_data/canonical/event_source_comparison.csv

Design principles:
  - No normalization or deduplication of raw values
  - Full provenance: source_file always populated
  - event_id = SHA-1(source_file + "|" + event_name_raw)[:10] — stable across re-runs
  - group_id = SHA-1(normalized_event_type + "|" + year)[:10]
  - validation_status is a label only; conflicts are never resolved automatically
"""

import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Input paths
REVIEW_DIR  = REPO_ROOT / "early_data" / "review"
OLD_RESULTS = REPO_ROOT / "inputs" / "OLD_RESULTS.txt"

# Output paths
EVENT_BLOCKS_CSV  = REPO_ROOT / "early_data" / "event_blocks" / "event_blocks.csv"
PLACEMENTS_CSV    = REPO_ROOT / "early_data" / "placements" / "placements_flat.csv"
OR_BLOCKS_CSV     = REPO_ROOT / "early_data" / "old_results" / "old_results_event_blocks.csv"
OR_PLACEMENTS_CSV = REPO_ROOT / "early_data" / "old_results" / "old_results_placements_flat.csv"
EVENT_GROUPS_CSV  = REPO_ROOT / "early_data" / "canonical" / "event_groups.csv"
CANONICAL_CSV     = REPO_ROOT / "early_data" / "canonical" / "canonical_events.csv"
ID_MAPPING_CSV    = REPO_ROOT / "early_data" / "canonical" / "event_id_mapping.csv"
COMPARISON_CSV    = REPO_ROOT / "early_data" / "canonical" / "event_source_comparison.csv"

PRE1997_CUTOFF = 1997
NOISE_CHAR = "\ufffd"

# ---------------------------------------------------------------------------
# Controlled vocabulary: normalized event type
# Rules applied in order — first match wins.
# ---------------------------------------------------------------------------
EVENT_TYPE_RULES = [
    ("IFAB World",                "IFAB_WORLD_CHAMPIONSHIPS"),
    ("WFA World",                 "WFA_WORLD_CHAMPIONSHIPS"),
    ("WFA National",              "WFA_NATIONALS"),
    ("NHSA National",             "NHSA_NATIONALS"),
    ("World Footbag",             "WORLD_CHAMPIONSHIPS"),
    ("World Championships",       "WORLD_CHAMPIONSHIPS"),
    ("National Footbag",          "US_NATIONALS"),
    ("U.S. National",             "US_NATIONALS"),
    ("European Footbag",          "EURO_CHAMPIONSHIPS"),
    ("Western Regionals",         "US_REGIONALS"),
    ("Oregon State",              "STATE_CHAMPIONSHIPS"),
    ("Mike Marshall",             "OTHER"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_event_id(source_file: str, event_name_raw: str) -> str:
    return hashlib.sha1(f"{source_file}|{event_name_raw}".encode()).hexdigest()[:10]


def make_group_id(normalized_event_type: str, year: str) -> str:
    return hashlib.sha1(f"{normalized_event_type}|{year}".encode()).hexdigest()[:10]


def classify_source_type(source_file: str) -> str:
    sf = source_file.upper()
    if "IFAB" in sf:
        return "IFAB"
    if sf == "OLD_RESULTS.TXT":
        return "OLD_RESULTS"
    return "FBW"


def extract_year(date_raw: str) -> str:
    m = re.search(r"\b([12][09]\d{2})\b", date_raw)
    return m.group(1) if m else ""


def normalize_event_type(event_name_raw: str, year: str = "",
                          is_old_results: bool = False) -> str:
    """Map event_name_raw to a controlled vocabulary type."""
    for substring, etype in EVENT_TYPE_RULES:
        if substring.lower() in event_name_raw.lower():
            return etype

    if is_old_results:
        yr = int(year) if year else 0
        if re.search(r"\bNHSA\b", event_name_raw, re.IGNORECASE):
            return "NHSA_NATIONALS"
        if re.search(r"\bWFA\b", event_name_raw, re.IGNORECASE):
            # WFA hosted Nationals 1983-1985, World Championships 1986-1992
            return "WFA_WORLD_CHAMPIONSHIPS" if yr >= 1986 else "WFA_NATIONALS"
        if re.match(r"^\d{4}$", event_name_raw.strip()):
            # Bare year (no org label) — section 2 "1984" / "1985"
            return "WORLD_CHAMPIONSHIPS"

    return "OTHER"


def write_csv(path: Path, fields: list, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Source 1: Gemini batch JSON processing
# ---------------------------------------------------------------------------

def process_gemini_batches():
    """
    Process all gemini_batch_*.json files.
    Returns (in_scope_events, in_scope_placements) — both lists of dicts.
    Out-of-scope events (year >= 1997) are excluded from both outputs.
    """
    batch_files = sorted(REVIEW_DIR.glob("gemini_batch_*.json"))
    if not batch_files:
        print(f"ERROR: No gemini_batch_*.json files found in {REVIEW_DIR}",
              file=sys.stderr)
        sys.exit(1)

    print(f"Gemini: found {len(batch_files)} batch files")

    event_rows = []
    placement_rows = []
    seen: dict = {}  # (source_file, event_name_raw) -> batch_name

    for batch_path in batch_files:
        with open(batch_path, encoding="utf-8") as fh:
            pages = json.load(fh)

        for page in pages:
            sf    = page.get("source_file", "")
            stype = classify_source_type(sf)

            for event in page.get("events", []):
                enr   = event.get("event_name_raw", "")
                dr    = event.get("date_raw", "")
                lr    = event.get("location_raw", "")
                year  = extract_year(dr)
                eid   = make_event_id(sf, enr)
                key   = (sf, enr)

                if key in seen:
                    print(f"  WARN: duplicate ({sf!r}, {enr!r}) "
                          f"in {batch_path.name} (first in {seen[key]})")
                else:
                    seen[key] = batch_path.name

                exclude = "TRUE" if (year and int(year) >= PRE1997_CUTOFF) else ""
                ntype   = normalize_event_type(enr, year=year)

                event_rows.append({
                    "event_id":              eid,
                    "event_name_raw":        enr,
                    "year":                  year,
                    "date_raw":              dr,
                    "location_raw":          lr,
                    "source_file":           sf,
                    "source_type":           stype,
                    "normalized_event_type": ntype,
                    "exclude_pre1997":       exclude,
                })

                for div in event.get("divisions", []):
                    dr2 = div.get("division_raw", "")
                    for res in div.get("results", []):
                        placement_rows.append({
                            "event_id":      eid,
                            "division_raw":  dr2,
                            "placement_raw": res.get("placement_raw", ""),
                            "placement_num": res.get("placement_num", ""),
                            "player_raw":    res.get("player_raw", ""),
                            "team_raw":      res.get("team_raw", ""),
                            "score_raw":     res.get("score_raw", ""),
                            "notes":         res.get("notes", ""),
                            "source_file":   sf,
                        })

    oos_ids       = {r["event_id"] for r in event_rows if r["exclude_pre1997"] == "TRUE"}
    in_scope_ev   = [r for r in event_rows if r["exclude_pre1997"] != "TRUE"]
    in_scope_plc  = [r for r in placement_rows if r["event_id"] not in oos_ids]

    print(f"  In-scope events: {len(in_scope_ev)}, placements: {len(in_scope_plc)}")
    print(f"  Out-of-scope:    {len(oos_ids)} events")
    return in_scope_ev, in_scope_plc


# ---------------------------------------------------------------------------
# Source 2: OLD_RESULTS.txt parsing
# ---------------------------------------------------------------------------

WORLD_RECORD_RE = re.compile(
    r"\s*[-–]\s*World\s+Record\s*[-–]\s*[\d,]+", re.IGNORECASE
)
EVENT_HEADER_RE = re.compile(
    r"^(\d{4})(?:\s+(NHSA|WFA))?\s*:?\s*$", re.IGNORECASE
)


def strip_noise(line: str) -> str:
    """Replace U+FFFD noise chars with a space (they often replaced spaces in source),
    collapse runs of whitespace, and strip leading/trailing whitespace."""
    cleaned = line.replace(NOISE_CHAR, " ")
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return cleaned.strip()


def strip_world_record(text: str) -> str:
    return WORLD_RECORD_RE.sub("", text).strip().rstrip(",").strip()


def parse_inline_placements(text: str) -> list:
    """
    Parse 'Nth - Name, Mth - Name' inline placement strings.
    Handles: commas between placements, spaces between placements (after WR strip),
    world record annotations.
    Returns list of (placement_num_str, player_raw_str).
    """
    text = strip_world_record(text)
    results = []
    # Find all ordinal markers: "Nth - " (suffix required)
    pattern = re.compile(r"(\d+)(?:st|nd|rd|th)\s*[-–]\s*", re.IGNORECASE)
    positions = [(m.start(), m.end(), m.group(1)) for m in pattern.finditer(text)]
    for i, (start, end, pnum) in enumerate(positions):
        end_pos = positions[i + 1][0] if i + 1 < len(positions) else len(text)
        player = text[end:end_pos].strip().rstrip(",").strip()
        player = strip_world_record(player)
        if player:
            results.append((pnum, player))
    return results


def merge_continuation_lines(lines: list) -> list:
    """
    Join indented continuation lines onto the previous non-empty line.
    An indented line is one whose first character is a space or tab.
    """
    merged = []
    for raw in lines:
        cleaned = raw.replace(NOISE_CHAR, " ")
        cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
        stripped = cleaned.strip()
        if not stripped:
            merged.append("")
            continue
        if cleaned and cleaned[0] in (" ", "\t"):
            # Continuation: append to previous non-empty merged line
            if merged:
                prev = merged[-1]
                if prev:
                    merged[-1] = prev.rstrip() + " " + stripped
                    continue
        merged.append(stripped)
    return merged


def make_placement_row(event_name: str, division: str, pnum: str,
                       player: str) -> dict | None:
    player = strip_world_record(player).strip().rstrip(",").strip()
    if not player or not event_name:
        return None
    return {
        "event_id":      make_event_id("OLD_RESULTS.txt", event_name),
        "division_raw":  division,
        "placement_raw": pnum,
        "placement_num": pnum,
        "player_raw":    player,
        "team_raw":      "",
        "score_raw":     "",
        "notes":         "",
        "source_file":   "OLD_RESULTS.txt",
    }


def parse_section1(lines: list) -> tuple:
    """
    Parse Section 1: 'Freestyle World Championships Results 1982-1986'.
    Returns (event_rows, placement_rows).
    """
    event_rows = []
    placement_rows = []
    current_event = None
    current_div = ""

    for raw in lines:
        line = strip_noise(raw)
        if not line or "Freestyle World Championships" in line:
            continue

        # Event header: "1983 NHSA:" or "1984 WFA:"
        m = EVENT_HEADER_RE.match(line)
        if m:
            year = m.group(1)
            org  = (m.group(2) or "").upper()
            name = f"{year} {org}".strip() if org else year
            current_event = name
            current_div   = ""
            ntype = normalize_event_type(name, year=year, is_old_results=True)
            event_rows.append({
                "event_id":              make_event_id("OLD_RESULTS.txt", name),
                "event_name_raw":        name,
                "year":                  year,
                "date_raw":              year,
                "location_raw":          "",
                "source_file":           "OLD_RESULTS.txt",
                "source_type":           "OLD_RESULTS",
                "normalized_event_type": ntype,
                "exclude_pre1997":       "TRUE" if int(year) >= PRE1997_CUTOFF else "",
            })
            continue

        if current_event is None:
            continue

        # Division header: "Singles:" or "Team:"
        if re.match(r"^(Singles|Team)\s*:?\s*$", line, re.IGNORECASE):
            current_div = re.match(r"^(\w+)", line).group(1)
            continue

        # Placement line: "Nth - Player"
        m = re.match(r"(\d+)(?:st|nd|rd|th)?\s*[-–]\s*(.+)$", line, re.IGNORECASE)
        if m:
            row = make_placement_row(current_event, current_div,
                                     m.group(1), m.group(2))
            if row:
                placement_rows.append(row)

    return event_rows, placement_rows


def parse_section2(lines: list) -> tuple:
    """
    Parse Section 2: '1980-1985 World Championships Results'.
    Returns (event_rows, placement_rows).
    """
    event_rows = []
    placement_rows = []
    current_event = None

    merged = merge_continuation_lines(lines)

    for line in merged:
        if not line or "1980-1985 World Championships" in line:
            continue

        # Event header
        m = EVENT_HEADER_RE.match(line)
        if m:
            year = m.group(1)
            org  = (m.group(2) or "").upper()
            name = f"{year} {org}".strip() if org else year
            current_event = name
            ntype = normalize_event_type(name, year=year, is_old_results=True)
            event_rows.append({
                "event_id":              make_event_id("OLD_RESULTS.txt", name),
                "event_name_raw":        name,
                "year":                  year,
                "date_raw":              year,
                "location_raw":          "",
                "source_file":           "OLD_RESULTS.txt",
                "source_type":           "OLD_RESULTS",
                "normalized_event_type": ntype,
                "exclude_pre1997":       "TRUE" if int(year) >= PRE1997_CUTOFF else "",
            })
            continue

        if current_event is None:
            continue

        # "Division Champions? - Name" (champion, no ordinal)
        m = re.match(r"^(.+?)\s+Champions?\s*[-–]\s*(.+)$", line, re.IGNORECASE)
        if m:
            division = m.group(1).strip()
            player   = strip_world_record(m.group(2)).strip()
            row = make_placement_row(current_event, division, "1", player)
            if row:
                placement_rows.append(row)
            continue

        # "Division - 1st - Name", "Division: 1st - Name", or "Division 1st - Name" inline
        # Lazy group 1 handles dashes inside division names (e.g. "One-Pass")
        # Separator is optional: some lines have no dash between div name and ordinal
        m = re.match(r"^(.+?)\s*[:\-–]?\s*(\d+(?:st|nd|rd|th)\s*[-–]\s*.+)$", line)
        if m:
            division_candidate = m.group(1).strip()
            # Reject if "division" starts with a digit (it's actually a bare placement)
            if not re.match(r"^\d", division_candidate):
                placements = parse_inline_placements(m.group(2))
                for pnum, player in placements:
                    row = make_placement_row(current_event, division_candidate,
                                             pnum, player)
                    if row:
                        placement_rows.append(row)
                continue

        # "Division -" line ending (continuation expected on next merged line)
        # After merge_continuation_lines this should already be resolved,
        # but catch bare division-only lines just in case
        if re.match(r"^[A-Za-z].+[-–]\s*$", line):
            # This division had no placements on same line — nothing to capture
            continue

    return event_rows, placement_rows


def process_old_results():
    """
    Parse OLD_RESULTS.txt → event_rows, placement_rows.
    Deduplicates event_rows by event_id (first-seen wins).
    Section 2 is more comprehensive; both sections are parsed and combined.
    """
    with open(OLD_RESULTS, encoding="utf-8", errors="replace") as fh:
        raw_lines = fh.readlines()

    # Split on "///" separator
    sep_idx = None
    for i, line in enumerate(raw_lines):
        if line.strip().replace(NOISE_CHAR, "") == "///":
            sep_idx = i
            break

    if sep_idx is None:
        print("WARN: OLD_RESULTS.txt: '///' separator not found; treating all as section 2")
        sec1_lines, sec2_lines = [], raw_lines
    else:
        sec1_lines = raw_lines[:sep_idx]
        sec2_lines = raw_lines[sep_idx + 1:]

    ev1, pl1 = parse_section1(sec1_lines)
    ev2, pl2 = parse_section2(sec2_lines)

    # Deduplicate event rows by event_id (section 1 first, section 2 fills gaps)
    seen_ids: set = set()
    event_rows = []
    for er in ev1 + ev2:
        if er["event_id"] not in seen_ids:
            seen_ids.add(er["event_id"])
            event_rows.append(er)

    # Placements: combine both sections (different divisions, no exact duplicates)
    placement_rows = pl1 + pl2

    in_scope_ev  = [r for r in event_rows if r["exclude_pre1997"] != "TRUE"]
    in_scope_plc = [r for r in placement_rows
                    if r["event_id"] in {e["event_id"] for e in in_scope_ev}]

    print(f"OLD_RESULTS: {len(in_scope_ev)} in-scope events, "
          f"{len(in_scope_plc)} placements")
    if any(r["exclude_pre1997"] == "TRUE" for r in event_rows):
        oos = [r for r in event_rows if r["exclude_pre1997"] == "TRUE"]
        print(f"  (excluded as out-of-scope: {[r['event_name_raw'] for r in oos]})")

    return in_scope_ev, in_scope_plc


# ---------------------------------------------------------------------------
# Canonical layer: grouping and comparison
# ---------------------------------------------------------------------------

def build_canonical_outputs(gemini_events: list, gemini_placements: list,
                             or_events: list, or_placements: list):
    """
    Build the four canonical output tables from all sources combined.
    """
    all_events = gemini_events + or_events
    all_placements = gemini_placements + or_placements

    # Placement count per event_id
    placement_counts: dict = defaultdict(int)
    for p in all_placements:
        placement_counts[p["event_id"]] += 1

    # Group events by (normalized_event_type, year)
    groups: dict = defaultdict(list)
    ungrouped = []
    for er in all_events:
        ntype = er.get("normalized_event_type", "")
        year  = er.get("year", "")
        if ntype and year:
            gid = make_group_id(ntype, year)
            groups[gid].append(er)
        else:
            ungrouped.append(er)

    event_groups_rows = []
    canonical_rows    = []
    id_mapping_rows   = []
    comparison_rows   = []

    for gid, members in sorted(groups.items(),
                                key=lambda kv: (kv[1][0]["year"],
                                                kv[1][0]["normalized_event_type"])):
        ntype = members[0]["normalized_event_type"]
        year  = members[0]["year"]

        source_types = sorted({m["source_type"] for m in members})
        num_sources  = len(members)
        num_st       = len(source_types)

        # Confidence: HIGH if 2+ independent source types, MEDIUM if 2+ same type,
        # LOW if single OLD_RESULTS entry
        if num_st >= 2:
            confidence = "HIGH"
        elif num_sources >= 2:
            confidence = "MEDIUM"
        elif source_types == ["OLD_RESULTS"]:
            confidence = "LOW"
        else:
            confidence = "MEDIUM"

        for m in members:
            event_groups_rows.append({
                "group_id":              gid,
                "event_id":              m["event_id"],
                "normalized_event_type": ntype,
                "year":                  year,
                "event_name_raw":        m["event_name_raw"],
                "source_file":           m["source_file"],
                "source_type":           m["source_type"],
                "location_raw":          m["location_raw"],
                "num_placements":        placement_counts[m["event_id"]],
                "confidence":            confidence,
            })
            id_mapping_rows.append({
                "event_id":           m["event_id"],
                "event_name_raw":     m["event_name_raw"],
                "source_file":        m["source_file"],
                "source_type":        m["source_type"],
                "canonical_event_id": gid,
                "group_id":           gid,
            })

        # Pick canonical representative: most placements; break tie by source priority
        SOURCE_PRIORITY = {"FBW": 0, "IFAB": 1, "OLD_RESULTS": 2}
        best = max(members, key=lambda m: (
            placement_counts[m["event_id"]],
            -SOURCE_PRIORITY.get(m["source_type"], 9),
        ))

        # Conflict detection: multiple non-empty, non-identical locations
        locs = {m["location_raw"].lower().strip()
                for m in members if m["location_raw"].strip()}
        has_loc_conflict = len(locs) > 1

        if num_st >= 2:
            vstatus = "CONFLICT" if has_loc_conflict else "CONFIRMED_MULTI_SOURCE"
        else:
            vstatus = "SINGLE_SOURCE"

        conflict_notes = ""
        if has_loc_conflict:
            conflict_notes = "location: " + " | ".join(sorted(locs))

        canonical_rows.append({
            "canonical_event_id":    gid,
            "event_name_raw":        best["event_name_raw"],
            "year":                  year,
            "location_raw":          best["location_raw"],
            "normalized_event_type": ntype,
            "group_id":              gid,
            "num_sources":           num_sources,
            "num_source_types":      num_st,
            "source_types":          "|".join(source_types),
            "primary_source_type":   best["source_type"],
            "validation_status":     vstatus,
            "confidence":            confidence,
            "num_placements":        placement_counts[best["event_id"]],
        })

        source_summary = "; ".join(
            f"{m['source_type']}:{m['event_name_raw']}" for m in members
        )
        comparison_rows.append({
            "group_id":              gid,
            "normalized_event_type": ntype,
            "year":                  year,
            "validation_status":     vstatus,
            "conflict_notes":        conflict_notes,
            "num_sources":           num_sources,
            "source_types":          "|".join(source_types),
            "sources":               source_summary,
        })

    # Ungrouped events still get id_mapping entries (canonical_event_id = own event_id)
    for er in ungrouped:
        id_mapping_rows.append({
            "event_id":           er["event_id"],
            "event_name_raw":     er["event_name_raw"],
            "source_file":        er["source_file"],
            "source_type":        er["source_type"],
            "canonical_event_id": er["event_id"],
            "group_id":           "",
        })

    return event_groups_rows, canonical_rows, id_mapping_rows, comparison_rows


# ---------------------------------------------------------------------------
# Field schemas
# ---------------------------------------------------------------------------

BLOCK_FIELDS = [
    "event_id", "event_name_raw", "year", "date_raw", "location_raw",
    "source_file", "source_type", "normalized_event_type", "exclude_pre1997",
]
PLACEMENT_FIELDS = [
    "event_id", "division_raw", "placement_raw", "placement_num",
    "player_raw", "team_raw", "score_raw", "notes", "source_file",
]
EVENT_GROUPS_FIELDS = [
    "group_id", "event_id", "normalized_event_type", "year",
    "event_name_raw", "source_file", "source_type", "location_raw",
    "num_placements", "confidence",
]
CANONICAL_FIELDS = [
    "canonical_event_id", "event_name_raw", "year", "location_raw",
    "normalized_event_type", "group_id", "num_sources", "num_source_types",
    "source_types", "primary_source_type", "validation_status", "confidence",
    "num_placements",
]
ID_MAPPING_FIELDS = [
    "event_id", "event_name_raw", "source_file", "source_type",
    "canonical_event_id", "group_id",
]
COMPARISON_FIELDS = [
    "group_id", "normalized_event_type", "year", "validation_status",
    "conflict_notes", "num_sources", "source_types", "sources",
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=== 05_build_historical_dataset.py ===\n")

    # Source 1: Gemini batches
    gemini_events, gemini_placements = process_gemini_batches()

    # Source 2: OLD_RESULTS.txt
    or_events, or_placements = process_old_results()

    # Write source-specific outputs
    write_csv(EVENT_BLOCKS_CSV,  BLOCK_FIELDS,     gemini_events)
    write_csv(PLACEMENTS_CSV,    PLACEMENT_FIELDS, gemini_placements)
    write_csv(OR_BLOCKS_CSV,     BLOCK_FIELDS,     or_events)
    write_csv(OR_PLACEMENTS_CSV, PLACEMENT_FIELDS, or_placements)

    print(f"\nWrote:")
    print(f"  {EVENT_BLOCKS_CSV.name}  ({len(gemini_events)} rows)")
    print(f"  {PLACEMENTS_CSV.name}  ({len(gemini_placements)} rows)")
    print(f"  {OR_BLOCKS_CSV.name}  ({len(or_events)} rows)")
    print(f"  {OR_PLACEMENTS_CSV.name}  ({len(or_placements)} rows)")

    # Canonical layer
    eg_rows, can_rows, idmap_rows, cmp_rows = build_canonical_outputs(
        gemini_events, gemini_placements, or_events, or_placements
    )

    write_csv(EVENT_GROUPS_CSV, EVENT_GROUPS_FIELDS, eg_rows)
    write_csv(CANONICAL_CSV,    CANONICAL_FIELDS,    can_rows)
    write_csv(ID_MAPPING_CSV,   ID_MAPPING_FIELDS,   idmap_rows)
    write_csv(COMPARISON_CSV,   COMPARISON_FIELDS,   cmp_rows)

    print(f"  {EVENT_GROUPS_CSV.name}  ({len(eg_rows)} rows)")
    print(f"  {CANONICAL_CSV.name}  ({len(can_rows)} groups)")
    print(f"  {ID_MAPPING_CSV.name}  ({len(idmap_rows)} rows)")
    print(f"  {COMPARISON_CSV.name}  ({len(cmp_rows)} rows)")

    # Summary: validation status breakdown
    vstatus_counts: dict = defaultdict(int)
    for r in cmp_rows:
        vstatus_counts[r["validation_status"]] += 1
    print(f"\nValidation status summary:")
    for status in ["CONFIRMED_MULTI_SOURCE", "SINGLE_SOURCE", "CONFLICT"]:
        print(f"  {status}: {vstatus_counts.get(status, 0)}")

    # List any conflicts
    conflicts = [r for r in cmp_rows if r["validation_status"] == "CONFLICT"]
    if conflicts:
        print(f"\nConflicts ({len(conflicts)}):")
        for c in conflicts:
            print(f"  {c['year']} {c['normalized_event_type']}: {c['conflict_notes']}")
            print(f"    sources: {c['sources']}")

    # List CONFIRMED_MULTI_SOURCE events
    confirmed = [r for r in cmp_rows if r["validation_status"] == "CONFIRMED_MULTI_SOURCE"]
    if confirmed:
        print(f"\nConfirmed multi-source ({len(confirmed)}):")
        for c in confirmed:
            print(f"  {c['year']} {c['normalized_event_type']}: {c['sources']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
