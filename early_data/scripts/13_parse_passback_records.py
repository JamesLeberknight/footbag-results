#!/usr/bin/env python3
"""
13_parse_passback_records.py — Parse passback.com records tab-delimited data

Input:  early_data/records/passback_raw_input.txt  (paste of tab-separated records table)
Output: early_data/records/records_raw_passback.csv   — one row per record entry, all source fields
        early_data/records/records_master.csv          — normalized, with record_id, unit, confidence

Source columns (from passback.com/records table):
  Trick/record name | Sort-friendly Name | Record | Place | ADDs | Player |
  Date | Approx. Date? | Video | Time/Clip # | Notes | Sort Col 1 | Sort Col 2

Optional fields (collapse when empty in space-delimited copy):
  Place, Approx. Date?, Time/Clip #, Notes

Usage:
  python early_data/scripts/13_parse_passback_records.py
"""

import csv
import re
import sys
import uuid
from pathlib import Path

ROOT   = Path(__file__).parent.parent.parent
INDIR  = ROOT / "early_data" / "records"
INFILE = INDIR / "passback_raw_input.txt"
RAW_CSV    = INDIR / "records_raw_passback.csv"
MASTER_CSV = INDIR / "records_master.csv"

# ── Parsing ──────────────────────────────────────────────────────────────────

SEP_RE = re.compile(r'    +')   # 4+ spaces = column separator in pasted output
PLACED_RE = re.compile(r'_\d+$')  # sort_col1 ends with _N → has a Place column


def is_placed(sort_col1: str) -> bool:
    """True if this row has a Place ranking (sort_col1 ends with _N)."""
    return bool(PLACED_RE.search(sort_col1))


def is_time_clip(s: str) -> bool:
    """True if s looks like a time/clip index: '1:14', '0:29', '3', '103'."""
    return bool(re.match(r'^\d+(:\d+)?$', s.strip()))


def parse_row(parts: list[str]) -> dict:
    """Map a list of split fields to named columns."""
    if len(parts) < 7:
        return {}

    trick_name = parts[0]
    sort_name  = parts[1]
    record     = parts[2]
    sort_col2  = parts[-1]
    sort_col1  = parts[-2]

    # Determine if this row has a Place field
    placed = is_placed(sort_col1)

    idx = 3  # cursor after record
    if placed:
        place = parts[idx];  idx += 1
    else:
        place = ""

    adds   = parts[idx];  idx += 1
    player = parts[idx];  idx += 1
    date   = parts[idx];  idx += 1

    # Remaining fields before sort_col1 (last two)
    remaining = parts[idx:-2]

    approx    = ""
    video     = ""
    time_clip = ""
    notes     = ""

    if not remaining:
        pass
    elif remaining[0] == "Yes":
        approx = "Yes"
        remaining = remaining[1:]
        if remaining:
            video = remaining[0]
            remaining = remaining[1:]
    else:
        video = remaining[0]
        remaining = remaining[1:]

    # remaining = [time_clip?] [notes?]
    if len(remaining) == 1:
        if is_time_clip(remaining[0]):
            time_clip = remaining[0]
        else:
            notes = remaining[0]
    elif len(remaining) >= 2:
        time_clip = remaining[0]
        notes = " ".join(remaining[1:])

    return {
        "trick_name":  trick_name,
        "sort_name":   sort_name,
        "record":      record,
        "place":       place,
        "adds":        adds,
        "player":      player,
        "date_raw":    date,
        "approx_date": approx,
        "video":       video,
        "time_clip":   time_clip,
        "notes":       notes,
        "sort_col1":   sort_col1,
        "sort_col2":   sort_col2,
    }


def parse_passback_data(text: str) -> list[dict]:
    """Parse full pasted passback table text into list of row dicts."""
    raw_lines = text.strip().split("\n")

    # Strip header row
    if raw_lines and "Trick/record name" in raw_lines[0]:
        raw_lines = raw_lines[1:]

    # Handle sort-col lines that split onto their own row (edge case)
    # If a line parses to 1 col and looks like a sort col (no spaces, ends with _),
    # attach it back to previous incomplete row
    merged_lines: list[str] = []
    pending_extra: list[str] = []

    for raw in raw_lines:
        parts = SEP_RE.split(raw)
        # Blank / empty
        if len(parts) <= 1 and not raw.strip():
            continue
        # Looks like an orphaned sort column (no spaces in value, ends with _)
        if len(parts) == 1 and re.match(r'^[\w\(\)\-\.]+_\d*$', raw.strip()):
            pending_extra.append(raw.strip())
            continue
        # Flush any pending sort cols onto the previous line
        if pending_extra and merged_lines:
            merged_lines[-1] = merged_lines[-1] + "    " + "    ".join(pending_extra)
            pending_extra = []
        merged_lines.append(raw)

    # Final flush
    if pending_extra and merged_lines:
        merged_lines[-1] = merged_lines[-1] + "    " + "    ".join(pending_extra)

    rows = []
    for line in merged_lines:
        parts = SEP_RE.split(line.strip())
        if len(parts) < 7:
            continue
        row = parse_row(parts)
        if row and row.get("trick_name") and row.get("player"):
            rows.append(row)

    return rows


# ── Normalization ─────────────────────────────────────────────────────────────

DATE_FIX = {
    "2/27/2925": "2/27/2025",
    "1/1/1905":  "1/1/2005",
}


def normalize_date(date_raw: str) -> tuple[str, str]:
    """Return (date_normalized, date_note)."""
    d = date_raw.strip()
    if d in DATE_FIX:
        return DATE_FIX[d], "date_corrected"
    return d, ""


def infer_unit(trick_name: str, record: str, adds: str) -> str:
    """Infer unit for the record value."""
    name_l = trick_name.lower()
    if "juggle" in name_l:
        return "consecutive_juggles"
    if "dex" in name_l or "unique" in name_l:
        return "consecutive_dex"
    # Most records are consecutive completions of a specific move
    return "consecutive_completions"


def infer_confidence(row: dict) -> str:
    """Default=medium, low if approx or note about video unavailable."""
    if row.get("approx_date") == "Yes":
        return "low"
    notes_l = (row.get("notes") or "").lower()
    if "unavailable" in notes_l or "previously verified" in notes_l:
        return "medium"  # verified but no current video
    if row.get("video"):
        return "medium"
    return "low"


def build_record_id(trick_name: str, place: str) -> str:
    """Stable UUID5 from trick name + place."""
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # URL namespace
    key = f"passback::{trick_name.lower().strip()}::{place or '0'}"
    return str(uuid.uuid5(ns, key))


# ── Main ──────────────────────────────────────────────────────────────────────

RAW_FIELDS = [
    "trick_name", "sort_name", "record", "place", "adds",
    "player", "date_raw", "approx_date", "video", "time_clip",
    "notes", "sort_col1", "sort_col2",
]

MASTER_FIELDS = [
    "record_id", "trick_name", "sort_name",
    "record_value", "place", "adds",
    "player", "date_normalized", "approx_date",
    "video", "time_clip", "notes",
    "unit", "confidence", "source",
]


def main() -> None:
    if not INFILE.exists():
        print(f"ERROR: Input file not found: {INFILE}", file=sys.stderr)
        print("Paste the passback.com/records table content into:", INFILE)
        sys.exit(1)

    text = INFILE.read_text(encoding="utf-8")
    rows = parse_passback_data(text)

    if not rows:
        print("ERROR: No rows parsed. Check input file format.", file=sys.stderr)
        sys.exit(1)

    print(f"Parsed {len(rows)} record entries.")

    # Write raw CSV
    INDIR.mkdir(parents=True, exist_ok=True)
    with RAW_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"  Raw CSV  → {RAW_CSV.relative_to(ROOT)}")

    # Write master CSV
    master_rows = []
    for row in rows:
        date_norm, date_note = normalize_date(row["date_raw"])
        note_parts = []
        if date_note:
            note_parts.append(date_note)
        if row.get("notes"):
            note_parts.append(row["notes"])

        master_rows.append({
            "record_id":       build_record_id(row["trick_name"], row["place"]),
            "trick_name":      row["trick_name"],
            "sort_name":       row["sort_name"],
            "record_value":    row["record"],
            "place":           row["place"],
            "adds":            row["adds"],
            "player":          row["player"],
            "date_normalized": date_norm,
            "approx_date":     row["approx_date"],
            "video":           row["video"],
            "time_clip":       row["time_clip"],
            "notes":           "; ".join(note_parts),
            "unit":            infer_unit(row["trick_name"], row["record"], row["adds"]),
            "confidence":      infer_confidence(row),
            "source":          "passback",
        })

    with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        w.writeheader()
        w.writerows(master_rows)
    print(f"  Master CSV → {MASTER_CSV.relative_to(ROOT)}")

    # Summary
    unique_tricks = len({r["trick_name"] for r in rows})
    placed_count  = sum(1 for r in rows if r["place"])
    approx_count  = sum(1 for r in rows if r["approx_date"] == "Yes")
    print()
    print(f"Summary:")
    print(f"  Unique trick/record names: {unique_tricks}")
    print(f"  Total record entries:      {len(rows)}")
    print(f"  Ranked (placed) entries:   {placed_count}")
    print(f"  Approx-dated entries:      {approx_count}")

    from collections import Counter
    player_counts = Counter(r["player"] for r in rows)
    print(f"  Top record holders:")
    for player, n in player_counts.most_common(5):
        print(f"    {n:3d}  {player}")


if __name__ == "__main__":
    main()
