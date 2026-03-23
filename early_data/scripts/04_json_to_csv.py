#!/usr/bin/env python3
"""
04_json_to_csv.py — Transform Gemini batch JSON extractions into structured CSVs.

Reads all gemini_batch_*.json files from early_data/review/
Produces:
  early_data/event_blocks/fbw_event_blocks.csv
  early_data/placements/fbw_placements_flat.csv

Usage:
  python early_data/scripts/04_json_to_csv.py

event_id generation: hashlib SHA-1 of (source_file + "|" + event_name_raw),
truncated to 10 hex chars — stable across re-runs.
"""

import csv
import glob
import hashlib
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_DIR     = REPO_ROOT / "early_data" / "review"
BLOCKS_CSV     = REPO_ROOT / "early_data" / "event_blocks" / "fbw_event_blocks.csv"
PLACEMENTS_CSV = REPO_ROOT / "early_data" / "placements" / "fbw_placements_flat.csv"
OOS_BLOCKS_CSV     = REPO_ROOT / "early_data" / "event_blocks" / "fbw_event_blocks_out_of_scope.csv"
OOS_PLACEMENTS_CSV = REPO_ROOT / "early_data" / "placements" / "fbw_placements_out_of_scope.csv"

PRE1997_CUTOFF = 1997  # year >= this → out of scope for pre-1997 dataset


def make_event_id(source_file: str, event_name_raw: str) -> str:
    key = f"{source_file}|{event_name_raw}"
    return hashlib.sha1(key.encode()).hexdigest()[:10]


def source_type(source_file: str) -> str:
    return "IFAB" if "IFAB" in source_file.upper() else "FBW"


def extract_year(date_raw: str) -> str:
    m = re.search(r"\b([12][09]\d{2})\b", date_raw)
    return m.group(1) if m else ""


def process_batches():
    batch_files = sorted(REVIEW_DIR.glob("gemini_batch_*.json"))
    if not batch_files:
        print(f"ERROR: No gemini_batch_*.json files found in {REVIEW_DIR}")
        sys.exit(1)

    print(f"Found {len(batch_files)} batch files:")
    for f in batch_files:
        print(f"  {f.name}")

    event_rows = []
    placement_rows = []
    seen_event_ids = {}  # event_id -> first occurrence for collision detection

    for batch_path in batch_files:
        with open(batch_path, encoding="utf-8") as fh:
            pages = json.load(fh)

        for page in pages:
            source_file = page.get("source_file", "")
            stype = source_type(source_file)

            for event in page.get("events", []):
                event_name_raw = event.get("event_name_raw", "")
                date_raw       = event.get("date_raw", "")
                location_raw   = event.get("location_raw", "")
                year           = extract_year(date_raw)
                event_id       = make_event_id(source_file, event_name_raw)

                # Warn on collision (same source_file + event_name from different batch)
                collision_key = (source_file, event_name_raw)
                if collision_key in seen_event_ids:
                    print(f"  WARNING: duplicate event in {batch_path.name}: "
                          f"'{event_name_raw}' from '{source_file}' "
                          f"(first seen in {seen_event_ids[collision_key]})")
                else:
                    seen_event_ids[collision_key] = batch_path.name

                exclude = "TRUE" if (year and int(year) >= PRE1997_CUTOFF) else ""

                event_rows.append({
                    "event_id":          event_id,
                    "event_name_raw":    event_name_raw,
                    "year":              year,
                    "date_raw":          date_raw,
                    "location_raw":      location_raw,
                    "source_file":       source_file,
                    "source_type":       stype,
                    "exclude_pre1997":   exclude,
                })

                for division in event.get("divisions", []):
                    division_raw = division.get("division_raw", "")
                    for result in division.get("results", []):
                        placement_rows.append({
                            "event_id":      event_id,
                            "division_raw":  division_raw,
                            "placement_raw": result.get("placement_raw", ""),
                            "placement_num": result.get("placement_num", ""),
                            "player_raw":    result.get("player_raw", ""),
                            "team_raw":      result.get("team_raw", ""),
                            "score_raw":     result.get("score_raw", ""),
                            "notes":         result.get("notes", ""),
                            "source_file":   source_file,
                        })

    # Partition by scope
    oos_event_ids  = {r["event_id"] for r in event_rows if r["exclude_pre1997"] == "TRUE"}
    in_scope_events  = [r for r in event_rows     if r["exclude_pre1997"] != "TRUE"]
    oos_events       = [r for r in event_rows     if r["exclude_pre1997"] == "TRUE"]
    in_scope_placements = [r for r in placement_rows if r["event_id"] not in oos_event_ids]
    oos_placements      = [r for r in placement_rows if r["event_id"] in oos_event_ids]

    block_fields = [
        "event_id", "event_name_raw", "year", "date_raw",
        "location_raw", "source_file", "source_type", "exclude_pre1997",
    ]
    placement_fields = [
        "event_id", "division_raw", "placement_raw", "placement_num",
        "player_raw", "team_raw", "score_raw", "notes", "source_file",
    ]

    def write_csv(path, fields, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    # Main pre-1997 outputs
    write_csv(BLOCKS_CSV,     block_fields,     in_scope_events)
    write_csv(PLACEMENTS_CSV, placement_fields, in_scope_placements)

    # Out-of-scope outputs
    write_csv(OOS_BLOCKS_CSV,     block_fields,     oos_events)
    write_csv(OOS_PLACEMENTS_CSV, placement_fields, oos_placements)

    # Summary
    unique_sources = len(set(r["source_file"] for r in event_rows))
    print(f"\nPre-1997 (in scope, year < {PRE1997_CUTOFF}):")
    print(f"  Event blocks:  {BLOCKS_CSV.name} ({len(in_scope_events)} rows)")
    print(f"  Placements:    {PLACEMENTS_CSV.name} ({len(in_scope_placements)} rows)")
    print(f"\nOut of scope (year >= {PRE1997_CUTOFF}):")
    print(f"  Event blocks:  {OOS_BLOCKS_CSV.name} ({len(oos_events)} rows)")
    print(f"  Placements:    {OOS_PLACEMENTS_CSV.name} ({len(oos_placements)} rows)")
    if oos_events:
        for r in oos_events:
            print(f"    {r['year']}  {r['event_name_raw']}  [{r['source_file']}]")
    print(f"\nTotals across all batches:")
    print(f"  Source pages:   {unique_sources}")
    print(f"  Events:         {len(event_rows)}")
    print(f"  Placements:     {len(placement_rows)}")
    not_held = [r for r in in_scope_placements if "not held" in r["notes"].lower()]
    print(f"  'Event Not Held' rows (in scope): {len(not_held)}")
    unknown = [r for r in in_scope_placements if r["player_raw"] == "?" or r["team_raw"] == "?"]
    print(f"  '?' entries (in scope):           {len(unknown)}")


if __name__ == "__main__":
    process_batches()
