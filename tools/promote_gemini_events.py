#!/usr/bin/env python3
"""
tools/promote_gemini_events.py

Promote non-superseded Gemini-transcribed FBW events from
early_data/placements/placements_flat.csv into
inputs/curated/events/structured/ as Variant B CSVs.

Non-superseded events: unique competitions (not worlds, not NHSA/WFA nationals)
that have no equivalent in any other current pipeline source.

Usage:
    python3 tools/promote_gemini_events.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

REPO_ROOT       = Path(__file__).resolve().parents[1]
PLACEMENTS_CSV  = REPO_ROOT / "early_data" / "placements" / "placements_flat.csv"
MAPPING_CSV     = REPO_ROOT / "early_data" / "canonical" / "event_id_mapping.csv"
OUT_DIR         = REPO_ROOT / "inputs" / "curated" / "events" / "structured"

# Non-superseded events: Gemini hex_id → output metadata
# (events that exist in NO other pipeline source)
PROMOTE = {
    "005cf9782a": {
        "event_name": "Mike Marshall Memorial",
        "year":       1980,
        "location":   "San Jose, California, United States",
        "filename":   "fbw_1980_mike_marshall_memorial.csv",
    },
    "a6acc8449b": {
        "event_name": "National Footbag Championships",
        "year":       1980,
        "location":   "United States",
        "filename":   "fbw_1980_national_footbag_championships.csv",
    },
    "2a8ad0dbe8": {
        "event_name": "Mike Marshall Memorial",
        "year":       1981,
        "location":   "San Jose, California, United States",
        "filename":   "fbw_1981_mike_marshall_memorial.csv",
    },
    "7242e1f63b": {
        "event_name": "Western Regionals",
        "year":       1982,
        "location":   "United States",
        "filename":   "fbw_1982_western_regionals.csv",
    },
    "524d5a426e": {
        "event_name": "Oregon State Footbag Championships",
        "year":       1983,
        "location":   "Oregon, United States",
        "filename":   "fbw_1983_oregon_state_championships.csv",
    },
    "da1be2ed69": {
        "event_name": "European Footbag Championships",
        "year":       1984,
        "location":   "Europe",
        "filename":   "fbw_1984_european_championships.csv",
    },
    "4a11e53de5": {
        "event_name": "European Footbag Championships",
        "year":       1987,
        "location":   "Europe",
        "filename":   "fbw_1987_european_championships.csv",
    },
    "9f5e7b1351": {
        "event_name": "U.S. National Footbag Championships",
        "year":       1988,
        "location":   "United States",
        "filename":   "fbw_1988_us_national_championships.csv",
    },
}

FIELDNAMES = [
    "event_name", "year", "location", "category",
    "division", "place", "player_1", "player_2", "score", "notes",
]

_RE_CONSEC   = re.compile(r"\bconsec", re.I)
_RE_GOLF     = re.compile(r"\bgolf\b", re.I)
_RE_STYLE    = re.compile(r"\b(freestyle|freestyle|routine|shred|circle|sick|battle|trick|combo|ironman)\b", re.I)


def infer_category(division: str) -> str:
    d = division.lower()
    if _RE_GOLF.search(d):
        return "GOLF"
    if _RE_CONSEC.search(d):
        return "CONSECUTIVE"
    if _RE_STYLE.search(d):
        return "FREESTYLE"
    # Contains "overall" / "standing"
    if "overall" in d or "standing" in d:
        return "OVERALL"
    return "NET"


def split_team(team_raw: str) -> tuple[str, str]:
    """Split 'A/B' into (A, B). Handle 3-member teams: keep all in player_1."""
    parts = [p.strip() for p in team_raw.split("/") if p.strip()]
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    if len(parts) == 2:
        return (parts[0], parts[1])
    # 3+ members: join all into player_1 with " / " (team consecutive etc.)
    return (" / ".join(parts), "")


def load_placements(hex_ids: set[str]) -> dict[str, list[dict]]:
    """Load and group placements_flat rows by event_id for the given hex_ids."""
    grouped: dict[str, list[dict]] = {eid: [] for eid in hex_ids}
    with open(PLACEMENTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["event_id"] in hex_ids:
                grouped[row["event_id"]].append(row)
    return grouped


def rows_for_event(placements: list[dict], meta: dict) -> list[dict]:
    """Convert placements_flat rows to Variant B output rows."""
    out = []
    for p in placements:
        # Skip "Event Not Held" and blank placement entries
        if p.get("notes", "").strip().lower() == "event not held":
            continue
        place_str = (p.get("placement_num") or "").strip()
        if not place_str:
            continue

        player_raw = (p.get("player_raw") or "").strip()
        team_raw   = (p.get("team_raw")   or "").strip()
        division   = (p.get("division_raw") or "").strip()
        score      = (p.get("score_raw")  or "").strip()
        notes      = (p.get("notes")      or "").strip()

        if not division:
            continue

        # Determine players
        if team_raw:
            player_1, player_2 = split_team(team_raw)
        elif player_raw and player_raw != "?":
            player_1, player_2 = player_raw, ""
        else:
            # Unknown player — include as blank (preserves placement slot)
            player_1, player_2 = "", ""

        out.append({
            "event_name": meta["event_name"],
            "year":       meta["year"],
            "location":   meta["location"],
            "category":   infer_category(division),
            "division":   division,
            "place":      place_str,
            "player_1":   player_1,
            "player_2":   player_2,
            "score":      score,
            "notes":      notes,
        })
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without writing files")
    args = parser.parse_args()

    placements = load_placements(set(PROMOTE.keys()))

    total_events = 0
    total_rows   = 0
    skipped_ids  = []

    for hex_id, meta in PROMOTE.items():
        event_rows = rows_for_event(placements[hex_id], meta)

        if not event_rows:
            print(f"  SKIP {meta['filename']} — no usable placements for {hex_id}")
            skipped_ids.append(hex_id)
            continue

        out_path = OUT_DIR / meta["filename"]

        if args.dry_run:
            print(f"  DRY-RUN {meta['filename']} ({len(event_rows)} rows) → {out_path}")
            for r in event_rows:
                print(f"    {r['division']!r:<40} p{r['place']}  {r['player_1']!r}")
        else:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerows(event_rows)
            print(f"  WROTE {meta['filename']} ({len(event_rows)} rows)")

        total_events += 1
        total_rows   += len(event_rows)

    print()
    print(f"Done: {total_events} files, {total_rows} total rows")
    if skipped_ids:
        print(f"Skipped (no usable placements): {skipped_ids}")

    if not args.dry_run:
        print(f"Files in {OUT_DIR}")


if __name__ == "__main__":
    main()
