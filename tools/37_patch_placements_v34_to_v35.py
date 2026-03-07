#!/usr/bin/env python3
"""
37_patch_placements_v34_to_v35.py — Targeted patch of Placements_ByPerson_v34 → v35.

Replaces stale placements for two events where data was recovered via
RESULTS_FILE_OVERRIDES (legacy_data/event_results/) after v34 was locked:

  Event 915561090 (1999 World Footbag Championships):
    - Remove 43 stale rows (Open Singles Net only)
    - Add 226 rows from stage2 (19 divisions — full results recovered)

  Event 1035277529 (2003 World Footbag Championships):
    - Remove 181 stale rows (10 divisions, partially mis-parsed)
    - Add 192 rows from stage2 (15 divisions — full results recovered)

Usage:
  python tools/37_patch_placements_v34_to_v35.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import unicodedata
import re as _re
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT           = Path(__file__).resolve().parent.parent
IDENTITY_LOCK  = ROOT / "inputs" / "identity_lock"
OUT            = ROOT / "out"

PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v34.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v35.csv"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PT_CSV         = IDENTITY_LOCK / "Persons_Truth_Final_v32.csv"

PATCH_EVENTS = {"915561090", "1035277529"}
EVENT_YEARS  = {"915561090": "1999", "1035277529": "2003"}


def _norm(s: str) -> str:
    return unicodedata.normalize("NFC", s).strip().lower()


def load_pt(pt_path: Path) -> dict[str, tuple[str, str]]:
    """Return {norm_canon: (effective_person_id, person_canon)}."""
    result = {}
    with open(pt_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pc = row["person_canon"].strip()
            result[_norm(pc)] = (row["effective_person_id"], pc)
            for alias in row.get("aliases", "").split("|"):
                a = alias.strip()
                if a:
                    result[_norm(a)] = (row["effective_person_id"], pc)
    return result


def load_stage2_event(stage2_path: Path, event_id: str) -> list[dict]:
    with open(stage2_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["event_id"] == event_id:
                return json.loads(row.get("placements_json", "[]"))
    return []


_RE_QUOTED_NICKNAME = _re.compile(
    r'[\"\\u201c\\u201d\\u2018\\u2019]\\w[\\w\\s]*[\"\\u201c\\u201d\\u2018\\u2019]'
)

_MANUAL_OVERRIDES: dict[str, str] = {
    "evanne lamarche":  "Evanne Lemarche",
    "evanne la marche": "Evanne Lemarche",
    "kenny shults":     "Kenneth Shults",
}


def strip_country(name: str) -> str:
    return _re.sub(r'\s*\([A-Z]{2,3}\)\s*$', '', name).strip()


def _name_variants(name: str) -> list[str]:
    variants = [name]
    if "-" in name:
        variants.append(name.replace("-", " "))
    stripped = _RE_QUOTED_NICKNAME.sub("", name)
    stripped = _re.sub(r'\s{2,}', ' ', stripped).strip()
    if stripped and stripped != name:
        variants.append(stripped)
        if "-" in stripped:
            variants.append(stripped.replace("-", " "))
    return variants


def lookup_person(name: str, pt: dict) -> tuple[str, str, str]:
    clean = strip_country(name)
    override = _MANUAL_OVERRIDES.get(_norm(clean))
    if override:
        key = _norm(override)
        if key in pt:
            pid, pcanon = pt[key]
            return pid, pcanon, ""
    for variant in _name_variants(clean):
        key = _norm(variant)
        if key in pt:
            pid, pcanon = pt[key]
            return pid, pcanon, ""
        nfd = unicodedata.normalize("NFD", key)
        ascii_key = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
        if ascii_key in pt:
            pid, pcanon = pt[ascii_key]
            return pid, pcanon, ""
    return "", clean, "1"


def build_rows_from_stage2(stage2_placements: list[dict], pt: dict,
                            event_id: str) -> list[dict]:
    """Build v35-format rows from stage2 placements for a given event."""
    year = EVENT_YEARS.get(event_id, "")
    rows = []
    for entry in stage2_placements:
        div_canon = entry.get("division_canon", "Unknown")
        div_cat   = entry.get("division_category", "net")
        place     = str(entry.get("place", ""))
        ctype     = entry.get("competitor_type", "player")
        p1        = entry.get("player1_name", "")
        p2        = entry.get("player2_name", "")

        if ctype == "team" and p1 and p2:
            pid1, pcan1, pun1 = lookup_person(p1, pt)
            pid2, pcan2, pun2 = lookup_person(p2, pt)
            team_key = f"{pid1}|{pid2}" if (pid1 and pid2) else ""
            team_display = f"{pcan1} / {pcan2}"
            for pid, pcan, pun in [(pid1, pcan1, pun1), (pid2, pcan2, pun2)]:
                rows.append({
                    "event_id":          event_id,
                    "year":              year,
                    "division_canon":    div_canon,
                    "division_category": div_cat,
                    "place":             place,
                    "competitor_type":   ctype,
                    "person_id":         pid,
                    "team_person_key":   team_key,
                    "person_canon":      pcan,
                    "team_display_name": team_display,
                    "coverage_flag":     "complete",
                    "person_unresolved": pun,
                    "norm":              _norm(pcan),
                })
        elif p1:
            pid, pcan, pun = lookup_person(p1, pt)
            rows.append({
                "event_id":          event_id,
                "year":              year,
                "division_canon":    div_canon,
                "division_category": div_cat,
                "place":             place,
                "competitor_type":   "player",
                "person_id":         pid,
                "team_person_key":   "",
                "person_canon":      pcan,
                "team_display_name": "",
                "coverage_flag":     "complete",
                "person_unresolved": pun,
                "norm":              _norm(pcan),
            })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Loading PT from {PT_CSV}")
    pt = load_pt(PT_CSV)
    print(f"  {len(pt)} entries (including aliases)")

    print(f"\nLoading v34 placements from {PLACEMENTS_IN}")
    with open(PLACEMENTS_IN, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        v34_rows = list(reader)
    print(f"  {len(v34_rows)} rows")

    # Remove stale rows for patch events
    kept = [r for r in v34_rows if r["event_id"] not in PATCH_EVENTS]
    removed = len(v34_rows) - len(kept)
    print(f"\nRemoved {removed} stale rows for events {sorted(PATCH_EVENTS)}")

    # Build replacement rows from stage2
    all_new = []
    for event_id in sorted(PATCH_EVENTS):
        print(f"\n=== Building rows for event {event_id} ===")
        stage2 = load_stage2_event(STAGE2_CSV, event_id)
        print(f"  {len(stage2)} stage2 placements")
        new_rows = build_rows_from_stage2(stage2, pt, event_id)
        print(f"  {len(new_rows)} new rows generated")

        # Stats
        matched    = sum(1 for r in new_rows if r["person_id"])
        unresolved = sum(1 for r in new_rows if r["person_unresolved"] == "1")
        print(f"  PT matched: {matched}, unresolved: {unresolved}")
        if unresolved:
            for r in new_rows:
                if r["person_unresolved"] == "1":
                    print(f"    [UNRESOLVED] {r['place']:3s} {r['division_canon']:30s} {r['person_canon']}")
        all_new.extend(new_rows)

    v35_rows = kept + all_new
    total = len(v35_rows)
    print(f"\nTotal rows after patch: {total} (was {len(v34_rows)}, delta {total - len(v34_rows):+d})")

    if args.dry_run:
        print("\n[DRY RUN] — not writing output file")
        return

    print(f"\nWriting {PLACEMENTS_OUT}")
    with open(PLACEMENTS_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(v35_rows)
    print(f"Done. Wrote {total} rows to {PLACEMENTS_OUT.name}")


if __name__ == "__main__":
    main()
