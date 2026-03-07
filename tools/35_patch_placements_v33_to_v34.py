#!/usr/bin/env python3
"""
35_patch_placements_v33_to_v34.py — Targeted patch of Placements_ByPerson_v33 → v34.

Patches two events where parser fixes (added in rescue-current) changed the
placements data but the identity lock (v33) still has the old incorrect data.

Event 1706036811 (Montreal 2024 Worlds):
  1. Reclassify 16 of 30 "Routines" rows as "Battles" (division split fix)
  2. Add François Pelletier to Open Doubles place 1 (dash-separator fix)

Event 857874500 (1997 Oregon April):
  - Remove all 20 stale rows (wrong division structure)
  - Add 34 new rows from stage2 (correct 9-division layout)

Usage:
  python tools/35_patch_placements_v33_to_v34.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import unicodedata
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"
OUT = ROOT / "out"

PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v33.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v34.csv"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PT_CSV         = IDENTITY_LOCK / "Persons_Truth_Final_v32.csv"


def _norm(s: str) -> str:
    return unicodedata.normalize("NFC", s).strip().lower()


def load_pt(pt_path: Path) -> dict[str, tuple[str, str]]:
    """Return {norm_canon: (effective_person_id, person_canon)}."""
    result = {}
    with open(pt_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pc = row["person_canon"].strip()
            result[_norm(pc)] = (row["effective_person_id"], pc)
            # Also index aliases
            for alias in row.get("aliases", "").split("|"):
                a = alias.strip()
                if a:
                    result[_norm(a)] = (row["effective_person_id"], pc)
    return result


def load_stage2_event(stage2_path: Path, event_id: str) -> list[dict]:
    with open(stage2_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["event_id"] == event_id:
                return json.loads(row["placements_json"])
    return []


import re as _re

def strip_country(name: str) -> str:
    """Strip trailing country code like '(USA)' or '(CAN)'."""
    return _re.sub(r'\s*\([A-Z]{2,3}\)\s*$', '', name).strip()


_RE_QUOTED_NICKNAME = _re.compile(r'[\"\u201c\u201d\u2018\u2019]\w[\w\s]*[\"\u201c\u201d\u2018\u2019]')


def _name_variants(name: str) -> list[str]:
    """Generate lookup variants for a player name."""
    variants = [name]
    # Hyphen → space (e.g. "Becca English-Ross" → "Becca English Ross")
    if "-" in name:
        variants.append(name.replace("-", " "))
    # Strip quoted nickname (e.g. 'Jim "Toes" Fitzgerald' → 'Jim Fitzgerald')
    stripped = _RE_QUOTED_NICKNAME.sub("", name)
    stripped = _re.sub(r'\s{2,}', ' ', stripped).strip()
    if stripped and stripped != name:
        variants.append(stripped)
        if "-" in stripped:
            variants.append(stripped.replace("-", " "))
    return variants


# Manual name overrides for known PT canon mismatches
_MANUAL_OVERRIDES: dict[str, str] = {
    "evanne lamarche":   "Evanne Lemarche",
    "evanne la marche":  "Evanne Lemarche",
    "kenny shults":      "Kenneth Shults",
}


def lookup_person(name: str, pt: dict) -> tuple[str, str, str]:
    """
    Look up a player name in PT.
    Returns (person_id, person_canon, person_unresolved).
    person_unresolved is "1" if not found in PT.
    """
    clean = strip_country(name)
    # Check manual overrides first
    override = _MANUAL_OVERRIDES.get(_norm(clean))
    if override:
        key = _norm(override)
        if key in pt:
            pid, pcanon = pt[key]
            return pid, pcanon, ""
    for variant in _name_variants(clean):
        # Try exact norm
        key = _norm(variant)
        if key in pt:
            pid, pcanon = pt[key]
            return pid, pcanon, ""
        # Try without diacritics
        nfd = unicodedata.normalize("NFD", key)
        ascii_key = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
        if ascii_key in pt:
            pid, pcanon = pt[ascii_key]
            return pid, pcanon, ""
    # Not found — unresolved
    return "", clean, "1"


# ---------------------------------------------------------------------------
# Battles/Routines classification for event 1706036811
# Derived by cross-referencing stage2 Battles/Routines against v33 rows.
# Key: (place, norm) → "Battles" or "Routines"
# People who appear in BOTH divisions at DIFFERENT places can be disambiguated.
# ---------------------------------------------------------------------------
ROUTINES_BATTLES_MAP: dict[tuple[str, str], str] = {
    # (place, norm) → division
    ("1",  "taishi ishida"):       "Routines",   # Routines p1; Battles p3
    ("1",  "christopher schillem"):"Battles",    # Battles p1; Routines p8
    ("2",  "brian sherrill"):       "Routines",   # Routines p2; Battles p9 (Brian Sherill)
    ("2",  "dante diotallevi"):     "Battles",    # Battles p2; Routines p9
    ("3",  "nick landes"):          "Routines",   # Routines p3; Battles p9
    ("3",  "taishi ishida"):        "Battles",    # Battles p3; Routines p1
    ("4",  "chris dean"):           "Battles",    # Battles only
    ("4",  "dustin rhodes"):        "Battles",    # Battles p4; Routines p6
    ("4",  "pawel nowak"):          "Routines",   # Routines p4; Battles p5
    ("5",  "pawel nowak"):          "Battles",    # Battles p5; Routines p4
    ("5",  "mathieu gauthier"):     "Routines",   # Routines p5; Battles p7
    ("6",  "dustin rhodes"):        "Routines",   # Routines p6; Battles p4
    ("7",  "kevin hogan"):          "Battles",    # Battles only
    ("7",  "rory dawson"):          "Routines",   # Routines only
    ("7",  "mathieu gauthier"):     "Battles",    # Battles p7; Routines p5
    ("8",  "christopher schillem"): "Routines",   # Routines p8; Battles p1
    ("9",  "nathan bonslaver"):     "Battles",    # Battles only
    ("9",  "brian sherrill"):       "Battles",    # Battles p9 (as "Brian Sherill"); v33 norm is "brian sherrill"
    ("9",  "matt kemmer"):          "Battles",    # Battles only
    ("9",  "nick landes"):          "Battles",    # Battles p9; Routines p3
    ("9",  "dante diotallevi"):     "Routines",   # Routines p9; Battles p2
    ("9",  "zach forth"):           "Routines",   # Routines only
    ("11", "scott davidson"):       "Routines",   # Routines only
    ("12", "drew martin"):          "Routines",   # Routines p12; Battles p13
    ("12", "jason varvaro"):        "Routines",   # Routines p12; Battles p13
    ("13", "drew martin"):          "Battles",    # Battles p13; Routines p12
    ("13", "tuomas riisalo"):       "Battles",    # Battles only
    ("13", "jason varvaro"):        "Battles",    # Battles p13; Routines p12
    ("13", "johnny sarah"):         "Battles",    # Battles only
    ("14", "benjamin barrows"):     "Routines",   # Routines only
}


def patch_1706036811(rows: list[dict], pt: dict) -> tuple[list[dict], int, int]:
    """
    1. Reclassify 16 Routines rows as Battles.
    2. Add François Pelletier to Open Doubles place 1.
    Returns (patched_rows, battles_fixed, pelletier_added).
    """
    battles_fixed = 0
    new_rows = []

    for r in rows:
        if r["event_id"] != "1706036811":
            new_rows.append(r)
            continue

        if r["division_canon"] == "Routines":
            key = (r["place"], r["norm"])
            target_div = ROUTINES_BATTLES_MAP.get(key)
            if target_div is None:
                # Fallback: keep as Routines
                new_rows.append(r)
                print(f"  WARN: no mapping for Routines row ({r['place']}, {r['norm']})")
            elif target_div == "Battles":
                r2 = dict(r)
                r2["division_canon"] = "Battles"
                new_rows.append(r2)
                battles_fixed += 1
            else:
                new_rows.append(r)
        else:
            new_rows.append(r)

    # Check if François Pelletier already present in Open Doubles p1
    pelletier_added = 0
    francois_norm = _norm("Francois Pelletier")
    already_there = any(
        r["event_id"] == "1706036811"
        and r["division_canon"] == "Open Doubles Net"
        and r["place"] == "1"
        and r["norm"] == francois_norm
        for r in new_rows
    )
    if not already_there:
        # Find the Emmanuel Bouchard p1 row as template
        template = next(
            (r for r in new_rows
             if r["event_id"] == "1706036811"
             and r["division_canon"] == "Open Doubles Net"
             and r["place"] == "1"),
            None
        )
        if template:
            pid, pcanon, punres = lookup_person("Francois Pelletier", pt)
            new_row = dict(template)
            new_row["person_id"] = pid
            new_row["person_canon"] = pcanon
            new_row["person_unresolved"] = punres
            new_row["norm"] = _norm(pcanon)
            # team_person_key: add alongside Emmanuel
            # Find Emmanuel's person_id
            emmanuel_id = template["person_id"]
            new_row["team_person_key"] = f"{emmanuel_id}|{pid}" if pid else ""
            new_rows.append(new_row)
            pelletier_added = 1
            print(f"  Added François Pelletier (Open Doubles p1): {pcanon} [{pid}]")

    return new_rows, battles_fixed, pelletier_added


def build_new_oregon_rows(stage2_placements: list[dict], pt: dict, year: str = "1997") -> list[dict]:
    """Build v34-format rows from stage2 data for event 857874500."""
    rows = []
    # coverage: all divisions have clean complete data (4 entries each)
    coverage_flag = "complete"

    for e in stage2_placements:
        div_canon = e.get("division_canon", "Unknown")
        div_cat   = e.get("division_category", "net")
        place     = str(e.get("place", ""))
        ctype     = e.get("competitor_type", "player")
        p1        = e.get("player1_name", "")
        p2        = e.get("player2_name", "")

        if ctype == "team" and p1 and p2:
            # Two rows: one per team member
            pid1, pcan1, pun1 = lookup_person(p1, pt)
            pid2, pcan2, pun2 = lookup_person(p2, pt)
            team_key = f"{pid1}|{pid2}" if (pid1 and pid2) else ""
            team_display = f"{pcan1} / {pcan2}"
            for pid, pcan, pun in [(pid1, pcan1, pun1), (pid2, pcan2, pun2)]:
                rows.append({
                    "event_id":          "857874500",
                    "year":              year,
                    "division_canon":    div_canon,
                    "division_category": div_cat,
                    "place":             place,
                    "competitor_type":   ctype,
                    "person_id":         pid,
                    "team_person_key":   team_key,
                    "person_canon":      pcan,
                    "team_display_name": team_display,
                    "coverage_flag":     coverage_flag,
                    "person_unresolved": pun,
                    "norm":              _norm(pcan),
                })
        elif p1:
            pid, pcan, pun = lookup_person(p1, pt)
            rows.append({
                "event_id":          "857874500",
                "year":              year,
                "division_canon":    div_canon,
                "division_category": div_cat,
                "place":             place,
                "competitor_type":   "player",
                "person_id":         pid,
                "team_person_key":   "",
                "person_canon":      pcan,
                "team_display_name": "",
                "coverage_flag":     coverage_flag,
                "person_unresolved": pun,
                "norm":              _norm(pcan),
            })

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print changes without writing output file")
    args = parser.parse_args()

    print(f"Loading PT from {PT_CSV}")
    pt = load_pt(PT_CSV)
    print(f"  {len(pt)} entries (including aliases)")

    print(f"\nLoading v33 placements from {PLACEMENTS_IN}")
    with open(PLACEMENTS_IN, newline="", encoding="utf-8") as f:
        fieldnames = None
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        v33_rows = list(reader)
    print(f"  {len(v33_rows)} rows")

    # -----------------------------------------------------------------------
    # Patch event 1706036811
    # -----------------------------------------------------------------------
    print("\n=== Patching event 1706036811 (Montreal 2024 Worlds) ===")
    v33_rows, battles_fixed, pelletier_added = patch_1706036811(v33_rows, pt)
    print(f"  Reclassified {battles_fixed} Routines → Battles")
    print(f"  Added {pelletier_added} François Pelletier row(s) to Open Doubles p1")

    # -----------------------------------------------------------------------
    # Replace event 857874500 rows with stage2 data
    # -----------------------------------------------------------------------
    print("\n=== Replacing event 857874500 (1997 Oregon April) ===")
    old_oregon = [r for r in v33_rows if r["event_id"] == "857874500"]
    print(f"  Removing {len(old_oregon)} old rows")
    v33_rows = [r for r in v33_rows if r["event_id"] != "857874500"]

    stage2_oregon = load_stage2_event(STAGE2_CSV, "857874500")
    print(f"  Loaded {len(stage2_oregon)} stage2 placements")
    new_oregon = build_new_oregon_rows(stage2_oregon, pt)
    print(f"  Generated {len(new_oregon)} new rows")

    # Report PT matches
    matched   = sum(1 for r in new_oregon if r["person_id"])
    unresolved = sum(1 for r in new_oregon if r["person_unresolved"] == "1")
    print(f"  PT matched: {matched}, unresolved: {unresolved}")
    for r in new_oregon:
        flag = " [UNRESOLVED]" if r["person_unresolved"] == "1" else ""
        print(f"    {r['place']:2s} {r['division_canon']:25s} {r['person_canon']}{flag}")

    # Insert Oregon rows back (sorted by event_id, division, place)
    v33_rows = v33_rows + new_oregon

    total = len(v33_rows)
    print(f"\nTotal rows after patch: {total}")

    if args.dry_run:
        print("\n[DRY RUN] — not writing output file")
        return

    print(f"\nWriting {PLACEMENTS_OUT}")
    with open(PLACEMENTS_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(v33_rows)
    print(f"Done. Wrote {total} rows to {PLACEMENTS_OUT.name}")


if __name__ == "__main__":
    main()
