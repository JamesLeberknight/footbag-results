#!/usr/bin/env python3
"""
tools/41_patch_placements_v37_to_v38.py

Migrate Placements_ByPerson v37 → v38.

Adds 8 new divisions for 2016 World Footbag Championships (event 1449259560)
that were recovered by the Stage 01 HYBRID merge fix:

  Open Singles Net         (37 placements)
  Open Doubles Net         (27 placements, teams)
  Open Mixed Doubles Net   (12 placements, teams)
  Master Singles Net       (11 placements)
  Master Doubles Net        (7 placements, teams)
  Women's Doubles Net       (6 placements, teams)
  Open Singles Routines    (22 placements, ties)
  Shred30                   (9 placements)

Usage:
  python tools/41_patch_placements_v37_to_v38.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import unicodedata
import uuid
from pathlib import Path

ROOT   = Path(__file__).parent.parent
IN_PBP = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v37.csv"
OUT_PBP= ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v38.csv"
PT_CSV = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v34.csv"
S2_CSV = ROOT / "out" / "stage2_canonical_events.csv"

EVENT_ID = "1449259560"
YEAR     = "2016"

# New divisions (entirely absent from v37)
NEW_DIVISIONS = {
    "Open Singles Net",
    "Open Doubles Net",
    "Open Mixed Doubles Net",
    "Master Singles Net",
    "Master Doubles Net",
    "Women's Doubles Net",
    "Open Singles Routines",
    "Shred30",
}

# Coverage flag for 2016 Worlds net divisions
# Net results are comprehensive (37 singles net = all registered entrants)
# Freestyle routines are tournament-bracket (complete top 22)
COVERAGE_FLAG = "complete"

csv.field_size_limit(10 ** 7)

# ── Name normalization ─────────────────────────────────────────────────────────

_COUNTRY_RE = re.compile(r"\s*\([^)]{2,30}\)\s*$")

def strip_country(name: str) -> str:
    """Remove trailing country/club suffix: 'Walt Houston (USA)' → 'Walt Houston'."""
    return _COUNTRY_RE.sub("", name).strip()


def norm_name(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def ascii_fold(s: str) -> str:
    """Decompose accented chars and strip combining marks: 'Kärki' → 'Karki'."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


# ── Manual encoding-corruption overrides ───────────────────────────────────────
# Some names in the 2016 Worlds mirror have encoding corruption (CP1252 characters
# not fixed by fix_encoding_corruption because they needed different mapping).
# Maps corrupted_name → correct_name (as it appears in PT or should appear).
ENCODING_FIXES = {
    "Patrik Èerný":       "Patrik Cerny",
    "Patrik Èerny":       "Patrik Cerny",
    "Krystof Maléø":      "Krystof Malér",
    "Dominik ¹imkù":      "Dominik Simku",
    "Dominik Šimku":      "Dominik Simku",
    "Barthélémy Meridjen":"Barthelemy Meridjen",
    "Timothée Lerolle":   "Timothee Lerolle",
    "Marcin Staroñ":      "Marcin Staron",
    "François Pelletier": "François Pelletier",   # keep accented, try match
    "Sébastien Maillet":  "Sébastien Maillet",
    "Tuomas Kärki":       "Tuomas Kärki",
    "Robin Püchel":       "Robin Puchel",
    "Filip Wójcik":       "Filip Wojcik",
}

# Hard-coded person_id overrides for names not matchable by normalisation alone.
# Includes 4 persons genuinely new to PT (added to PT v35 by this migration).
MANUAL_ID_OVERRIDES: dict[str, str] = {
    # "Alex Trenner" is "Alexander Trenner" in PT
    "Alex Trenner":             "24ffb4bb-a58e-54d8-b0d9-06b989c794cd",  # = Alexander Trenner
    # Genuinely new persons — also added to Persons_Truth_Final_v35.csv
    "Jean-Marie Letort":        "43ac12c4-b074-55ec-be51-6858775ac7c7",
    "Slava Sidorin":            "2862a91d-b120-5c46-a85d-1eb5efb6520e",
    "Sergio Hernandez Santiago":"89fd34b7-e602-535b-93bc-d43a686fbed4",
    "Sergio Hernández Santiago":"89fd34b7-e602-535b-93bc-d43a686fbed4",
    "Maude Landreville":        "f2ce846c-fa31-52e4-a88e-d8f7bccbe92e",
}


# ── Load PT ───────────────────────────────────────────────────────────────────

def load_pt() -> tuple[dict, dict, dict]:
    """Returns (canon_to_id, norm_to_id, ascii_to_id)."""
    canon_to_id: dict[str, str] = {}
    norm_to_id:  dict[str, str] = {}
    ascii_to_id: dict[str, str] = {}

    with open(PT_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid    = row["effective_person_id"].strip()
            canon  = row["person_canon"].strip()
            canon_to_id[canon.lower()] = pid
            norm_to_id[norm_name(canon)] = pid
            ascii_to_id[norm_name(ascii_fold(canon))] = pid

            for alias in row.get("aliases", "").split("|"):
                a = alias.strip()
                if a:
                    canon_to_id[a.lower()] = pid
                    norm_to_id[norm_name(a)] = pid
                    ascii_to_id[norm_name(ascii_fold(a))] = pid

    return canon_to_id, norm_to_id, ascii_to_id


def resolve_name(raw: str, canon_to_id: dict, norm_to_id: dict,
                 ascii_to_id: dict) -> tuple[str | None, str]:
    """
    Try to resolve a raw player name to a person_id.
    Returns (person_id or None, resolved_canon).
    Matching order: encoding-fix → manual override → exact →
                    normalised → ASCII-fold normalised.
    """
    # Apply encoding fix if needed
    name = ENCODING_FIXES.get(raw, raw)

    # Strip country suffix
    name_sc = strip_country(name)

    # Manual override (checked after country strip too)
    for attempt in (name, name_sc):
        if attempt in MANUAL_ID_OVERRIDES:
            return MANUAL_ID_OVERRIDES[attempt], attempt

    # Try exact (case-insensitive), normalised, and ASCII-folded match
    for attempt in (name_sc, name):
        if attempt.lower() in canon_to_id:
            return canon_to_id[attempt.lower()], attempt
        nk = norm_name(attempt)
        if nk in norm_to_id:
            return norm_to_id[nk], attempt
        ak = norm_name(ascii_fold(attempt))
        if ak in ascii_to_id:
            return ascii_to_id[ak], attempt

    return None, name_sc


# ── Team helpers ───────────────────────────────────────────────────────────────

def make_team_person_key(pid1: str, pid2: str) -> str:
    """Stable team key: sorted UUIDs joined by '|'."""
    parts = sorted([pid1, pid2])
    return "|".join(parts)


def make_unresolved_id(name: str) -> str:
    """Generate a deterministic UUID5 for an unresolved name."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"unresolved:{name.lower().strip()}"))


# ── Build new PBP rows ─────────────────────────────────────────────────────────

def build_new_rows(pj: list[dict], canon_to_id: dict, norm_to_id: dict,
                   ascii_to_id: dict) -> tuple[list[dict], list[str]]:
    """
    Convert stage2 placements for new divisions into PBP rows.
    Returns (rows, warnings).
    """
    rows: list[dict]    = []
    warnings: list[str] = []

    for p in pj:
        div = p["division_canon"]
        if div not in NEW_DIVISIONS:
            continue

        place       = str(p["place"])
        cat         = p["division_category"]
        ctype       = p["competitor_type"]
        p1_raw      = p.get("player1_name", "").strip()
        p2_raw      = p.get("player2_name", "").strip()

        def _row(pid: str, pcanon: str, team_key: str = "", team_display: str = "",
                 unresolved: str = "") -> dict:
            return {
                "event_id":         EVENT_ID,
                "year":             YEAR,
                "division_canon":   div,
                "division_category":cat,
                "place":            place,
                "competitor_type":  ctype,
                "person_id":        pid,
                "team_person_key":  team_key,
                "person_canon":     pcanon,
                "team_display_name":team_display,
                "coverage_flag":    COVERAGE_FLAG,
                "person_unresolved":unresolved,
                "norm":             "",
            }

        if ctype == "player":
            pid, canon = resolve_name(p1_raw, canon_to_id, norm_to_id, ascii_to_id)
            if pid is None:
                pid = make_unresolved_id(canon)
                warnings.append(f"UNRESOLVED  place={place}  [{div}]  {repr(p1_raw)} → {repr(canon)}")
                rows.append(_row("", canon, unresolved="1"))
            else:
                rows.append(_row(pid, canon))

        else:
            # Team: resolve both players
            pid1, canon1 = resolve_name(p1_raw, canon_to_id, norm_to_id, ascii_to_id)
            pid2, canon2 = resolve_name(p2_raw, canon_to_id, norm_to_id, ascii_to_id)

            # Determine display names (strip country for display)
            disp1 = strip_country(p1_raw)
            disp2 = strip_country(p2_raw)
            team_display = f"{disp1} / {disp2}"

            # Fall back to unresolved IDs if needed
            if pid1 is None:
                pid1 = make_unresolved_id(canon1)
                warnings.append(f"UNRESOLVED  place={place}  [{div}]  p1={repr(p1_raw)} → {repr(canon1)}")
            if pid2 is None:
                pid2 = make_unresolved_id(canon2)
                warnings.append(f"UNRESOLVED  place={place}  [{div}]  p2={repr(p2_raw)} → {repr(canon2)}")

            team_key = make_team_person_key(pid1, pid2)

            # PBP has one row per person in the team
            rows.append(_row(pid1, canon1, team_key, team_display,
                             "1" if pid1.startswith("00000") else ""))
            rows.append(_row(pid2, canon2, team_key, team_display,
                             "1" if pid2.startswith("00000") else ""))

    return rows, warnings


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print new rows and warnings, do not write output file")
    args = ap.parse_args()

    print("Loading Persons_Truth…")
    canon_to_id, norm_to_id, ascii_to_id = load_pt()
    print(f"  {len(canon_to_id)} name/alias entries")

    print("Loading stage2 placements for 2016 Worlds…")
    pj: list[dict] = []
    with open(S2_CSV, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] == EVENT_ID:
                pj = json.loads(row.get("placements_json") or "[]")
                break
    print(f"  {len(pj)} stage2 placements total")

    # Confirm existing lock baseline
    existing_divs: set[str] = set()
    with open(IN_PBP, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] == EVENT_ID:
                existing_divs.add(row["division_canon"])
    print(f"  {len(existing_divs)} divisions already in lock: {sorted(existing_divs)}")

    print("Building new PBP rows…")
    new_rows, warnings = build_new_rows(pj, canon_to_id, norm_to_id, ascii_to_id)
    print(f"  {len(new_rows)} new rows across {len(NEW_DIVISIONS)} divisions")

    if warnings:
        print(f"\n  {len(warnings)} unresolved names:")
        for w in warnings:
            print(f"    {w}")
    else:
        print("  All names resolved to known persons.")

    if args.dry_run:
        print("\n[dry-run] — not writing output file")
        print("\nSample new rows:")
        for r in new_rows[:8]:
            print(f"  place={r['place']:>2}  [{r['division_canon']}]  "
                  f"canon={r['person_canon']!r}  pid={r['person_id'][:8] if r['person_id'] else 'UNRES'}…")
        return

    # Write v38: copy v37, append new rows
    print(f"\nReading {IN_PBP.name}…")
    existing_rows: list[dict] = []
    fieldnames: list[str] = []
    with open(IN_PBP, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        existing_rows = list(reader)
    print(f"  {len(existing_rows)} existing rows")

    all_rows = existing_rows + new_rows
    print(f"Writing {OUT_PBP.name} ({len(all_rows)} rows)…")
    with open(OUT_PBP, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone. v37={len(existing_rows)} rows → v38={len(all_rows)} rows (+{len(new_rows)})")
    print(f"Output: {OUT_PBP}")


if __name__ == "__main__":
    main()
