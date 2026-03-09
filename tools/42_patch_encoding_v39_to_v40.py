#!/usr/bin/env python3
"""
tools/42_patch_encoding_v39_to_v40.py

Migrate Placements_ByPerson v39 → v40 and Persons_Truth v35 → v36.

Fixes:
  1. team_display_name encoding corruption: embedded '?' from ISO-8859-2 → ASCII
     lossy conversion in original mirror HTML. Applies targeted string fixes for
     known Czech (š/Š) and Irish (') patterns.
  2. Trailing '???' / '??' / '?' score annotations stripped from team names.
  3. PT person_canon corrections:
     - 'Klouda Vasek' → 'Vašek Klouda' (Last,First inversion + missing diacritic)
     - 'Dominik imku' merged into 'Dominik Šimků' (encoding corruption split same
       person into two rows; Dominik Simku renamed to Dominik Šimků)

Usage:
  python tools/42_patch_encoding_v39_to_v40.py [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

ROOT    = Path(__file__).parent.parent
IN_PBP  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v39.csv"
OUT_PBP = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v40.csv"
IN_PT   = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v35.csv"
OUT_PT  = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v36.csv"

# ---------------------------------------------------------------------------
# Person UUID constants
# ---------------------------------------------------------------------------
# Klouda Vasek (will be renamed Vašek Klouda in PT; stays as separate person)
UUID_KLOUDA_VASEK  = "8b1e5ce1-b6be-58dd-90c3-02bafdb34ec6"
# Dominik imku (corrupted stub — will be merged into Dominik Šimků)
UUID_DOMINIK_IMKU  = "2f7ec960-47a1-5c94-88f6-6f3aefd58623"
# Dominik Simku (will be renamed Dominik Šimků and absorbs Dominik imku rows)
UUID_DOMINIK_SIMKU = "b3a275df-fe1c-58b4-967b-d0396cec4211"

# ---------------------------------------------------------------------------
# team_display_name string fixes
# ---------------------------------------------------------------------------
# Applied in order; each is (pattern, replacement) for re.sub (flags=0).
# Keep specific patterns before general ones.
TDN_REGEX_FIXES = [
    # Czech: Last,First inversion in team display (Klouda Vasek → Vašek Klouda)
    (r"Klouda Vasek", "Vašek Klouda"),
    # Czech: š-caron corrupted to ?
    (r"Va\?ek",   "Vašek"),      # Vašek (dim. of Václav)
    (r"Vá\?k",    "Vášk"),       # Vášek / Vá?ka variant
    (r"\?imk",    "Šimk"),       # Šimků
    (r"\?andrik", "Šandrik"),    # Šandrik
    (r"Kry\?tof", "Kryštof"),    # Kryštof
    (r"Tomá\?",   "Tomáš"),      # Tomáš
    (r"Ale\?(?=\W|$)", "Aleš"),  # Aleš (word-boundary safe)
    # Irish: apostrophe corrupted to ?
    (r"O\?Brien", "O'Brien"),    # O'Brien
    # Czech: z without diacritic (all-caps events in mirror confirm "Struz")
    (r"Stru\?", "Struz"),        # Dexter Struz / Jan Struz
    # French: trailing accent lost (PT entry is "Martin Cote", ? = é)
    (r"Cote\?", "Coté"),         # Martin Coté
    # Strip trailing score-annotation question marks (e.g. "Name ???", "Name ??")
    (r"\s*\?{2,3}$", ""),
    # Strip single trailing unknown-partner marker after a word (e.g. "Marek ?")
    # but NOT after "/ " (which is the solo_in_doubles sentinel "Name / ?")
    (r"(\w)\s+\?$", r"\1"),
]


def fix_team_display_name(tdn: str) -> str:
    for pattern, repl in TDN_REGEX_FIXES:
        tdn = re.sub(pattern, repl, tdn)
    return tdn


# ---------------------------------------------------------------------------
# PT patch
# ---------------------------------------------------------------------------

def patch_pt(rows: list[dict], dry_run: bool) -> tuple[list[dict], dict]:
    """
    Returns (new_rows, uuid_remap) where uuid_remap maps old→new UUID for
    merged persons (so PBP can be updated).
    """
    uuid_remap: dict[str, str] = {}
    out = []
    dominik_simku_row = None

    for r in rows:
        pid = r["effective_person_id"]

        if pid == UUID_DOMINIK_IMKU:
            # Merge into Dominik Šimků — drop this row, redirect UUID
            uuid_remap[UUID_DOMINIK_IMKU] = UUID_DOMINIK_SIMKU
            if not dry_run:
                print(f"  PT DROP  : {r['person_canon']} ({pid[:8]}) → merged into Dominik Šimků")
            continue

        if pid == UUID_DOMINIK_SIMKU:
            # Rename to Dominik Šimků
            if not dry_run:
                print(f"  PT RENAME: {r['person_canon']} → Dominik Šimků")
            r = dict(r)
            r["person_canon"] = "Dominik Šimků"
            r["person_canon_clean"] = "Dominik Šimků"
            r["person_canon_clean_reason"] = ""
            dominik_simku_row = r

        elif pid == UUID_KLOUDA_VASEK:
            # Fix Last,First inversion + restore diacritic
            if not dry_run:
                print(f"  PT RENAME: {r['person_canon']} → Vašek Klouda")
            r = dict(r)
            r["person_canon"] = "Vašek Klouda"
            r["person_canon_clean"] = "Vašek Klouda"
            r["person_canon_clean_reason"] = ""

        out.append(r)

    return out, uuid_remap


# ---------------------------------------------------------------------------
# PBP patch
# ---------------------------------------------------------------------------

def patch_pbp(rows: list[dict], uuid_remap: dict[str, str], dry_run: bool) -> list[dict]:
    out = []
    tdn_fixed = 0
    uuid_redirected = 0

    for r in rows:
        r = dict(r)

        # Redirect merged person UUIDs
        pid = r.get("person_id", "")
        if pid in uuid_remap:
            new_pid = uuid_remap[pid]
            if not dry_run:
                print(f"  PBP REDIR: eid={r['event_id']} pid {pid[:8]}→{new_pid[:8]} "
                      f"({r.get('person_canon','')}) place={r.get('place','')}")
            r["person_id"] = new_pid
            # Update person_canon to match new UUID
            if new_pid == UUID_DOMINIK_SIMKU:
                r["person_canon"] = "Dominik Šimků"
            uuid_redirected += 1

        # Fix person_canon for renamed PT entries (solo rows)
        if r.get("person_id") == UUID_KLOUDA_VASEK:
            r["person_canon"] = "Vašek Klouda"
        if r.get("person_id") == UUID_DOMINIK_SIMKU:
            r["person_canon"] = "Dominik Šimků"

        # Fix team_display_name (encoding corruption + name inversions)
        tdn = r.get("team_display_name", "")
        if tdn:
            fixed = fix_team_display_name(tdn)
            if fixed != tdn:
                if not dry_run:
                    print(f"  TDN FIX  : {tdn!r} → {fixed!r}")
                r["team_display_name"] = fixed
                tdn_fixed += 1

        out.append(r)

    print(f"\nSummary: {tdn_fixed} team_display_name fixes, {uuid_redirected} UUID redirects")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dry_run = args.dry_run
    if dry_run:
        print("=== DRY RUN — no files written ===\n")

    # ── Load PT ─────────────────────────────────────────────────────────────
    with open(IN_PT, newline="", encoding="utf-8") as f:
        pt_reader = csv.DictReader(f)
        pt_fieldnames = pt_reader.fieldnames
        pt_rows = list(pt_reader)
    print(f"PT  v35: {len(pt_rows)} rows loaded from {IN_PT.name}")

    # ── Patch PT ─────────────────────────────────────────────────────────────
    new_pt, uuid_remap = patch_pt(pt_rows, dry_run)
    print(f"PT  v36: {len(new_pt)} rows (dropped {len(pt_rows)-len(new_pt)})")
    print(f"UUID remap: {uuid_remap}")

    # ── Load PBP ─────────────────────────────────────────────────────────────
    with open(IN_PBP, newline="", encoding="utf-8") as f:
        pbp_reader = csv.DictReader(f)
        pbp_fieldnames = pbp_reader.fieldnames
        pbp_rows = list(pbp_reader)
    print(f"\nPBP v39: {len(pbp_rows)} rows loaded from {IN_PBP.name}")

    # ── Patch PBP ─────────────────────────────────────────────────────────────
    new_pbp = patch_pbp(pbp_rows, uuid_remap, dry_run)
    print(f"PBP v40: {len(new_pbp)} rows")

    # ── Verify ? residual ────────────────────────────────────────────────────
    residual = [(i, r["team_display_name"]) for i, r in enumerate(new_pbp)
                if "?" in r.get("team_display_name", "")]
    if residual:
        print(f"\nResidual '?' in team_display_name ({len(residual)} rows):")
        for _, tdn in residual:
            print(f"  {tdn!r}")
    else:
        print("\nNo residual '?' in team_display_name.")

    if dry_run:
        return

    # ── Write PT v36 ─────────────────────────────────────────────────────────
    with open(OUT_PT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pt_fieldnames)
        w.writeheader()
        w.writerows(new_pt)
    print(f"\nWrote {OUT_PT.name} ({len(new_pt)} rows)")

    # ── Write PBP v40 ────────────────────────────────────────────────────────
    with open(OUT_PBP, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pbp_fieldnames)
        w.writeheader()
        w.writerows(new_pbp)
    print(f"Wrote {OUT_PBP.name} ({len(new_pbp)} rows)")


if __name__ == "__main__":
    main()
