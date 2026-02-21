#!/usr/bin/env python3
"""
06_resolve_unmapped.py — Gate 4: Resolve T1_UNMAPPED_PERSON_NAME warnings.

Mode 1: --generate
  Reads  out/qc_tier1_people_issues.jsonl — T1_UNMAPPED entries
  Reads  out/Placements_Flat.csv         — to find event/year/division evidence
  Reads  out/Persons_Truth.csv           — to find co-competitors and candidates
  Writes out/unmapped_review.csv         — human fills alias_target_id

  Columns in unmapped_review.csv:
    name              — the unmapped abbreviated name (e.g. "François D.")
    appearances       — count of placements using this name
    years             — pipe-separated distinct years
    events            — pipe-separated event_ids (first 5)
    divisions         — pipe-separated division_canons
    co_competitors    — persons from Persons_Truth seen in same events
    candidate_matches — persons whose canon starts with same first name + initial
    alias_target_id   — HUMAN: UUID of identified person, OR blank for auto-stub
    notes             — HUMAN: optional notes

Mode 2: --apply
  Reads  out/unmapped_review.csv (human-filled)
  Updates overrides/person_aliases.csv:
    - non-blank alias_target_id → add alias row pointing to that UUID
    - blank alias_target_id     → generate uuid5 stub, add stub alias row
  Idempotent: skips rows already present in aliases.

Usage:
  python 06_resolve_unmapped.py --generate
  # ... human fills alias_target_id for identifiable names, leaves blank for stubs ...
  python 06_resolve_unmapped.py --apply
  python 02p5_player_token_cleanup.py
  python qc_tier1_people.py
  python 04_build_analytics.py
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
OVERRIDES = ROOT / "overrides"

ISSUES_JSONL = OUT / "qc_tier1_people_issues.jsonl"
PLACEMENTS_CSV = OUT / "Placements_Flat.csv"
PERSONS_TRUTH_CSV = OUT / "Persons_Truth.csv"
UNMAPPED_REVIEW_CSV = OUT / "unmapped_review.csv"
ALIASES_CSV = OVERRIDES / "person_aliases.csv"

# Namespace UUID for deterministic stub generation
_STUB_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # UUID_NAMESPACE_URL


def _stub_uuid(name: str) -> str:
    """Generate a deterministic UUID5 for a stub abbreviated name."""
    return str(uuid.uuid5(_STUB_NAMESPACE, f"stub:unmapped:{name}"))


def _parse_name_from_example(example_value: str) -> str:
    """Extract name from 'Name (count=N)' format."""
    m = re.match(r"^(.+?)\s+\(count=\d+\)\s*$", example_value.strip())
    if m:
        return m.group(1).strip()
    return example_value.strip()


# ---------------------------------------------------------------------------
# --generate mode
# ---------------------------------------------------------------------------

def generate_review() -> int:
    """Produce unmapped_review.csv for human review."""
    for p in [ISSUES_JSONL, PLACEMENTS_CSV, PERSONS_TRUTH_CSV]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 2

    # Load T1_UNMAPPED issues
    unmapped_names: set[str] = set()
    with ISSUES_JSONL.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("check_id") != "T1_UNMAPPED_PERSON_NAME":
                continue
            name = _parse_name_from_example(obj.get("example_value", ""))
            if name:
                unmapped_names.add(name)

    if not unmapped_names:
        print("No T1_UNMAPPED_PERSON_NAME issues found. Nothing to do.")
        return 0

    print(f"Found {len(unmapped_names)} distinct unmapped names from issues file.")

    # Load Placements_Flat
    pf = pd.read_csv(PLACEMENTS_CSV, dtype=str).fillna("")
    print(f"Loaded {len(pf)} placements from {PLACEMENTS_CSV}")

    # Load Persons_Truth
    pt = pd.read_csv(PERSONS_TRUTH_CSV, dtype=str).fillna("")
    print(f"Loaded {len(pt)} persons from {PERSONS_TRUTH_CSV}")

    # Build event → persons dict from Placements_Flat (persons with known IDs)
    # Keys: event_id → set of person_canons
    event_to_persons: dict[str, set[str]] = {}
    for side in ["player1", "player2"]:
        id_col = f"{side}_person_id"
        canon_col = f"{side}_person_canon"
        if id_col not in pf.columns or canon_col not in pf.columns:
            continue
        sub = pf[(pf[id_col].str.strip() != "") & (pf[canon_col].str.strip() != "")]
        for _, row in sub[["event_id", canon_col]].iterrows():
            eid = str(row["event_id"]).strip()
            canon = str(row[canon_col]).strip()
            if eid and canon:
                event_to_persons.setdefault(eid, set()).add(canon)

    # Build candidate lookup: first_name+initial → list of person_canons
    # e.g. "François D." → candidates whose canon starts with "François "
    # We'll extract first_name from unmapped name and initial letter from last token
    def _first_name(name: str) -> str:
        parts = name.strip().split()
        return parts[0] if parts else ""

    def _last_initial(name: str) -> str:
        """Return the initial letter of the last significant token (stripped of '.')."""
        # For "François D." → "D"
        parts = name.strip().split()
        for part in reversed(parts):
            cleaned = part.rstrip(".")
            if cleaned and cleaned[0].isalpha():
                return cleaned[0].upper()
        return ""

    all_canons = pt["person_canon"].dropna().astype(str).str.strip().tolist()

    # Build rows
    rows: list[dict] = []
    for name in sorted(unmapped_names):
        # Find all placements with this name and no person_id
        name_rows_list: list[pd.DataFrame] = []
        for side in ["player1", "player2"]:
            nm_col = f"{side}_name"
            id_col = f"{side}_person_id"
            if nm_col not in pf.columns:
                continue
            mask = (pf[nm_col].str.strip() == name) & (pf[id_col].str.strip() == "")
            name_rows_list.append(pf[mask][["event_id", "year", "division_canon"]])

        if name_rows_list:
            name_df = pd.concat(name_rows_list, ignore_index=True)
        else:
            name_df = pd.DataFrame(columns=["event_id", "year", "division_canon"])

        appearances = len(name_df)
        years = " | ".join(sorted({str(v).strip() for v in name_df["year"] if str(v).strip()}))
        event_ids = sorted({str(v).strip() for v in name_df["event_id"] if str(v).strip()})
        events_str = " | ".join(event_ids[:5])
        divisions = " | ".join(sorted({str(v).strip() for v in name_df["division_canon"] if str(v).strip()}))

        # Co-competitors: persons seen in the same events (excluding stub blanks)
        co_set: set[str] = set()
        for eid in event_ids:
            co_set.update(event_to_persons.get(eid, set()))
        co_str = " | ".join(sorted(co_set)[:10])  # cap at 10

        # Candidate matches: persons whose canon starts with same first name
        # and whose last word starts with same initial
        first = _first_name(name)
        initial = _last_initial(name)
        candidates: list[str] = []
        for canon in all_canons:
            canon_parts = canon.split()
            if not canon_parts:
                continue
            # First name must match (case-insensitive)
            if canon_parts[0].lower() != first.lower():
                continue
            # If we have an initial, last word must start with it
            if initial and len(canon_parts) >= 2:
                last_word = canon_parts[-1].lstrip("(").rstrip(")")
                if last_word and last_word[0].upper() != initial:
                    continue
            candidates.append(canon)
        candidates_str = " | ".join(sorted(set(candidates))[:10])  # cap at 10

        rows.append({
            "name": name,
            "appearances": appearances,
            "years": years,
            "events": events_str,
            "divisions": divisions,
            "co_competitors": co_str,
            "candidate_matches": candidates_str,
            "alias_target_id": "",
            "notes": "",
        })

    # Sort by appearances DESC
    rows.sort(key=lambda r: -r["appearances"])

    OUT.mkdir(exist_ok=True)
    fieldnames = [
        "name", "appearances", "years", "events", "divisions",
        "co_competitors", "candidate_matches", "alias_target_id", "notes",
    ]
    with UNMAPPED_REVIEW_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote: {UNMAPPED_REVIEW_CSV} ({len(rows)} rows)")
    print()
    print("Next steps:")
    print("  1. Open out/unmapped_review.csv")
    print("  2. For each row, inspect co_competitors and candidate_matches")
    print("  3. If you can identify the person, fill alias_target_id with their UUID from Persons_Truth.csv")
    print("  4. Leave alias_target_id blank to auto-generate a stub UUID (valid decision)")
    print("  5. Run: python 06_resolve_unmapped.py --apply")
    return 0


# ---------------------------------------------------------------------------
# --apply mode
# ---------------------------------------------------------------------------

def apply_decisions() -> int:
    """Apply human decisions from unmapped_review.csv to person_aliases.csv."""
    alias_lock = OVERRIDES / "person_aliases.lock"
    if alias_lock.exists():
        print("ERROR: person_aliases.csv is frozen (overrides/person_aliases.lock exists).", file=sys.stderr)
        print("       Remove the lock file to add aliases.", file=sys.stderr)
        raise SystemExit(1)
    for p in [UNMAPPED_REVIEW_CSV, ALIASES_CSV, PERSONS_TRUTH_CSV]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            if p == UNMAPPED_REVIEW_CSV:
                print("       Run --generate first.", file=sys.stderr)
            return 2

    review = pd.read_csv(UNMAPPED_REVIEW_CSV, dtype=str).fillna("")
    print(f"Loaded {len(review)} review rows from {UNMAPPED_REVIEW_CSV}")

    # Build UUID → person_canon lookup from Persons_Truth
    pt = pd.read_csv(PERSONS_TRUTH_CSV, dtype=str).fillna("")
    pid_to_canon: dict[str, str] = {}
    for _, row in pt.iterrows():
        pid = str(row.get("effective_person_id", "")).strip()
        canon = str(row.get("person_canon", "")).strip()
        if pid and canon:
            pid_to_canon[pid] = canon

    # Load existing aliases (idempotency check)
    fieldnames_list: list[str] = []
    existing_rows: list[dict] = []
    with ALIASES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames_list = list(reader.fieldnames or ["alias", "person_id", "person_canon", "status", "notes"])
        for row in reader:
            existing_rows.append(dict(row))

    existing_aliases: set[str] = {row.get("alias", "").strip() for row in existing_rows}

    new_rows: list[dict] = []
    skipped = 0
    identified = 0
    stubbed = 0
    errors = 0

    for _, row in review.iterrows():
        name = str(row.get("name", "")).strip()
        target_id = str(row.get("alias_target_id", "")).strip()
        user_notes = str(row.get("notes", "")).strip()

        if not name:
            continue

        # Idempotency: skip if alias already exists
        if name in existing_aliases:
            print(f"  SKIP (already aliased): {name!r}")
            skipped += 1
            continue

        if target_id:
            # Identified: alias → known person UUID
            person_canon = pid_to_canon.get(target_id, "")
            if not person_canon:
                print(f"  ERROR: alias_target_id {target_id!r} not found in Persons_Truth for name {name!r}",
                      file=sys.stderr)
                errors += 1
                continue
            new_rows.append({
                "alias": name,
                "person_id": target_id,
                "person_canon": person_canon,
                "status": "verified",
                "notes": user_notes or "gate4:identified",
            })
            print(f"  IDENTIFIED: {name!r} → {person_canon!r} ({target_id})")
            identified += 1
        else:
            # Stub: generate deterministic UUID
            stub_id = _stub_uuid(name)
            new_rows.append({
                "alias": name,
                "person_id": stub_id,
                "person_canon": name,
                "status": "verified",
                "notes": user_notes or "stub:abbreviated_name",
            })
            print(f"  STUB:       {name!r} → {stub_id}")
            stubbed += 1

    if errors > 0:
        print(f"\nERROR: {errors} rows had invalid alias_target_id values. Fix and re-run.", file=sys.stderr)
        return 1

    if not new_rows:
        print("No new alias rows to add.")
        return 0

    # Append to person_aliases.csv
    # Ensure all fieldnames present
    for nr in new_rows:
        for fn in fieldnames_list:
            nr.setdefault(fn, "")

    with ALIASES_CSV.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_list, extrasaction="ignore")
        writer.writerows(new_rows)

    print()
    print(f"Added {len(new_rows)} rows to {ALIASES_CSV}")
    print(f"  Identified aliases:  {identified}")
    print(f"  Stub aliases:        {stubbed}")
    print(f"  Skipped (existing):  {skipped}")
    print()
    print("Next steps:")
    print("  python 02p5_player_token_cleanup.py   # re-assigns person_ids")
    print("  python qc_tier1_people.py             # verify T1_UNMAPPED = 0")
    print("  python 04_build_analytics.py          # rebuild analytics + Excel")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Gate 4: Resolve T1_UNMAPPED_PERSON_NAME warnings.")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--generate", action="store_true",
                     help="Generate out/unmapped_review.csv for human review.")
    grp.add_argument("--apply", action="store_true",
                     help="Apply decisions from out/unmapped_review.csv to person_aliases.csv.")
    args = parser.parse_args()

    if args.generate:
        return generate_review()
    else:
        return apply_decisions()


if __name__ == "__main__":
    sys.exit(main())
