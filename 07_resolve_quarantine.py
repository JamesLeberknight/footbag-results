#!/usr/bin/env python3
"""
07_resolve_quarantine.py — Gate 6: Resolve two-person concatenation quarantine entries.

Mode 1: --generate
  Reads  out/Persons_Truth_Quarantine_TwoPeople.csv
  Filters to quarantine_evidence starting with "split_known:"
  Writes out/quarantine_resolution.csv — human reviews and sets action per row

  Columns in quarantine_resolution.csv:
    old_person_canon  — the bad concatenated name (e.g. "John Smith Mary Jones")
    old_person_id     — UUID of the bad concat entry
    name_1            — first person parsed from split_known evidence
    name_2            — second person parsed from split_known evidence
    action            — default "split"; user may change to "skip" or "manual"
    notes             — blank for user annotations

Mode 2: --apply
  Reads  out/quarantine_resolution.csv (human-reviewed)
  For each action == "split" row:
    - If name_1 not in existing canons → add new stub alias
    - If name_2 not in existing canons → add new stub alias
    - Add old_person_canon → name_1 redirect (so old UUID's placements go to name_1)
  For action == "skip" rows: do nothing (quarantine entry stays excluded)
  Idempotent: skips rows already present in aliases.

Usage:
  python 07_resolve_quarantine.py --generate
  # ... human reviews out/quarantine_resolution.csv, sets action per row ...
  python 07_resolve_quarantine.py --apply
  python 04_build_analytics.py --force-identity
"""

from __future__ import annotations

import argparse
import csv
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
OVERRIDES = ROOT / "overrides"

QUARANTINE_CSV = OUT / "Persons_Truth_Quarantine_TwoPeople.csv"
RESOLUTION_CSV = OUT / "quarantine_resolution.csv"
ALIASES_CSV = OVERRIDES / "person_aliases.csv"
PERSONS_TRUTH_CSV = OUT / "Persons_Truth.csv"

# Namespace UUID for deterministic stub generation
_STUB_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # UUID_NAMESPACE_URL


def _stub_uuid(name: str) -> str:
    """Generate a deterministic UUID5 for a stub person name."""
    return str(uuid.uuid5(_STUB_NAMESPACE, f"stub:quarantine_split:{name}"))


# ---------------------------------------------------------------------------
# --generate mode
# ---------------------------------------------------------------------------

def generate_review() -> int:
    """Produce quarantine_resolution.csv for human review."""
    if not QUARANTINE_CSV.exists():
        print(f"ERROR: missing {QUARANTINE_CSV}", file=sys.stderr)
        print("       Run 04_build_analytics.py first.", file=sys.stderr)
        return 2

    quarantine = pd.read_csv(QUARANTINE_CSV, dtype=str).fillna("")
    print(f"Loaded {len(quarantine)} rows from {QUARANTINE_CSV}")

    # Filter to split_known rows
    split_rows = quarantine[quarantine["quarantine_evidence"].str.startswith("split_known:")].copy()
    print(f"  {len(split_rows)} rows with split_known evidence")

    if split_rows.empty:
        print("No split_known rows found. Nothing to do.")
        return 0

    rows: list[dict] = []
    parse_errors = 0

    for _, row in split_rows.iterrows():
        old_canon = str(row.get("person_canon", "")).strip()
        old_id = str(row.get("effective_person_id", "")).strip()
        evidence = str(row.get("quarantine_evidence", "")).strip()

        # Parse "split_known:Name1 || Name2"
        payload = evidence[len("split_known:"):]
        if "||" not in payload:
            print(f"  WARN: cannot parse evidence for {old_canon!r}: {evidence!r}", file=sys.stderr)
            parse_errors += 1
            continue

        parts = [p.strip() for p in payload.split("||", 1)]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            print(f"  WARN: malformed split for {old_canon!r}: {evidence!r}", file=sys.stderr)
            parse_errors += 1
            continue

        name_1, name_2 = parts[0], parts[1]

        rows.append({
            "old_person_canon": old_canon,
            "old_person_id": old_id,
            "name_1": name_1,
            "name_2": name_2,
            "action": "split",
            "notes": "",
        })

    if parse_errors:
        print(f"  {parse_errors} rows could not be parsed (see warnings above)")

    # Sort by old_person_canon for easy review
    rows.sort(key=lambda r: r["old_person_canon"])

    OUT.mkdir(exist_ok=True)
    fieldnames = ["old_person_canon", "old_person_id", "name_1", "name_2", "action", "notes"]
    with RESOLUTION_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote: {RESOLUTION_CSV} ({len(rows)} rows)")
    print()
    print("Next steps:")
    print("  1. Open out/quarantine_resolution.csv")
    print("  2. Review each row — default action is 'split'")
    print("  3. Change action to 'skip' to leave a quarantine entry excluded")
    print("  4. Change action to 'manual' if the split names need manual correction (fix name_1/name_2 too)")
    print("  5. Run: python 07_resolve_quarantine.py --apply")
    return 0


# ---------------------------------------------------------------------------
# --apply mode
# ---------------------------------------------------------------------------

def apply_decisions() -> int:
    """Apply human decisions from quarantine_resolution.csv to person_aliases.csv."""
    alias_lock = OVERRIDES / "person_aliases.lock"
    if alias_lock.exists():
        print("ERROR: person_aliases.csv is frozen (overrides/person_aliases.lock exists).", file=sys.stderr)
        print("       Remove the lock file to add aliases.", file=sys.stderr)
        raise SystemExit(1)

    for p in [RESOLUTION_CSV, ALIASES_CSV]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            if p == RESOLUTION_CSV:
                print("       Run --generate first.", file=sys.stderr)
            return 2

    resolution = pd.read_csv(RESOLUTION_CSV, dtype=str).fillna("")
    print(f"Loaded {len(resolution)} resolution rows from {RESOLUTION_CSV}")

    split_rows = resolution[resolution["action"].str.strip() == "split"]
    skip_rows = resolution[resolution["action"].str.strip() == "skip"]
    manual_rows = resolution[resolution["action"].str.strip() == "manual"]
    print(f"  split: {len(split_rows)}, skip: {len(skip_rows)}, manual: {len(manual_rows)}")

    if manual_rows.shape[0] > 0:
        print("\nWARN: 'manual' action rows — please fix name_1/name_2 and change action to 'split' or 'skip':")
        for _, r in manual_rows.iterrows():
            print(f"  {r['old_person_canon']!r} → name_1={r['name_1']!r}, name_2={r['name_2']!r}")
        print("Aborting. Fix manual rows and re-run.")
        return 1

    # Load existing aliases
    fieldnames_list: list[str] = []
    existing_rows: list[dict] = []
    with ALIASES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames_list = list(reader.fieldnames or ["alias", "person_id", "person_canon", "status", "notes"])
        for row in reader:
            existing_rows.append(dict(row))

    existing_aliases: set[str] = {row.get("alias", "").strip() for row in existing_rows}

    # Build canon → person_id lookup from existing aliases (for redirect case)
    # If name_1 or name_2 already exists as a person_canon, we find their ID
    canon_to_id: dict[str, str] = {}
    for row in existing_rows:
        canon = row.get("person_canon", "").strip()
        pid = row.get("person_id", "").strip()
        if canon and pid:
            canon_to_id[canon] = pid

    # Also load Persons_Truth if available for canon → ID lookup
    if PERSONS_TRUTH_CSV.exists():
        pt = pd.read_csv(PERSONS_TRUTH_CSV, dtype=str).fillna("")
        for _, row in pt.iterrows():
            pid = str(row.get("effective_person_id", "")).strip()
            canon = str(row.get("person_canon", "")).strip()
            if pid and canon and canon not in canon_to_id:
                canon_to_id[canon] = pid
        print(f"Loaded {len(pt)} persons from {PERSONS_TRUTH_CSV} for canon→ID lookup")

    new_rows: list[dict] = []
    skipped = 0
    stubs_added = 0
    redirects_added = 0

    for _, row in split_rows.iterrows():
        old_canon = str(row.get("old_person_canon", "")).strip()
        old_id = str(row.get("old_person_id", "")).strip()
        name_1 = str(row.get("name_1", "")).strip()
        name_2 = str(row.get("name_2", "")).strip()
        user_notes = str(row.get("notes", "")).strip()

        if not old_canon or not name_1 or not name_2:
            print(f"  WARN: incomplete row skipped: {row.to_dict()}", file=sys.stderr)
            skipped += 1
            continue

        # --- Handle name_1 ---
        if name_1 in existing_aliases:
            print(f"  SKIP stub (already aliased): {name_1!r}")
            skipped += 1
            name_1_id = canon_to_id.get(name_1, _stub_uuid(name_1))
        elif name_1 in canon_to_id:
            # Already exists as a known person — add redirect alias pointing to them
            name_1_id = canon_to_id[name_1]
            new_rows.append({
                "alias": name_1,
                "person_id": name_1_id,
                "person_canon": name_1,
                "status": "verified",
                "notes": user_notes or "quarantine_split:existing_person",
            })
            print(f"  REDIRECT stub: {name_1!r} → existing {name_1_id}")
            stubs_added += 1
        else:
            # New stub
            name_1_id = _stub_uuid(name_1)
            new_rows.append({
                "alias": name_1,
                "person_id": name_1_id,
                "person_canon": name_1,
                "status": "verified",
                "notes": user_notes or "stub:quarantine_split",
            })
            print(f"  STUB: {name_1!r} → {name_1_id}")
            stubs_added += 1

        # --- Handle name_2 ---
        if name_2 in existing_aliases:
            print(f"  SKIP stub (already aliased): {name_2!r}")
            skipped += 1
        elif name_2 in canon_to_id:
            name_2_id = canon_to_id[name_2]
            new_rows.append({
                "alias": name_2,
                "person_id": name_2_id,
                "person_canon": name_2,
                "status": "verified",
                "notes": user_notes or "quarantine_split:existing_person",
            })
            print(f"  REDIRECT stub: {name_2!r} → existing {name_2_id}")
            stubs_added += 1
        else:
            name_2_id = _stub_uuid(name_2)
            new_rows.append({
                "alias": name_2,
                "person_id": name_2_id,
                "person_canon": name_2,
                "status": "verified",
                "notes": user_notes or "stub:quarantine_split",
            })
            print(f"  STUB: {name_2!r} → {name_2_id}")
            stubs_added += 1

        # --- Add redirect: old_person_canon → name_1 ---
        # This ensures placements attributed to the old concat UUID get reassigned to name_1
        if old_canon in existing_aliases:
            print(f"  SKIP redirect (already aliased): {old_canon!r}")
            skipped += 1
        else:
            new_rows.append({
                "alias": old_canon,
                "person_id": name_1_id,
                "person_canon": name_1,
                "status": "verified",
                "notes": user_notes or f"quarantine_split:redirect_from_concat",
            })
            print(f"  REDIRECT: {old_canon!r} → {name_1!r} ({name_1_id})")
            redirects_added += 1

    if not new_rows:
        print("\nNo new alias rows to add.")
        return 0

    # Ensure all fieldnames present
    for nr in new_rows:
        for fn in fieldnames_list:
            nr.setdefault(fn, "")

    with ALIASES_CSV.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_list, extrasaction="ignore")
        writer.writerows(new_rows)

    print()
    print(f"Added {len(new_rows)} rows to {ALIASES_CSV}")
    print(f"  Stub aliases added:     {stubs_added}")
    print(f"  Redirect aliases added: {redirects_added}")
    print(f"  Skipped (existing):     {skipped}")
    print()
    print("Next steps:")
    print("  python 04_build_analytics.py --force-identity")
    print("  touch out/persons_truth.lock")
    print("  # Verify: Persons_Truth count increases, Gate 3 still PASS")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gate 6: Resolve two-person concatenation quarantine entries."
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--generate", action="store_true",
                     help="Generate out/quarantine_resolution.csv for human review.")
    grp.add_argument("--apply", action="store_true",
                     help="Apply decisions from out/quarantine_resolution.csv to person_aliases.csv.")
    args = parser.parse_args()

    if args.generate:
        return generate_review()
    else:
        return apply_decisions()


if __name__ == "__main__":
    sys.exit(main())
