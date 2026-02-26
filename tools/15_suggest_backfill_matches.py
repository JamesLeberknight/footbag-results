#!/usr/bin/env python3
"""
15_suggest_backfill_matches.py — Fuzzy-match AUTO_COVERAGE_BACKFILL unresolved entries
against Persons_Truth to suggest candidate identity resolutions.

Modes:

  --generate  (default)
    Reads:  inputs/identity_lock/Persons_Unresolved_Organized_v13.csv
    Reads:  inputs/identity_lock/Persons_Truth_Final_v15.csv
    Reads:  inputs/identity_lock/Placements_ByPerson_v15.csv  (for appearance counts)
    Writes: out/backfill_candidates.csv

    Output columns:
      unresolved_person_id  — person_id of this entry in Placements_ByPerson
      unresolved_canon      — person_canon from Unresolved
      token_count           — number of name tokens
      appearances           — placement count
      match_person_id       — effective_person_id of best Truth match
      match_canon           — person_canon of best Truth match
      score                 — WRatio score (0–100)
      confidence_tier       — HIGH (>=95) / MEDIUM (85-94) / LOW (75-84)
      decision              — HUMAN INPUT: "accept" / "reject" / blank
      notes                 — HUMAN INPUT: optional

  --apply
    Reads:  out/backfill_candidates.csv  (human-filled 'decision' column)
    Writes: out/backfill_resolutions.csv
      unresolved_person_id, resolved_to_person_id

    backfill_resolutions.csv is consumed by tools/18_migrate_identity_lock.py.

Usage:
  python tools/15_suggest_backfill_matches.py --generate
  # ... human reviews out/backfill_candidates.csv, fills 'decision' column ...
  # ... "accept" for confirmed matches, "reject" or blank to skip ...
  python tools/15_suggest_backfill_matches.py --apply
  python tools/18_migrate_identity_lock.py --apply  # incorporate resolutions
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:
    print("ERROR: rapidfuzz not installed. Run: pip install rapidfuzz", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"
OUT = ROOT / "out"

UNRESOLVED_CSV = IDENTITY_LOCK / "Persons_Unresolved_Organized_v13.csv"
TRUTH_CSV = IDENTITY_LOCK / "Persons_Truth_Final_v15.csv"
PLACEMENTS_CSV = IDENTITY_LOCK / "Placements_ByPerson_v15.csv"

CANDIDATES_CSV = OUT / "backfill_candidates.csv"
RESOLUTIONS_CSV = OUT / "backfill_resolutions.csv"

# ---------------------------------------------------------------------------
# Normalization helpers (borrowed from tools/12_generate_alias_suggestions.py)
# ---------------------------------------------------------------------------

_RE_PUNCT = re.compile(r"[^\w\s]")
_RE_MULTI_WS = re.compile(r"\s+")


def strip_diacritics(s: str) -> str:
    """Convert to NFKD and drop combining marks."""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_key(name: str) -> str:
    """
    Conservative normalization key:
      - strip diacritics
      - lowercase
      - remove punctuation
      - collapse whitespace
    """
    s = (name or "").strip()
    s = strip_diacritics(s)
    s = s.lower()
    s = _RE_PUNCT.sub(" ", s)
    s = _RE_MULTI_WS.sub(" ", s).strip()
    return s


# ---------------------------------------------------------------------------
# Confidence tier
# ---------------------------------------------------------------------------

TIER_HIGH = "HIGH"
TIER_MEDIUM = "MEDIUM"
TIER_LOW = "LOW"

TIER_ORDER = {TIER_HIGH: 0, TIER_MEDIUM: 1, TIER_LOW: 2}


def score_to_tier(score: float) -> str:
    if score >= 95:
        return TIER_HIGH
    if score >= 85:
        return TIER_MEDIUM
    return TIER_LOW


# ---------------------------------------------------------------------------
# --generate mode
# ---------------------------------------------------------------------------

def generate_candidates() -> int:
    for p in [UNRESOLVED_CSV, TRUTH_CSV, PLACEMENTS_CSV]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 2

    # Load Unresolved — filter to AUTO_COVERAGE_BACKFILL
    unresolved = pd.read_csv(UNRESOLVED_CSV, dtype=str).fillna("")
    acb = unresolved[unresolved["unresolved_class"] == "AUTO_COVERAGE_BACKFILL"].copy()
    print(f"Loaded {len(unresolved)} unresolved rows; {len(acb)} AUTO_COVERAGE_BACKFILL")

    # Load Persons_Truth
    truth = pd.read_csv(TRUTH_CSV, dtype=str).fillna("")
    print(f"Loaded {len(truth)} Persons_Truth entries")

    # Load Placements — to compute appearance counts and resolve person_id for ACB entries
    placements = pd.read_csv(PLACEMENTS_CSV, dtype=str).fillna("")
    print(f"Loaded {len(placements)} Placements_ByPerson entries")

    # Build appearance counts and dominant person_id per person_canon in Placements.
    # ACB rows have no effective_person_id in Unresolved — their person_id lives in Placements.
    # Group by person_canon to get (count, dominant_person_id).
    pf_canon_groups = (
        placements[placements["person_canon"] != ""]
        .groupby("person_canon")["person_id"]
        .agg(list)
        .reset_index()
    )
    pf_canon_groups.columns = ["person_canon", "person_ids"]

    def _dominant_pid(pid_list: list) -> str:
        """Return the person_id that appears most often (non-empty)."""
        counts: dict[str, int] = {}
        for pid in pid_list:
            pid = str(pid).strip()
            if pid:
                counts[pid] = counts.get(pid, 0) + 1
        if not counts:
            return ""
        return max(counts, key=lambda k: counts[k])

    canon_to_pid: dict[str, str] = {}
    canon_to_appearances: dict[str, int] = {}
    for _, row in pf_canon_groups.iterrows():
        canon = str(row["person_canon"]).strip()
        pid_list = row["person_ids"]
        canon_to_pid[canon] = _dominant_pid(pid_list)
        canon_to_appearances[canon] = len(pid_list)

    # Build Truth lookup for fuzzy matching
    # List of (effective_person_id, person_canon, norm_key)
    truth_entries: list[tuple[str, str, str]] = []
    for _, row in truth.iterrows():
        eid = str(row.get("effective_person_id", "")).strip()
        canon = str(row.get("person_canon", "")).strip()
        if eid and canon:
            truth_entries.append((eid, canon, normalize_key(canon)))

    print(f"Truth fuzzy pool: {len(truth_entries)} entries")

    # Process ACB entries
    # Filter: token_count >= 2
    acb["_token_count_int"] = pd.to_numeric(acb["token_count"], errors="coerce").fillna(0).astype(int)
    eligible = acb[acb["_token_count_int"] >= 2].copy()
    print(f"ACB entries with token_count >= 2: {len(eligible)}")

    rows: list[dict] = []
    skipped_no_appearances = 0
    skipped_no_match = 0
    matched = 0

    for _, row in eligible.iterrows():
        canon = str(row.get("person_canon", "")).strip()
        if not canon:
            continue

        token_count = int(row["_token_count_int"])
        appearances = canon_to_appearances.get(canon, 0)
        unresolved_pid = canon_to_pid.get(canon, "")

        # Skip entries with no placements
        if appearances < 1:
            skipped_no_appearances += 1
            continue

        norm = normalize_key(canon)

        # Fuzzy match against all Truth entries
        best_score: float = 0.0
        best_eid: str = ""
        best_match_canon: str = ""

        for (eid, match_canon, match_norm) in truth_entries:
            score = fuzz.WRatio(norm, match_norm)
            if score > best_score:
                best_score = score
                best_eid = eid
                best_match_canon = match_canon

        if best_score < 75:
            skipped_no_match += 1
            continue

        tier = score_to_tier(best_score)
        matched += 1

        rows.append({
            "unresolved_person_id": unresolved_pid,
            "unresolved_canon": canon,
            "token_count": token_count,
            "appearances": appearances,
            "match_person_id": best_eid,
            "match_canon": best_match_canon,
            "score": round(best_score, 1),
            "confidence_tier": tier,
            "decision": "",
            "notes": "",
        })

    print(f"Matched {matched} entries; skipped {skipped_no_appearances} (no appearances), "
          f"{skipped_no_match} (no match >= 75)")

    # Sort: HIGH first, then MEDIUM, then LOW; within tier by score desc
    rows.sort(key=lambda r: (TIER_ORDER[r["confidence_tier"]], -r["score"]))

    OUT.mkdir(exist_ok=True)
    fieldnames = [
        "unresolved_person_id",
        "unresolved_canon",
        "token_count",
        "appearances",
        "match_person_id",
        "match_canon",
        "score",
        "confidence_tier",
        "decision",
        "notes",
    ]
    with CANDIDATES_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    tier_counts = {t: 0 for t in [TIER_HIGH, TIER_MEDIUM, TIER_LOW]}
    for r in rows:
        tier_counts[r["confidence_tier"]] += 1

    print()
    print(f"Wrote: {CANDIDATES_CSV} ({len(rows)} candidate matches)")
    print(f"  HIGH   (score >= 95): {tier_counts[TIER_HIGH]}")
    print(f"  MEDIUM (score 85-94): {tier_counts[TIER_MEDIUM]}")
    print(f"  LOW    (score 75-84): {tier_counts[TIER_LOW]}")
    print()
    print("Next steps:")
    print("  1. Open out/backfill_candidates.csv")
    print("  2. Review HIGH tier first — fill decision='accept' for confirmed matches")
    print("  3. Fill decision='reject' or leave blank to skip a match")
    print("  4. Run: python tools/15_suggest_backfill_matches.py --apply")
    return 0


# ---------------------------------------------------------------------------
# --apply mode
# ---------------------------------------------------------------------------

def apply_decisions() -> int:
    if not CANDIDATES_CSV.exists():
        print(f"ERROR: missing {CANDIDATES_CSV}", file=sys.stderr)
        print("       Run --generate first.", file=sys.stderr)
        return 2

    candidates = pd.read_csv(CANDIDATES_CSV, dtype=str).fillna("")
    print(f"Loaded {len(candidates)} candidates from {CANDIDATES_CSV}")

    accepted = candidates[candidates["decision"].str.strip().str.lower() == "accept"]
    rejected = candidates[candidates["decision"].str.strip().str.lower() == "reject"]
    pending = candidates[candidates["decision"].str.strip() == ""]

    print(f"  accept:  {len(accepted)}")
    print(f"  reject:  {len(rejected)}")
    print(f"  pending: {len(pending)}")

    if len(accepted) == 0:
        print("No accepted decisions. Nothing to write.")
        return 0

    # Validate: match_person_id must be non-empty for all accepted rows
    bad = accepted[accepted["match_person_id"].str.strip() == ""]
    if len(bad) > 0:
        print(f"ERROR: {len(bad)} accepted rows have empty match_person_id:", file=sys.stderr)
        for _, row in bad.iterrows():
            print(f"  canon={row['unresolved_canon']!r}", file=sys.stderr)
        return 1

    # Note: unresolved_person_id may be empty for ACB entries whose person_canon appears
    # in Placements with blank person_id (encoding-corrupted names).  Those still need
    # Unresolved removal and Placements update-by-canon in tool 18.
    empty_pid = accepted[accepted["unresolved_person_id"].str.strip() == ""]
    if len(empty_pid) > 0:
        print(f"  (note: {len(empty_pid)} accepted rows have empty unresolved_person_id — "
              f"Placements will be updated by person_canon in tool 18)")

    # Write resolutions — include unresolved_canon so tool 18 can update Placements
    # by canon for blank-pid rows.
    fieldnames = ["unresolved_person_id", "unresolved_canon", "resolved_to_person_id"]
    OUT.mkdir(exist_ok=True)
    with RESOLUTIONS_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _, row in accepted.iterrows():
            writer.writerow({
                "unresolved_person_id": row["unresolved_person_id"].strip(),
                "unresolved_canon": row["unresolved_canon"].strip(),
                "resolved_to_person_id": row["match_person_id"].strip(),
            })

    print()
    print(f"Wrote: {RESOLUTIONS_CSV} ({len(accepted)} resolutions)")
    print()
    print("Next steps:")
    print("  python tools/18_migrate_identity_lock.py           # dry run")
    print("  python tools/18_migrate_identity_lock.py --apply   # apply all migrations")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fuzzy-match AUTO_COVERAGE_BACKFILL entries against Persons_Truth.",
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--generate", action="store_true", default=True,
                     help="Generate out/backfill_candidates.csv (default)")
    grp.add_argument("--apply", action="store_true",
                     help="Apply decisions from backfill_candidates.csv → backfill_resolutions.csv")
    args = parser.parse_args()

    if args.apply:
        return apply_decisions()
    return generate_candidates()


if __name__ == "__main__":
    sys.exit(main())
