#!/usr/bin/env python3
"""
tools/34_identity_suggestions.py — Identity Suggestion Layer

QC-only tool: produces candidate match suggestions for human review.
Does NOT modify any canonical data.

Mode A (default): finds unresolved/missing PBP names and proposes PT candidates.
Mode B (--near-dupes): scans PT for intra-PT near-duplicates by last_token grouping.

Usage:
    .venv/bin/python tools/34_identity_suggestions.py
    .venv/bin/python tools/34_identity_suggestions.py --near-dupes
    .venv/bin/python tools/34_identity_suggestions.py --major-only
    .venv/bin/python tools/34_identity_suggestions.py \
        --pbp inputs/identity_lock/Placements_ByPerson_v64.csv \
        --pt  inputs/identity_lock/Persons_Truth_Final_v42.csv
"""

import argparse
import csv
import json
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

from rapidfuzz.distance import JaroWinkler
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
INPUTS = ROOT / "inputs" / "identity_lock"
OUT = ROOT / "out"

DEFAULT_PT  = INPUTS / "Persons_Truth_Final_v42.csv"
DEFAULT_PBP = INPUTS / "Placements_ByPerson_v64.csv"
DEFAULT_ALIASES = ROOT / "overrides" / "person_aliases.csv"
STAGE2_EVENTS = OUT / "stage2_canonical_events.csv"

OUT_SUGGESTIONS = OUT / "identity_suggestions.csv"
OUT_NEAR_DUPES  = OUT / "identity_near_dupes.csv"

csv.field_size_limit(sys.maxsize)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HIGH_THRESHOLD   = 0.85
MEDIUM_THRESHOLD = 0.65
LOW_THRESHOLD    = 0.45

MAJOR_EVENT_NAMES = {"world", "us open", "pan am", "european", "national championship"}

TOP_N_CANDIDATES = 3  # emit top-N candidates per source name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    s = name.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = " ".join(s.split())
    return s


def score_name_pair(src: str, cand: str) -> tuple[float, float, float, bool]:
    """
    Score two full names.  Returns (score_total, score_last_jw, score_first_jw, last_name_exact).
    Both inputs should already be normalized (lowercase, no accents).
    """
    src_parts  = src.split()
    cand_parts = cand.split()

    if not src_parts or not cand_parts:
        return 0.0, 0.0, 0.0, False

    src_last  = src_parts[-1]
    src_first = src_parts[0]
    cand_last  = cand_parts[-1]
    cand_first = cand_parts[0]

    last_exact = src_last == cand_last
    last_jw    = JaroWinkler.similarity(src_last, cand_last)
    first_jw   = JaroWinkler.similarity(src_first, cand_first)
    full_ratio = fuzz.ratio(src, cand) / 100.0

    if last_exact:
        score = 0.50 + (first_jw * 0.40) + (full_ratio * 0.10)
    else:
        score = (last_jw * 0.45) + (first_jw * 0.30) + (full_ratio * 0.25)

    return score, last_jw, first_jw, last_exact


def confidence_bucket(score: float) -> str:
    if score >= HIGH_THRESHOLD:
        return "HIGH"
    if score >= MEDIUM_THRESHOLD:
        return "MEDIUM"
    if score >= LOW_THRESHOLD:
        return "LOW"
    return "BELOW"


def suggested_action(bucket: str) -> str:
    return {"HIGH": "LIKELY_ALIAS", "MEDIUM": "REVIEW", "LOW": "INSPECT"}.get(bucket, "")


def is_major_event(event_type: str, event_name: str) -> bool:
    if event_type and event_type.lower() == "worlds":
        return True
    en = event_name.lower()
    return any(kw in en for kw in MAJOR_EVENT_NAMES)


def match_reason(last_exact: bool, score_last: float, score_first: float) -> str:
    parts = []
    if last_exact:
        parts.append("last_exact")
    elif score_last >= 0.90:
        parts.append("last_near_exact")
    elif score_last >= 0.75:
        parts.append("last_similar")
    if score_first >= 0.90:
        parts.append("first_near_exact")
    elif score_first >= 0.75:
        parts.append("first_similar")
    elif score_first >= 0.55:
        parts.append("first_partial")
    return "+".join(parts) if parts else "fuzzy_full"


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_pt(pt_path: Path) -> list[dict]:
    with open(pt_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_pbp(pbp_path: Path) -> list[dict]:
    with open(pbp_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_stage2_events(events_path: Path) -> list[dict]:
    with open(events_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_aliases(aliases_path: Path) -> dict[str, str]:
    """Returns alias_norm → person_id mapping."""
    result: dict[str, str] = {}
    if not aliases_path.exists():
        return result
    with open(aliases_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            alias = row.get("alias", "").strip()
            pid   = row.get("person_id", "").strip()
            if alias and pid:
                result[normalize_name(alias)] = pid
    return result


# ---------------------------------------------------------------------------
# Build indices
# ---------------------------------------------------------------------------

def build_pt_indices(pt_rows: list[dict]) -> tuple[dict, dict, dict]:
    """
    Returns:
      by_id:         person_id → PT row
      by_last_token: last_token → [PT rows]   (exact last-name groups)
      norm_to_id:    norm_key  → person_id     (exact norm lookup)
    """
    by_id: dict[str, dict] = {}
    by_last_token: dict[str, list[dict]] = defaultdict(list)
    norm_to_id: dict[str, str] = {}

    for row in pt_rows:
        pid = row["effective_person_id"]
        by_id[pid] = row
        lt = (row.get("last_token") or "").lower().strip()
        if lt:
            by_last_token[lt].append(row)
        nk = (row.get("norm_key") or "").lower().strip()
        if nk:
            norm_to_id[nk] = pid

    return by_id, by_last_token, norm_to_id


def build_pbp_index(pbp_rows: list[dict]) -> dict[tuple, dict]:
    """
    Index PBP by (event_id, division_canon, place, competitor_type).
    Returns first match (should be unique per team slot, but teams have two rows).
    We index by person_id for fast lookup too.
    """
    # Primary: set of (event_id, division_canon, place, person_id) for fast existence check
    # We also want: event_id → list of PBP rows for status lookup
    by_event: dict[str, list[dict]] = defaultdict(list)
    for row in pbp_rows:
        by_event[row["event_id"]].append(row)
    return by_event


# ---------------------------------------------------------------------------
# Mode A: Unresolved name → PT candidate suggestions
# ---------------------------------------------------------------------------

def find_candidates(
    src_norm: str,
    by_last_token: dict,
    norm_to_id: dict,
    pt_by_id: dict,
    min_score: float,
    top_n: int,
) -> list[dict]:
    """
    Find top-N PT candidates for a normalized source name.
    Returns list of dicts sorted by score desc.
    """
    src_parts = src_norm.split()
    if not src_parts:
        return []

    src_last = src_parts[-1]

    # Collect candidate PT rows: exact last-token group first, then all PT
    candidate_ids_seen: set[str] = set()
    candidates: list[dict] = []

    # Tier 1: exact last-token match
    for pt_row in by_last_token.get(src_last, []):
        pid = pt_row["effective_person_id"]
        if pid in candidate_ids_seen:
            continue
        candidate_ids_seen.add(pid)
        cand_norm = normalize_name(pt_row["person_canon"])
        score, s_last, s_first, last_exact = score_name_pair(src_norm, cand_norm)
        if score >= min_score:
            candidates.append({
                "pid": pid,
                "name": pt_row["person_canon"],
                "score": score,
                "s_last": s_last,
                "s_first": s_first,
                "last_exact": last_exact,
            })

    # Tier 2: broad fuzzy scan (only if we have very few candidates from tier 1)
    if len(candidates) < top_n:
        for pt_row in pt_by_id.values():
            pid = pt_row["effective_person_id"]
            if pid in candidate_ids_seen:
                continue
            cand_norm = normalize_name(pt_row["person_canon"])
            cand_last = cand_norm.split()[-1] if cand_norm.split() else ""
            # Quick pre-filter: last-name JW must be at least 0.75 to be worth scoring
            if JaroWinkler.similarity(src_last, cand_last) < 0.75:
                continue
            candidate_ids_seen.add(pid)
            score, s_last, s_first, last_exact = score_name_pair(src_norm, cand_norm)
            if score >= min_score:
                candidates.append({
                    "pid": pid,
                    "name": pt_row["person_canon"],
                    "score": score,
                    "s_last": s_last,
                    "s_first": s_first,
                    "last_exact": last_exact,
                })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:top_n]


def run_mode_a(
    pt_path: Path,
    pbp_path: Path,
    aliases_path: Path,
    events_path: Path,
    major_only: bool,
) -> int:
    """Mode A: surface unresolved/missing PBP names with PT candidate suggestions."""
    print("=== Mode A: Unresolved Name Suggestions ===")
    print(f"  PT:      {pt_path}")
    print(f"  PBP:     {pbp_path}")
    print(f"  Events:  {events_path}")

    pt_rows   = load_pt(pt_path)
    pbp_rows  = load_pbp(pbp_path)
    events    = load_stage2_events(events_path)
    alias_map = load_aliases(aliases_path)  # norm → person_id

    print(f"  Loaded: {len(pt_rows)} PT rows, {len(pbp_rows)} PBP rows, {len(events)} events")

    pt_by_id, by_last_token, norm_to_id = build_pt_indices(pt_rows)
    pbp_by_event = build_pbp_index(pbp_rows)

    # Build a fast lookup: (event_id, division_canon, place) → PBP rows
    # For unresolved status checking
    pbp_lookup: dict[tuple, list[dict]] = defaultdict(list)
    for row in pbp_rows:
        key = (row["event_id"], row["division_canon"], row["place"], row["competitor_type"])
        pbp_lookup[key].append(row)

    # Build set of known resolved names: norm → True
    # A resolved entry has person_unresolved != '1' and person_id not empty
    resolved_norms: set[str] = set()
    for row in pbp_rows:
        if row.get("person_unresolved") != "1" and row.get("person_id"):
            n = (row.get("norm") or "").strip()
            if n and n != "__non_person__":
                resolved_norms.add(n)

    output_rows: list[dict] = []
    stats = {"total_source_names": 0, "unresolved_pbp": 0, "missing_pbp": 0, "suggestions_emitted": 0}

    for event in events:
        event_id   = event["event_id"]
        year       = event["year"]
        event_name = event["event_name"]
        event_type = event.get("event_type", "")
        major      = is_major_event(event_type, event_name)

        if major_only and not major:
            continue

        placements_json = event.get("placements_json") or "[]"
        try:
            placements = json.loads(placements_json)
        except json.JSONDecodeError:
            continue

        for p in placements:
            competitor_type = p.get("competitor_type", "player")
            division_canon  = p.get("division_canon", "")
            place           = str(p.get("place", ""))

            # Collect player names (player1 always present; player2 for doubles)
            players = [(p.get("player1_name", ""), "player1")]
            if competitor_type == "team" and p.get("player2_name"):
                players.append((p["player2_name"], "player2"))

            for raw_name, slot in players:
                raw_name = raw_name.strip()
                if not raw_name:
                    continue

                stats["total_source_names"] += 1
                src_norm = normalize_name(raw_name)

                # Skip obvious non-persons
                if src_norm in ("unknown", "__non_person__", ""):
                    continue

                # Check PBP status for this placement
                pbp_key = (event_id, division_canon, place, competitor_type)
                pbp_entries = pbp_lookup.get(pbp_key, [])

                pbp_status = "MISSING"
                partner_name = ""
                if pbp_entries:
                    # For teams, find the entry matching this player
                    matched_entry = None
                    for entry in pbp_entries:
                        entry_norm = (entry.get("norm") or "").strip().lower()
                        if entry_norm == src_norm or entry.get("person_canon", "").lower().strip() == src_norm:
                            matched_entry = entry
                            break
                    if matched_entry is None and pbp_entries:
                        matched_entry = pbp_entries[0]

                    if matched_entry:
                        if matched_entry.get("person_unresolved") == "1":
                            pbp_status = "UNRESOLVED"
                        else:
                            pbp_status = "RESOLVED"
                        # Grab partner from team_display_name if doubles
                        tdname = matched_entry.get("team_display_name", "")
                        if tdname and " / " in tdname:
                            parts = tdname.split(" / ")
                            partner_name = parts[1] if slot == "player1" else parts[0]

                if pbp_status == "RESOLVED":
                    # Already clean — skip unless we want to verify (we don't in Mode A)
                    continue

                if pbp_status == "UNRESOLVED":
                    stats["unresolved_pbp"] += 1
                else:
                    stats["missing_pbp"] += 1

                # Determine minimum score threshold
                min_score = LOW_THRESHOLD if major else MEDIUM_THRESHOLD

                # Check alias map first (exact alias match → skip, it's already handled)
                if src_norm in alias_map:
                    continue  # Already aliased — pipeline knows about it

                # Find candidates
                candidates = find_candidates(src_norm, by_last_token, norm_to_id, pt_by_id, min_score, TOP_N_CANDIDATES)

                if not candidates:
                    continue

                n_above = len(candidates)
                stats["suggestions_emitted"] += 1

                for rank, cand in enumerate(candidates, 1):
                    bucket = confidence_bucket(cand["score"])
                    # Skip LOW for non-major
                    if bucket == "LOW" and not major:
                        continue

                    output_rows.append({
                        "event_id":          event_id,
                        "year":              year,
                        "event_name":        event_name,
                        "event_type":        event_type,
                        "is_major":          "1" if major else "0",
                        "division_canon":    division_canon,
                        "place":             place,
                        "competitor_type":   competitor_type,
                        "source_name_raw":   raw_name,
                        "source_name_norm":  src_norm,
                        "pbp_status":        pbp_status,
                        "partner_name":      partner_name,
                        "candidate_rank":    rank,
                        "candidate_person_id": cand["pid"],
                        "candidate_name":    cand["name"],
                        "score_total":       f"{cand['score']:.4f}",
                        "score_last_name":   f"{cand['s_last']:.4f}",
                        "score_first_name":  f"{cand['s_first']:.4f}",
                        "last_name_exact":   "1" if cand["last_exact"] else "0",
                        "confidence_bucket": bucket,
                        "match_reason":      match_reason(cand["last_exact"], cand["s_last"], cand["s_first"]),
                        "suggested_action":  suggested_action(bucket),
                        "n_candidates_above_threshold": n_above,
                    })

    # Sort: HIGH first, then MEDIUM, then LOW; within bucket by score desc
    bucket_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    output_rows.sort(key=lambda r: (bucket_order.get(r["confidence_bucket"], 9), -float(r["score_total"])))

    # Write output
    fieldnames = [
        "event_id", "year", "event_name", "event_type", "is_major",
        "division_canon", "place", "competitor_type",
        "source_name_raw", "source_name_norm",
        "pbp_status", "partner_name",
        "candidate_rank", "candidate_person_id", "candidate_name",
        "score_total", "score_last_name", "score_first_name",
        "last_name_exact", "confidence_bucket",
        "match_reason", "suggested_action",
        "n_candidates_above_threshold",
    ]

    with open(OUT_SUGGESTIONS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(output_rows)

    print(f"\nStats:")
    print(f"  Source names scanned:     {stats['total_source_names']}")
    print(f"  Unresolved in PBP:        {stats['unresolved_pbp']}")
    print(f"  Missing from PBP:         {stats['missing_pbp']}")
    print(f"  Unique names w/ suggestions: {stats['suggestions_emitted']}")
    print(f"  Total suggestion rows:    {len(output_rows)}")
    print(f"\nOutput: {OUT_SUGGESTIONS}")

    # Summary by bucket
    by_bucket: dict[str, int] = defaultdict(int)
    for r in output_rows:
        if r["candidate_rank"] == 1:  # count unique suggestions, not all candidates
            by_bucket[r["confidence_bucket"]] += 1
    print(f"  HIGH (LIKELY_ALIAS):  {by_bucket['HIGH']}")
    print(f"  MEDIUM (REVIEW):      {by_bucket['MEDIUM']}")
    print(f"  LOW (INSPECT):        {by_bucket['LOW']}")

    return 0


# ---------------------------------------------------------------------------
# Mode B: Intra-PT near-duplicate detection
# ---------------------------------------------------------------------------

def get_pt_years(person_id: str, pbp_rows: list[dict]) -> list[str]:
    """Get sorted unique years for a person from PBP."""
    years = sorted(set(r["year"] for r in pbp_rows if r.get("person_id") == person_id))
    return years


def run_mode_b(pt_path: Path, pbp_path: Path) -> int:
    """Mode B: detect intra-PT near-duplicates by last_token grouping."""
    print("=== Mode B: Intra-PT Near-Duplicate Detection ===")
    print(f"  PT:  {pt_path}")
    print(f"  PBP: {pbp_path}")

    pt_rows  = load_pt(pt_path)
    pbp_rows = load_pbp(pbp_path)

    print(f"  Loaded: {len(pt_rows)} PT rows, {len(pbp_rows)} PBP rows")

    # Count appearances per person
    appearances: dict[str, int] = defaultdict(int)
    for row in pbp_rows:
        pid = row.get("person_id", "")
        if pid:
            appearances[pid] += 1

    # Build years index
    years_by_person: dict[str, list[str]] = defaultdict(list)
    for row in pbp_rows:
        pid = row.get("person_id", "")
        yr  = row.get("year", "")
        if pid and yr:
            years_by_person[pid].append(yr)
    years_by_person = {pid: sorted(set(yrs)) for pid, yrs in years_by_person.items()}

    # Group PT by last_token
    by_last_token: dict[str, list[dict]] = defaultdict(list)
    for row in pt_rows:
        lt = (row.get("last_token") or "").strip().lower()
        if lt:
            by_last_token[lt].append(row)

    output_rows: list[dict] = []
    stats = {"groups_checked": 0, "pairs_scored": 0, "pairs_above_threshold": 0}

    for last_token, group in by_last_token.items():
        if len(group) < 2:
            continue

        stats["groups_checked"] += 1

        # Score all pairs in this group
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a = group[i]
                b = group[j]

                a_norm = normalize_name(a["person_canon"])
                b_norm = normalize_name(b["person_canon"])

                score, s_last, s_first, last_exact = score_name_pair(a_norm, b_norm)
                stats["pairs_scored"] += 1

                bucket = confidence_bucket(score)
                if bucket == "BELOW":
                    continue

                stats["pairs_above_threshold"] += 1

                a_id = a["effective_person_id"]
                b_id = b["effective_person_id"]

                output_rows.append({
                    "person_a_id":         a_id,
                    "person_a_name":        a["person_canon"],
                    "person_a_appearances": appearances.get(a_id, 0),
                    "person_b_id":         b_id,
                    "person_b_name":        b["person_canon"],
                    "person_b_appearances": appearances.get(b_id, 0),
                    "shared_last_token":    last_token,
                    "score_first_name":     f"{s_first:.4f}",
                    "score_total":          f"{score:.4f}",
                    "confidence_bucket":    bucket,
                    "suggested_action":     suggested_action(bucket),
                    "years_a":              "|".join(years_by_person.get(a_id, [])),
                    "years_b":              "|".join(years_by_person.get(b_id, [])),
                })

    # Sort: HIGH first, then by score desc
    bucket_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    output_rows.sort(key=lambda r: (bucket_order.get(r["confidence_bucket"], 9), -float(r["score_total"])))

    fieldnames = [
        "person_a_id", "person_a_name", "person_a_appearances",
        "person_b_id", "person_b_name", "person_b_appearances",
        "shared_last_token",
        "score_first_name", "score_total",
        "confidence_bucket", "suggested_action",
        "years_a", "years_b",
    ]

    with open(OUT_NEAR_DUPES, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(output_rows)

    print(f"\nStats:")
    print(f"  Last-token groups checked: {stats['groups_checked']}")
    print(f"  Pairs scored:              {stats['pairs_scored']}")
    print(f"  Pairs above threshold:     {stats['pairs_above_threshold']}")
    print(f"\nOutput: {OUT_NEAR_DUPES}")

    by_bucket: dict[str, int] = defaultdict(int)
    for r in output_rows:
        by_bucket[r["confidence_bucket"]] += 1
    print(f"  HIGH (LIKELY_ALIAS):  {by_bucket['HIGH']}")
    print(f"  MEDIUM (REVIEW):      {by_bucket['MEDIUM']}")
    print(f"  LOW (INSPECT):        {by_bucket['LOW']}")

    # Spot-check: Jolene/Jody
    welch_pairs = [r for r in output_rows if r["shared_last_token"] == "welch"]
    if welch_pairs:
        print("\nValidation — Welch pairs found:")
        for r in welch_pairs:
            print(f"  {r['person_a_name']} vs {r['person_b_name']}: "
                  f"score={r['score_total']}, bucket={r['confidence_bucket']}")
    else:
        print("\nValidation — WARNING: no Welch near-dup pair found (check thresholds)")

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Identity suggestion layer — QC-only, no canonical changes"
    )
    parser.add_argument("--near-dupes", action="store_true",
                        help="Run Mode B: intra-PT near-duplicate detection")
    parser.add_argument("--major-only", action="store_true",
                        help="Mode A: only scan major events (lower LOW threshold)")
    parser.add_argument("--pbp", type=Path, default=DEFAULT_PBP,
                        help=f"Path to PBP CSV (default: {DEFAULT_PBP.name})")
    parser.add_argument("--pt",  type=Path, default=DEFAULT_PT,
                        help=f"Path to PT CSV (default: {DEFAULT_PT.name})")
    args = parser.parse_args()

    # Validate inputs
    for path, label in [(args.pt, "PT"), (args.pbp, "PBP"), (STAGE2_EVENTS, "stage2_events")]:
        if not path.exists():
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            return 1

    if args.near_dupes:
        rc = run_mode_b(args.pt, args.pbp)
    else:
        rc = run_mode_a(args.pt, args.pbp, DEFAULT_ALIASES, STAGE2_EVENTS, args.major_only)
        if not args.major_only:
            # Also run Mode B automatically when doing a full run
            print()
            rc |= run_mode_b(args.pt, args.pbp)

    return rc


if __name__ == "__main__":
    sys.exit(main())
