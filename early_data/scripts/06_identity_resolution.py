#!/usr/bin/env python3
"""
06_identity_resolution.py — Match pre-1997 raw names against Persons_Truth.

Reads:
  early_data/placements/placements_flat.csv
  early_data/old_results/old_results_placements_flat.csv
  inputs/Persons_Truth.csv

Produces:
  early_data/identity/person_match_candidates.csv
  early_data/identity/unresolved_names.csv

Matching strategy (conservative — no guessing):
  1. Exact match against person_canon
  2. Case-insensitive match against person_canon
  3. Match against aliases field (exact, then case-insensitive)
  4. Match against player_names_seen field (post-1997 raw names logged for this person)

Team entries (slash-separated) are split into individual names first.
"""

import csv
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

PLACEMENTS_CSV   = REPO_ROOT / "early_data" / "placements" / "placements_flat.csv"
OR_PLACEMENTS_CSV = REPO_ROOT / "early_data" / "old_results" / "old_results_placements_flat.csv"
PERSONS_TRUTH    = REPO_ROOT / "inputs" / "Persons_Truth.csv"

OUTPUT_DIR       = REPO_ROOT / "early_data" / "identity"
CANDIDATES_CSV   = OUTPUT_DIR / "person_match_candidates.csv"
UNRESOLVED_CSV   = OUTPUT_DIR / "unresolved_names.csv"


# ---------------------------------------------------------------------------
# Name normalisation (for comparison only — raw values always preserved)
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Lowercase + collapse whitespace for comparison."""
    return re.sub(r"\s+", " ", s.strip().lower())


# ---------------------------------------------------------------------------
# Load Persons_Truth into lookup structures
# ---------------------------------------------------------------------------

def load_persons_truth(path: Path) -> tuple:
    """
    Returns:
      pt_rows         — list of full PT dicts
      exact_index     — norm(name) -> list of (person_id, person_canon, match_source)
    """
    pt_rows = list(csv.DictReader(open(path, encoding="utf-8")))

    # Index: normalised name → list of (pid, canon, match_source)
    index: dict = {}

    def add(name: str, pid: str, canon: str, match_source: str):
        key = norm(name)
        if not key:
            return
        if key not in index:
            index[key] = []
        # Avoid duplicate entries for the same pid
        if not any(e[0] == pid for e in index[key]):
            index[key].append((pid, canon, match_source))

    for row in pt_rows:
        pid   = row["effective_person_id"]
        canon = row["person_canon"]
        clean = row.get("person_canon_clean", "").strip()

        # 1. person_canon
        add(canon, pid, canon, "person_canon")

        # 2. person_canon_clean (if different)
        if clean and clean != canon:
            add(clean, pid, canon, "person_canon_clean")

        # 3. aliases (pipe-separated, may have leading/trailing spaces around "|")
        aliases_raw = row.get("aliases", "")
        if aliases_raw:
            for alias in re.split(r"\s*\|\s*", aliases_raw):
                alias = alias.strip()
                if alias:
                    add(alias, pid, canon, "alias")

        # 4. player_names_seen (pipe-separated historical raw names from post-1997 data)
        names_seen_raw = row.get("player_names_seen", "")
        if names_seen_raw:
            for name in re.split(r"\s*\|\s*", names_seen_raw):
                name = name.strip()
                if name:
                    add(name, pid, canon, "player_names_seen")

    return pt_rows, index


# ---------------------------------------------------------------------------
# Extract all unique raw names from placements
# ---------------------------------------------------------------------------

def extract_raw_names(placements: list) -> dict:
    """
    Returns dict: raw_name -> {'sources': set of source_file values}
    Names are extracted from player_raw (split on "/") and team_raw (split on "/").
    """
    names: dict = {}

    def add_name(raw: str, source: str):
        raw = raw.strip()
        if not raw or raw in ("?", "", "__UNKNOWN__"):
            return
        if raw not in names:
            names[raw] = {"sources": set()}
        names[raw]["sources"].add(source)

    for row in placements:
        src = row.get("source_file", "")
        pr  = row.get("player_raw", "").strip()
        tr  = row.get("team_raw", "").strip()

        # player_raw: split on "/" (team entries use "/" as separator)
        if pr:
            if "/" in pr:
                for part in pr.split("/"):
                    add_name(part, src)
            else:
                add_name(pr, src)

        # team_raw: always slash-separated
        if tr:
            for part in tr.split("/"):
                add_name(part, src)

    return names


# ---------------------------------------------------------------------------
# Match a single name against the index
# ---------------------------------------------------------------------------

def match_name(raw: str, index: dict) -> dict:
    """
    Returns a result dict with fields for the output CSV.
    """
    key = norm(raw)
    hits = index.get(key, [])

    if not hits:
        return {
            "raw_name":            raw,
            "person_id":           "",
            "person_canon":        "",
            "match_type":          "NONE",
            "match_source":        "",
            "candidate_person_ids": "",
            "confidence":          "LOW",
            "notes":               "",
        }

    # Check if all hits point to the same person_id (collapsed by alias/name variants)
    unique_pids = list({h[0] for h in hits})

    if len(unique_pids) == 1:
        pid, canon, msource = hits[0]
        # Determine match type precision
        canon_key = norm(canon)
        if raw == canon:
            match_type = "EXACT"
            confidence = "HIGH"
        elif key == canon_key:
            match_type = "CASE"
            confidence = "HIGH"
        else:
            match_type = "ALIAS"
            confidence = "HIGH"
        return {
            "raw_name":            raw,
            "person_id":           pid,
            "person_canon":        canon,
            "match_type":          match_type,
            "match_source":        msource,
            "candidate_person_ids": pid,
            "confidence":          confidence,
            "notes":               "",
        }
    else:
        # Ambiguous: multiple different persons share this normalised name
        cand_str = "|".join(sorted(unique_pids))
        canons   = "|".join(sorted({h[1] for h in hits}))
        return {
            "raw_name":            raw,
            "person_id":           "",
            "person_canon":        "",
            "match_type":          "AMBIGUOUS",
            "match_source":        "",
            "candidate_person_ids": cand_str,
            "confidence":          "LOW",
            "notes":               f"multiple persons: {canons}",
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Near-miss suggestion for unresolved names (informational only)
# ---------------------------------------------------------------------------

def token_set(name: str) -> set:
    """Lowercase tokens from a name, length >= 3 to skip short noise."""
    return {t for t in re.sub(r"[^a-z ]", "", name.lower()).split() if len(t) >= 3}


def _last_token(name: str) -> str:
    parts = re.sub(r"[^a-z ]", "", name.lower()).split()
    return parts[-1] if parts else ""


def find_near_miss(raw: str, pt_rows: list, max_suggestions: int = 2) -> str:
    """
    Return a note string listing PT names that share ≥1 significant token with raw.
    Surname (last token) matches are weighted higher than first-name-only matches.
    Never assigns a person_id — for human review only.
    """
    raw_tokens = token_set(raw)
    raw_surname = _last_token(raw)
    if not raw_tokens:
        return ""
    candidates = []
    for r in pt_rows:
        canon = r["person_canon"]
        pt_tokens = token_set(canon)
        pt_surname = _last_token(canon)
        shared = raw_tokens & pt_tokens
        if not shared:
            continue
        # Weight: surname match = 10 + shared count; first-name-only = shared count
        surname_bonus = 10 if (raw_surname and raw_surname == pt_surname) else 0
        candidates.append((surname_bonus + len(shared), canon, r["effective_person_id"]))
    candidates.sort(key=lambda x: -x[0])
    best = candidates[:max_suggestions]
    if not best:
        return ""
    parts = [f"{canon} ({pid[:8]})" for _, canon, pid in best]
    return "possible PT match: " + "; ".join(parts)


CANDIDATES_FIELDS = [
    "raw_name", "person_id", "person_canon", "match_type", "match_source",
    "candidate_person_ids", "confidence", "notes",
]
UNRESOLVED_FIELDS = ["raw_name", "notes"]


def main():
    print("=== 06_identity_resolution.py ===\n")

    # Load inputs
    print("Loading Persons_Truth…")
    pt_rows, pt_index = load_persons_truth(PERSONS_TRUTH)
    print(f"  {len(pt_rows)} persons, {len(pt_index)} normalised name keys\n")

    print("Loading placements…")
    plc_rows = list(csv.DictReader(open(PLACEMENTS_CSV, encoding="utf-8")))
    or_rows  = list(csv.DictReader(open(OR_PLACEMENTS_CSV, encoding="utf-8")))
    all_placements = plc_rows + or_rows
    print(f"  FBW/IFAB: {len(plc_rows)} rows")
    print(f"  OLD_RESULTS: {len(or_rows)} rows")
    print(f"  Total: {len(all_placements)} rows\n")

    # Extract unique raw names
    raw_names = extract_raw_names(all_placements)
    print(f"Unique raw names extracted: {len(raw_names)}")

    # Match each name; add near-miss suggestions for unresolved
    results = []
    for raw in sorted(raw_names.keys()):
        res = match_name(raw, pt_index)
        if res["match_type"] == "NONE":
            suggestion = find_near_miss(raw, pt_rows)
            if suggestion:
                res["notes"] = suggestion
        results.append(res)

    # Summary
    by_type: dict = {}
    for r in results:
        t = r["match_type"]
        by_type[t] = by_type.get(t, 0) + 1

    print(f"\nMatch summary:")
    for t in ["EXACT", "CASE", "ALIAS", "AMBIGUOUS", "NONE"]:
        print(f"  {t:10s}: {by_type.get(t, 0)}")
    print(f"  Total:      {len(results)}")

    matched  = [r for r in results if r["match_type"] not in ("NONE", "AMBIGUOUS")]
    unres    = [r for r in results if r["match_type"] == "NONE"]
    ambig    = [r for r in results if r["match_type"] == "AMBIGUOUS"]

    match_rate = len(matched) / len(results) * 100 if results else 0
    print(f"\nMatch rate: {len(matched)}/{len(results)} ({match_rate:.1f}%)")
    print(f"Unresolved: {len(unres)}")
    print(f"Ambiguous:  {len(ambig)}")

    # Write candidates
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CANDIDATES_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CANDIDATES_FIELDS)
        w.writeheader()
        w.writerows(results)
    print(f"\nWrote: {CANDIDATES_CSV.name}  ({len(results)} rows)")

    # Write unresolved
    unres_rows = [{"raw_name": r["raw_name"], "notes": r["notes"]} for r in unres]
    with open(UNRESOLVED_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=UNRESOLVED_FIELDS)
        w.writeheader()
        w.writerows(unres_rows)
    print(f"Wrote: {UNRESOLVED_CSV.name}  ({len(unres_rows)} rows)")

    # Print unresolved names for review
    if unres_rows:
        print(f"\nUnresolved names (with near-miss suggestions where found):")
        for r in sorted(unres_rows, key=lambda x: x["raw_name"]):
            note = f"  → {r['notes']}" if r["notes"] else ""
            print(f"  {r['raw_name']}{note}")

    if ambig:
        print(f"\nAmbiguous names (manual review required):")
        for r in ambig:
            print(f"  {r['raw_name']}  →  {r['notes']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
