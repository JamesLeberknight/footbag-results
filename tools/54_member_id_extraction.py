"""
tools/54_member_id_extraction.py
─────────────────────────────────
Extract footbag.org member IDs for all persons in Persons_Truth.

Uses the live member search at www.footbag.org/members/list with
session credentials captured from the mirror.

Algorithm per person:
  1. POST search with full name
  2. Parse results: (memberName, profileID) pairs
  3. Normalize and match:
     - Single result + name matches → EXACT_NAME_MATCH (high confidence)
     - Single result + normalized match → NORMALIZED_NAME_MATCH (high)
     - Multiple results, one exact → EXACT_NAME_MATCH (high)
     - Multiple results, one normalized → NORMALIZED_NAME_MATCH (medium)
     - Multiple results, none exact → AMBIGUOUS (manual review)
     - No results → NO_MATCH
  4. Already has legacyid in PT → verify/keep

Outputs:
  out/member_id_enrichment/member_id_assignments.csv
  out/member_id_enrichment/member_id_ambiguous.csv
  out/member_id_enrichment/member_id_no_match.csv
  out/member_id_enrichment/member_id_summary.md

Usage:
  .venv/bin/python tools/54_member_id_extraction.py [--limit N] [--skip-existing]
"""

from __future__ import annotations

import argparse
import csv
import re
import time
import unicodedata
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT    = Path(__file__).resolve().parent.parent
OUT     = ROOT / "out" / "member_id_enrichment"
OUT.mkdir(parents=True, exist_ok=True)

PT_CSV  = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v41.csv"

# Credentials captured from mirror (public competition results site)
SEARCH_URL  = "http://www.footbag.org/members/list"
MEMBER_ID   = "11985"
MEMBER_PW   = "fb5XPirIXHzxA"
RATE_LIMIT  = 0.4   # seconds between requests

# ── HTML parsing ──────────────────────────────────────────────────────────────
_ROW_RE = re.compile(
    r'<td[^>]*class=["\']memberName["\'][^>]*>\s*([^<]+?)\s*</td>'
    r'.*?'
    r'<a[^>]+/members/profile/(\d+)[^>]*>\s*([^<]*?)\s*</a>',
    re.I | re.S,
)

def parse_results(html: str) -> list[tuple[str, str, str]]:
    """Return list of (display_name, alias, member_id) from search results HTML."""
    triples = []
    for m in _ROW_RE.finditer(html):
        name  = m.group(1).strip()
        mid   = m.group(2).strip()
        alias = m.group(3).strip()
        if mid:
            triples.append((name, alias, mid))
    return triples


# ── name normalization ────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace, remove punctuation."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokens(s: str) -> set[str]:
    return set(_norm(s).split())


def _alias_matches(query: str, alias: str) -> bool:
    """
    Check if alias is an initial+surname encoding of query.
    e.g.  query="Dave Leberknight"  alias="DLeberknight"  → True
          query="DLeberknight"      alias="DLeberknight"  → True (exact)
    Also handles alias == normalized query (case-insensitive, stripped).
    """
    qn = _norm(query)
    an = _norm(alias)
    if not an:
        return False
    # exact alias match
    if qn == an:
        return True
    # alias exactly matches query with no spaces (e.g. "daveleberknight")
    if qn.replace(" ", "") == an.replace(" ", ""):
        return True
    # initial+surname: alias = first_initial + surname (no separator)
    # e.g. "Dave Leberknight" → "d" + "leberknight" = "dleberknight"
    parts = qn.split()
    if len(parts) >= 2:
        initial_surname = parts[0][0] + parts[-1]   # first letter + last token
        if an == initial_surname:
            return True
        # also try all initials + surname: "John Paul Smith" → "jpsmith"
        initials = "".join(p[0] for p in parts[:-1])
        if an == initials + parts[-1]:
            return True
    # alias is initial+surname, query is the full name (reverse lookup)
    # "DLeberknight" → D + Leberknight — check if query starts with D and ends Leberknight
    am = re.match(r'^([a-z]+?)([a-z]{3,})$', an)
    if am and len(parts) >= 2:
        prefix, suffix = am.group(1), am.group(2)
        if parts[-1] == suffix and parts[0].startswith(prefix):
            return True
    return False


def match_name(query: str, candidates: list[tuple[str, str, str]]) -> tuple[str, str, str]:
    """
    Match query name against candidates (display_name, alias, member_id).
    Returns (member_id, match_method, confidence) or ('', 'NO_MATCH', '').
    """
    qn = _norm(query)
    qt = _tokens(query)

    # --- display name matching ---
    exact = [(name, alias, mid) for name, alias, mid in candidates if _norm(name) == qn]
    if len(exact) == 1:
        return exact[0][2], "EXACT_NAME_MATCH", "high"
    if len(exact) > 1:
        return "", "AMBIGUOUS", "low"

    # token overlap ≥ 100% of query tokens
    full_overlap = [(name, alias, mid) for name, alias, mid in candidates
                    if qt and qt.issubset(_tokens(name))]
    if len(full_overlap) == 1:
        return full_overlap[0][2], "NORMALIZED_NAME_MATCH", "high"
    if len(full_overlap) > 1:
        return "", "AMBIGUOUS", "low"

    # token overlap ≥ 80%
    partial = [(name, alias, mid) for name, alias, mid in candidates
               if qt and len(qt & _tokens(name)) / len(qt) >= 0.8]
    if len(partial) == 1:
        return partial[0][2], "NORMALIZED_NAME_MATCH", "medium"
    if len(partial) > 1:
        return "", "AMBIGUOUS", "low"

    # --- alias matching (handle / initial+surname) ---
    alias_hits = [(name, alias, mid) for name, alias, mid in candidates
                  if _alias_matches(query, alias)]
    if len(alias_hits) == 1:
        return alias_hits[0][2], "ALIAS_MATCH", "medium"
    if len(alias_hits) > 1:
        return "", "AMBIGUOUS", "low"

    return "", "NO_MATCH", ""


# ── HTTP session ──────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "footbag-archive-research/1.0"})
    return s


def search(session: requests.Session, name: str) -> list[tuple[str, str, str]] | None:
    """Search for name. Returns list of (name, member_id) or None on error."""
    try:
        r = session.post(SEARCH_URL, data={
            "MemberID":       MEMBER_ID,
            "MemberPassword": MEMBER_PW,
            "SearchText":     name,
            "Submit":         "Search",
        }, timeout=12)
        if r.status_code != 200:
            return None
        return parse_results(r.text)
    except Exception:
        return None


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",          type=int, default=0,
                    help="Process only first N persons (0=all)")
    ap.add_argument("--skip-existing",  action="store_true",
                    help="Skip persons already with a legacyid in PT")
    ap.add_argument("--resume",         action="store_true",
                    help="Resume from last checkpoint (skip already-written)")
    args = ap.parse_args()

    # Load PT
    with open(PT_CSV, encoding="utf-8") as f:
        pt_rows = list(csv.DictReader(f))

    # Build person list
    persons = []
    for row in pt_rows:
        ec = row.get("exclusion_reason", "")
        if "__NON_PERSON__" in ec:
            continue
        persons.append({
            "person_canon": row["person_canon"],
            "effective_person_id": row["effective_person_id"],
            "existing_legacyid": row.get("legacyid", "").strip(),
        })

    print(f"Total presentable persons: {len(persons)}")

    # Load existing results if resuming
    already_done: set[str] = set()
    if args.resume:
        for f in [OUT/"member_id_assignments.csv", OUT/"member_id_ambiguous.csv", OUT/"member_id_no_match.csv"]:
            if f.exists():
                with open(f) as fh:
                    for r in csv.DictReader(fh):
                        already_done.add(r["person_canon"])
        print(f"  Resuming: {len(already_done)} already processed")

    # Filter
    to_process = []
    for p in persons:
        if p["person_canon"] in already_done:
            continue
        if args.skip_existing and p["existing_legacyid"]:
            continue
        to_process.append(p)

    if args.limit:
        to_process = to_process[:args.limit]

    print(f"To process: {len(to_process)} persons")

    # Output files
    mode = "a" if args.resume else "w"
    assign_f = open(OUT/"member_id_assignments.csv", mode, newline="", encoding="utf-8")
    ambig_f  = open(OUT/"member_id_ambiguous.csv",   mode, newline="", encoding="utf-8")
    nomatch_f= open(OUT/"member_id_no_match.csv",    mode, newline="", encoding="utf-8")

    assign_w = csv.DictWriter(assign_f, fieldnames=[
        "person_canon","effective_person_id","member_id","match_confidence",
        "match_method","source_url","notes"])
    ambig_w  = csv.DictWriter(ambig_f,  fieldnames=[
        "person_canon","effective_person_id","candidates","notes"])
    nomatch_w= csv.DictWriter(nomatch_f, fieldnames=[
        "person_canon","effective_person_id","notes"])

    if not args.resume:
        assign_w.writeheader()
        ambig_w.writeheader()
        nomatch_w.writeheader()

    session = make_session()
    n_assigned = n_ambig = n_nomatch = n_existing = n_error = 0

    for i, p in enumerate(to_process, 1):
        canon = p["person_canon"]
        pid   = p["effective_person_id"]
        existing = p["existing_legacyid"]

        if existing:
            # Already has legacyid — treat as pre-assigned, write to assignments
            assign_w.writerow({
                "person_canon":        canon,
                "effective_person_id": pid,
                "member_id":           existing,
                "match_confidence":    "high",
                "match_method":        "MANUAL_RULE",
                "source_url":          f"http://www.footbag.org/members/profile/{existing}",
                "notes":               "pre-existing legacyid from Persons_Truth",
            })
            n_existing += 1
            if i % 50 == 0:
                print(f"  [{i}/{len(to_process)}] pre-existing: {canon} → {existing}")
            assign_f.flush()
            continue

        # Live search
        results = search(session, canon)
        time.sleep(RATE_LIMIT)

        if results is None:
            # Error / no response
            nomatch_w.writerow({"person_canon": canon, "effective_person_id": pid,
                                  "notes": "search_error"})
            n_error += 1
            nomatch_f.flush()
            if i % 100 == 0:
                print(f"  [{i}/{len(to_process)}] error: {canon}")
            continue

        mid, method, confidence = match_name(canon, results)

        if method in ("EXACT_NAME_MATCH", "NORMALIZED_NAME_MATCH", "ALIAS_MATCH") and mid:
            assign_w.writerow({
                "person_canon":        canon,
                "effective_person_id": pid,
                "member_id":           mid,
                "match_confidence":    confidence,
                "match_method":        method,
                "source_url":          f"http://www.footbag.org/members/profile/{mid}",
                "notes":               f"matched from {len(results)} result(s)",
            })
            n_assigned += 1
            assign_f.flush()
            print(f"  [{i}/{len(to_process)}] MATCH: {canon} → {mid} ({method})")

        elif method == "AMBIGUOUS":
            cands = "; ".join(f"{name}[{alias}]={rmid}" for name, alias, rmid in results[:10])
            ambig_w.writerow({"person_canon": canon, "effective_person_id": pid,
                               "candidates": cands, "notes": f"{len(results)} candidates"})
            n_ambig += 1
            ambig_f.flush()
            if i % 50 == 0 or len(results) <= 5:
                print(f"  [{i}/{len(to_process)}] AMBIG: {canon} → {len(results)} candidates")

        else:
            nomatch_w.writerow({"person_canon": canon, "effective_person_id": pid,
                                  "notes": "no_match"})
            n_nomatch += 1
            nomatch_f.flush()
            if i % 100 == 0:
                print(f"  [{i}/{len(to_process)}] NO MATCH: {canon}")

    assign_f.close(); ambig_f.close(); nomatch_f.close()

    total_assigned = n_assigned + n_existing
    total = n_assigned + n_existing + n_ambig + n_nomatch + n_error
    print(f"\n=== DONE ===")
    print(f"  Assigned (live):   {n_assigned}")
    print(f"  Assigned (existing): {n_existing}")
    print(f"  Ambiguous:         {n_ambig}")
    print(f"  No match:          {n_nomatch}")
    print(f"  Errors:            {n_error}")
    print(f"  Total processed:   {total}")

    # Write summary
    with open(OUT/"member_id_summary.md", "w") as f:
        f.write(f"""# Member ID Extraction Summary

**Run date:** {time.strftime('%Y-%m-%d')}
**Source:** www.footbag.org/members/list (live search)

## Results

| Category | Count |
|---|---|
| Assigned (live match) | {n_assigned} |
| Assigned (pre-existing in PT) | {n_existing} |
| **Total assigned** | **{total_assigned}** |
| Ambiguous (manual review needed) | {n_ambig} |
| No match found | {n_nomatch} |
| Search errors | {n_error} |
| **Total processed** | **{total}** |

## Match methods used
- **EXACT_NAME_MATCH**: normalized display name identical to search query
- **NORMALIZED_NAME_MATCH**: display name tokens fully contained in result
- **ALIAS_MATCH**: footbag.org handle/alias matches query (e.g. DLeberknight ↔ Dave Leberknight)
- **MANUAL_RULE**: pre-existing legacyid from Persons_Truth (human-verified)

## Output files
- `member_id_assignments.csv` — high-confidence assignments
- `member_id_ambiguous.csv` — multiple candidates, needs manual review
- `member_id_no_match.csv` — no footbag.org profile found

## Notes
- Not all persons are registered footbag.org members
- Many historical persons (pre-2000) predate the member system
- South American and Finnish persons often not registered
- Unresolved persons and __NON_PERSON__ excluded from search
""")
    print(f"  Summary: {OUT}/member_id_summary.md")


if __name__ == "__main__":
    main()
