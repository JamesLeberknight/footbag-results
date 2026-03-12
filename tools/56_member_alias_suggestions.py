"""
tools/56_member_alias_suggestions.py
─────────────────────────────────────
Post-process member_id_assignments.csv to generate alias suggestions.

For each ALIAS_MATCH row the person_canon didn't match via display name —
it matched via the footbag.org handle/alias (e.g. DLeberknight ↔ Dave Leberknight).
This tells us the person_canon and the footbag.org display name are different
forms of the same person, and we may need a new entry in person_aliases.csv.

Algorithm:
  1. Load ALIAS_MATCH rows from member_id_assignments.csv
  2. For each, fetch the member profile page to get their display name
  3. Cross-reference PT:
     a. If display_name already in PT as a different person_canon → suggest alias
        person_canon → that person_canon (or vice-versa)
     b. If display_name NOT in PT → suggest the person_canon may be a handle
        form; propose alias display_name → person_canon
  4. Skip aliases that already exist in person_aliases.csv
  5. Write out/member_id_enrichment/alias_suggestions.csv

Usage:
    .venv/bin/python tools/56_member_alias_suggestions.py [--limit N]
"""

from __future__ import annotations

import csv, re, sys, time, unicodedata, argparse
from pathlib import Path

csv.field_size_limit(sys.maxsize)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT         = Path(__file__).resolve().parent.parent
ASSIGNMENTS  = ROOT / "out" / "member_id_enrichment" / "member_id_assignments.csv"
PT_CSV       = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v41.csv"
ALIASES_CSV  = ROOT / "overrides" / "person_aliases.csv"
OUT_FILE     = ROOT / "out" / "member_id_enrichment" / "alias_suggestions.csv"

MEMBER_ID    = "11985"
MEMBER_PW    = "fb5XPirIXHzxA"
SEARCH_URL   = "http://www.footbag.org/members/list"
RATE_LIMIT   = 0.35


# ── helpers ───────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "footbag-archive-research/1.0"})
    return s


_ROW_RE = re.compile(
    r'<td[^>]*class=["\']memberName["\'][^>]*>\s*([^<]+?)\s*</td>'
    r'.*?'
    r'<a[^>]+/members/profile/(\d+)[^>]*>\s*([^<]*?)\s*</a>',
    re.I | re.S,
)

def fetch_display_name(session: requests.Session, person_canon: str, member_id: str) -> str:
    """
    Re-search for person_canon and find the result row with the given member_id.
    Returns the display_name of that member, or '' on failure.
    """
    try:
        r = session.post(SEARCH_URL, data={
            "MemberID":       MEMBER_ID,
            "MemberPassword": MEMBER_PW,
            "SearchText":     person_canon,
            "Submit":         "Search",
        }, timeout=12)
        if r.status_code != 200:
            return ""
        for m in _ROW_RE.finditer(r.text):
            if m.group(2).strip() == member_id:
                return m.group(1).strip()
        return ""
    except Exception:
        return ""


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    # Load ALIAS_MATCH rows
    alias_match_rows = []
    with open(ASSIGNMENTS, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("match_method", "") == "ALIAS_MATCH":
                alias_match_rows.append(row)

    print(f"ALIAS_MATCH rows: {len(alias_match_rows)}")
    if args.limit:
        alias_match_rows = alias_match_rows[:args.limit]

    # Build PT lookup: norm_key → person_canon, person_id
    pt_by_norm: dict[str, tuple[str, str]] = {}
    pt_canon_to_id: dict[str, str] = {}
    with open(PT_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pc  = row["person_canon"]
            pid = row["effective_person_id"]
            pt_by_norm[_norm(pc)] = (pc, pid)
            pt_canon_to_id[pc] = pid

    # Build existing alias set (norm) to skip duplicates
    existing_aliases: set[str] = set()
    with open(ALIASES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            existing_aliases.add(_norm(row.get("alias", "")))

    session = make_session()
    suggestions = []

    for i, row in enumerate(alias_match_rows, 1):
        person_canon = row["person_canon"]
        person_id    = row["effective_person_id"]
        member_id    = row["member_id"]

        # Re-search to find the display name for this member_id
        display_name = fetch_display_name(session, person_canon, member_id)
        time.sleep(RATE_LIMIT)

        if not display_name:
            print(f"  [{i}] {person_canon} → id={member_id} : could not fetch display name")
            continue

        dn_norm = _norm(display_name)
        pc_norm = _norm(person_canon)

        if dn_norm == pc_norm:
            # Same after normalization — alias already redundant
            continue

        print(f"  [{i}] {person_canon!r} → id={member_id} display_name={display_name!r}")

        # Case A: display_name is already a different person_canon in PT
        if dn_norm in pt_by_norm:
            canonical_pc, canonical_pid = pt_by_norm[dn_norm]
            if canonical_pid != person_id:
                # Different PT person — these might be the same person under two names
                # Suggest alias: person_canon → canonical_pc (or flag for review)
                if _norm(person_canon) not in existing_aliases:
                    suggestions.append({
                        "alias":         person_canon,
                        "person_id":     canonical_pid,
                        "person_canon":  canonical_pc,
                        "status":        "suggested",
                        "notes":         f"ALIAS_MATCH via member {member_id}; display_name={display_name!r}; review before applying",
                        "action":        "REVIEW_MERGE",
                    })
                    print(f"    → REVIEW_MERGE: {person_canon!r} may = PT:{canonical_pc!r}")
            else:
                # Same person — display_name is just another form; add as alias
                if dn_norm not in existing_aliases:
                    suggestions.append({
                        "alias":         display_name,
                        "person_id":     person_id,
                        "person_canon":  person_canon,
                        "status":        "suggested",
                        "notes":         f"ALIAS_MATCH via member {member_id}; footbag.org display name",
                        "action":        "ADD_ALIAS",
                    })
                    print(f"    → ADD_ALIAS: {display_name!r} → {person_canon!r}")
        else:
            # display_name not in PT — add as alias pointing to existing person_canon
            if dn_norm not in existing_aliases:
                suggestions.append({
                    "alias":         display_name,
                    "person_id":     person_id,
                    "person_canon":  person_canon,
                    "status":        "suggested",
                    "notes":         f"ALIAS_MATCH via member {member_id}; display name not yet in aliases",
                    "action":        "ADD_ALIAS",
                })
                print(f"    → ADD_ALIAS: {display_name!r} → {person_canon!r}")

    # Write suggestions
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "alias", "person_id", "person_canon", "status", "notes", "action"])
        w.writeheader()
        w.writerows(suggestions)

    add_count    = sum(1 for s in suggestions if s["action"] == "ADD_ALIAS")
    review_count = sum(1 for s in suggestions if s["action"] == "REVIEW_MERGE")

    print(f"\n=== DONE ===")
    print(f"  ALIAS_MATCH rows processed: {len(alias_match_rows)}")
    print(f"  ADD_ALIAS suggestions:      {add_count}")
    print(f"  REVIEW_MERGE suggestions:   {review_count}")
    print(f"  Written: {OUT_FILE}")


if __name__ == "__main__":
    main()
