#!/usr/bin/env python3
"""
patch_pbp_v70_to_v71.py

Generates PBP v71 by appending rows for all BLOCKER_GENUINE (event_id,
division_canon) pairs that are absent from PBP v70.

These come primarily from the magazine ingestion (01d_ingest_magazine_data.py):
  - 25 new pre-mirror synthetic events (2001980002 through 2001995002)
  - New divisions in existing pre-mirror events (2001980001, 2001981001,
    2001982001, 2001983001) filled in from magazine sources

Resolution strategy per player slot:
  1. person_aliases.csv lookup (exact, case-insensitive)
  2. PT exact match (case-insensitive on person_canon)
  3. PT normalised match (strip diacritics + punctuation)
  4. PT player_names_seen / aliases column
  5. Reversed "Last First" name order
  6. Single-token → __NON_PERSON__
  7. Multi-token, no match → person_unresolved=1

Output:
  inputs/identity_lock/Placements_ByPerson_v71.csv
  out/pbp_v71_patch_summary.md
"""

import csv
import hashlib
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO        = Path(__file__).resolve().parent.parent
OUT         = REPO / "out"
LOCK        = REPO / "inputs" / "identity_lock"
OVERRIDES   = REPO / "overrides"

PBP_IN      = LOCK / "Placements_ByPerson_v70.csv"
PBP_OUT     = LOCK / "Placements_ByPerson_v71.csv"
STAGE2      = OUT / "stage2_canonical_events.csv"
PT_CSV      = OUT / "Persons_Truth.csv"
ALIASES_CSV = OVERRIDES / "person_aliases.csv"
BLOCKER_RPT = OUT / "final_validation" / "source_coverage_blocker_report.csv"
SUMMARY_MD  = OUT / "pbp_v71_patch_summary.md"

# ── Normalisation helpers ────────────────────────────────────────────────────

_RE_NON_ALPHA = re.compile(r"[^a-z0-9 ]")


def _norm(s: str) -> str:
    s = s.lower().strip()
    nfd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return _RE_NON_ALPHA.sub("", s).strip()


def _norm_names_set(s: str) -> set:
    parts = re.split(r"[|,;]", (s or ""))
    return {_norm(p) for p in parts if p.strip()}


def _is_single_token(s: str) -> bool:
    return len(s.split()) == 1


def _looks_like_artifact(s: str) -> bool:
    s = s.strip()
    if re.search(r"[A-Z]{2}\s*-[A-Z]+\)?", s):
        return True
    if re.match(r"^[\d,. ]+$", s):
        return True
    if s.startswith(")"):
        s = s.lstrip(")").strip()
        if not s:
            return True
    if not s or not any(c.isalpha() for c in s):
        return True
    # Known non-person placeholders and locations
    _NON_PERSON_LITERALS = {"unknown", "london england"}
    if s.lower().strip() in _NON_PERSON_LITERALS:
        return True
    return False


def _clean_player(s: str) -> str:
    s = (s or "").strip()
    s = s.lstrip(")").strip()
    s = re.sub(r"\s+\d+[\d ,]+$", "", s).strip()
    s = re.sub(r"\s*[-–]\s*\d+$", "", s).strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s).strip()
    return s


def _is_artifact_division(dc: str) -> bool:
    dc = dc.strip()
    if dc.lower() == "unknown":
        return True
    if len(dc) < 3:
        return True
    if dc.endswith(".") and len(dc) > 20:
        return True
    return False


# ── Lookup tables ────────────────────────────────────────────────────────────

def load_aliases() -> dict:
    """Return {normalised_alias → {pid, canon}} from person_aliases.csv."""
    lookup: dict[str, dict] = {}
    if not ALIASES_CSV.exists():
        return lookup
    with open(ALIASES_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            alias  = row.get("alias", "").strip()
            pid    = row.get("person_id", "").strip()
            canon  = row.get("person_canon", "").strip()
            if alias and pid and canon:
                lookup[_norm(alias)] = {"pid": pid, "canon": canon}
    return lookup


def load_pt(alias_lookup: dict) -> tuple[dict, dict]:
    """
    Returns:
      norm_to_pt  — normalised person_canon → PT row
      alias_to_pt — normalised seen-name/alias → PT row
                    (includes person_aliases.csv entries)
    """
    norm_to_pt:  dict[str, dict] = {}
    alias_to_pt: dict[str, dict] = {}

    with open(PT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            canon = row.get("person_canon", "").strip()
            pid   = row.get("effective_person_id", "").strip()
            if not canon or not pid:
                continue
            row["_pid"]   = pid
            row["_canon"] = canon
            norm_to_pt[_norm(canon)] = row

            for field in ("player_names_seen", "aliases"):
                for alt in _norm_names_set(row.get(field, "")):
                    if alt and alt not in norm_to_pt:
                        alias_to_pt[alt] = row

    # Overlay person_aliases.csv entries into alias_to_pt
    for norm_alias, data in alias_lookup.items():
        pid   = data["pid"]
        canon = data["canon"]
        # Find PT row by pid
        pt_row = next((r for r in norm_to_pt.values() if r["_pid"] == pid), None)
        if pt_row and norm_alias not in norm_to_pt:
            alias_to_pt[norm_alias] = pt_row

    return norm_to_pt, alias_to_pt


def pt_lookup(name: str, norm_to_pt: dict, alias_to_pt: dict) -> dict | None:
    n = _norm(name)
    if n in norm_to_pt:
        return norm_to_pt[n]
    if n in alias_to_pt:
        return alias_to_pt[n]
    parts = n.split()
    if len(parts) == 2:
        rev = f"{parts[1]} {parts[0]}"
        if rev in norm_to_pt:
            return norm_to_pt[rev]
        if rev in alias_to_pt:
            return alias_to_pt[rev]
    return None


# ── Division helpers ─────────────────────────────────────────────────────────

def _division_category(dc: str) -> str:
    dcl = dc.lower()
    if any(k in dcl for k in ("net", "consecutive", "speed consecutive",
                               "distance", "one-step")):
        if "freestyle" not in dcl and "routine" not in dcl:
            return "net"
    if any(k in dcl for k in ("routine", "shred", "sick", "ironman", "freestyle",
                               "circle", "battle", "trick", "combo", "rippin",
                               "4-square", "2-square", "four square")):
        return "freestyle"
    if "golf" in dcl:
        return "golf"
    return "freestyle"


def _competitor_type(dc: str) -> str:
    if re.search(r"\bdoubles\b|\bdouble\b", dc.lower()):
        return "team"
    return "player"


def _coverage_flag(resolved: int, total: int) -> str:
    if total == 0:
        return "sparse"
    pct = resolved / total
    if pct == 1.0:
        return "complete"
    if pct >= 0.67:
        return "mostly_complete"
    if pct >= 0.25:
        return "partial"
    return "sparse"


def _team_key(eid, dc, place, n1, n2) -> str:
    raw = f"{eid}|{dc}|{place}|{min(n1,n2)}|{max(n1,n2)}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ── Stage2 loader ────────────────────────────────────────────────────────────

def load_stage2_placements(target_pairs: set) -> dict:
    result = defaultdict(list)
    with open(STAGE2, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            eid = row["event_id"].strip()
            try:
                pls = json.loads(row.get("placements_json") or "[]")
            except Exception:
                pls = []
            for p in pls:
                dc = (p.get("division_canon") or "").strip()
                if (eid, dc) not in target_pairs:
                    continue
                place = p.get("place")
                p1 = (p.get("player1_name") or "").strip()
                p2 = (p.get("player2_name") or "").strip()
                if place is not None and p1:
                    try:
                        result[(eid, dc)].append((int(float(place)), p1, p2))
                    except (ValueError, TypeError):
                        pass
    return result


def load_event_years() -> dict:
    years = {}
    with open(STAGE2, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            years[row["event_id"].strip()] = (row.get("year") or "").strip()
    return years


# ── PBP schema ───────────────────────────────────────────────────────────────

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]


def make_row(eid, year, dc, cat, place, ctype, tpk,
             pid, canon, tdisplay, cflag, unresolved, norm_val) -> dict:
    return {
        "event_id":          eid,
        "year":              year,
        "division_canon":    dc,
        "division_category": cat,
        "place":             str(place),
        "competitor_type":   ctype,
        "person_id":         pid,
        "team_person_key":   tpk,
        "person_canon":      canon,
        "team_display_name": tdisplay,
        "coverage_flag":     cflag,
        "person_unresolved": unresolved,
        "norm":              norm_val,
        "division_raw":      dc,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading person_aliases.csv …")
    alias_lookup = load_aliases()
    print(f"  {len(alias_lookup)} alias entries")

    print("Loading PT …")
    norm_to_pt, alias_to_pt = load_pt(alias_lookup)
    print(f"  PT entries: {len(norm_to_pt)}  alias entries: {len(alias_to_pt)}")

    print("Loading BLOCKER_GENUINE pairs …")
    genuine_pairs: list[tuple[str, str]] = []
    with open(BLOCKER_RPT, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["severity"] != "BLOCKER_GENUINE":
                continue
            dc = r["division_canon"]
            if _is_artifact_division(dc):
                continue
            genuine_pairs.append((r["event_id"], dc))

    target_set = set(genuine_pairs)
    print(f"  Target pairs: {len(target_set)}")

    print("Loading stage2 placements …")
    s2_pls = load_stage2_placements(target_set)
    years  = load_event_years()

    print("Generating new PBP rows …")
    new_rows: list[dict] = []
    stats = {"resolved": 0, "non_person": 0, "unresolved": 0, "artifact": 0}
    summary: list[dict] = []

    for eid, dc in sorted(target_set):
        placements = s2_pls.get((eid, dc), [])
        if not placements:
            continue

        year  = years.get(eid, "")
        cat   = _division_category(dc)
        ctype = _competitor_type(dc)
        is_team = (ctype == "team")

        div_rows: list[dict] = []
        resolved_cnt = 0
        total_slots  = 0

        seen_places: set = set()
        for place, p1_raw, p2_raw in sorted(placements, key=lambda x: x[0]):
            if place in seen_places:
                continue
            seen_places.add(place)

            p1 = _clean_player(p1_raw)
            p2 = _clean_player(p2_raw)

            if _looks_like_artifact(p1):
                stats["artifact"] += 1
                continue

            if is_team:
                p2_clean = p2 if p2 and not _looks_like_artifact(p2) else ""
                pt1 = pt_lookup(p1, norm_to_pt, alias_to_pt)
                pt2 = pt_lookup(p2_clean, norm_to_pt, alias_to_pt) if p2_clean else None

                tpk = _team_key(eid, dc, place,
                                pt1["_canon"] if pt1 else _norm(p1),
                                pt2["_canon"] if pt2 else _norm(p2_clean))

                n1 = pt1["_canon"] if pt1 else (p1 or "?")
                n2 = pt2["_canon"] if pt2 else (p2_clean or "?")
                tdisplay = f"{n1} / {n2}" if p2_clean else n1

                for pt_row, raw_name in [(pt1, p1), (pt2, p2_clean)]:
                    if not raw_name:
                        continue
                    total_slots += 1
                    if pt_row:
                        pid, canon, unres = pt_row["_pid"], pt_row["_canon"], ""
                        stats["resolved"] += 1
                        resolved_cnt += 1
                    elif _is_single_token(raw_name):
                        pid, canon, unres = "", "__NON_PERSON__", ""
                        stats["non_person"] += 1
                    else:
                        pid, canon, unres = "", raw_name, "1"
                        stats["unresolved"] += 1

                    div_rows.append(make_row(
                        eid, year, dc, cat, place, "team", tpk,
                        pid, canon, tdisplay, "", unres, _norm(canon)
                    ))
            else:
                total_slots += 1
                pt_row = pt_lookup(p1, norm_to_pt, alias_to_pt)
                if pt_row:
                    pid, canon, unres = pt_row["_pid"], pt_row["_canon"], ""
                    stats["resolved"] += 1
                    resolved_cnt += 1
                elif _is_single_token(p1):
                    pid, canon, unres = "", "__NON_PERSON__", ""
                    stats["non_person"] += 1
                else:
                    pid, canon, unres = "", p1, "1"
                    stats["unresolved"] += 1

                div_rows.append(make_row(
                    eid, year, dc, cat, place, "player", "",
                    pid, canon, "", "", unres, _norm(canon)
                ))

        cflag = _coverage_flag(resolved_cnt, total_slots)
        for r in div_rows:
            r["coverage_flag"] = cflag

        new_rows.extend(div_rows)
        summary.append({
            "event_id": eid, "year": year, "div": dc,
            "total": total_slots, "resolved": resolved_cnt, "coverage": cflag,
        })

    print(f"  New rows generated:   {len(new_rows)}")
    print(f"  Resolved (PT match):  {stats['resolved']}")
    print(f"  __NON_PERSON__:       {stats['non_person']}")
    print(f"  Unresolved:           {stats['unresolved']}")
    print(f"  Artifact-skipped:     {stats['artifact']}")

    # Write v71: base v70 rows + new rows
    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_IN, newline="", encoding="utf-8") as fin:
        base_data = list(csv.DictReader(fin))

    # No stale rows to drop from v70 (new events only; existing ones are additive)
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as fout:
        w = csv.DictWriter(fout, fieldnames=PBP_FIELDS)
        w.writeheader()
        for row in base_data:
            w.writerow({f: row.get(f, "") for f in PBP_FIELDS})
        w.writerows(new_rows)

    total = len(base_data) + len(new_rows)
    print(f"  Base (v70): {len(base_data):,} rows")
    print(f"  New rows:   {len(new_rows):,}")
    print(f"  v71 total:  {total:,} rows")

    # Summary report
    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("# PBP v70 → v71 Patch Summary\n\n")
        f.write(f"Source: magazine ingestion (01d_ingest_magazine_data.py)\n")
        f.write(f"Target pairs (BLOCKER_GENUINE): {len(target_set)}\n\n")
        f.write("## Resolution statistics\n\n")
        f.write("| Category | Count |\n|---|---|\n")
        f.write(f"| Resolved (PT match) | {stats['resolved']} |\n")
        f.write(f"| __NON_PERSON__ (single-name) | {stats['non_person']} |\n")
        f.write(f"| Unresolved (multi-token, no PT) | {stats['unresolved']} |\n")
        f.write(f"| Artifact-skipped | {stats['artifact']} |\n")
        f.write(f"| **Total new rows** | **{len(new_rows)}** |\n\n")
        f.write(f"Base (v70): {len(base_data):,} rows → v71: {total:,} rows (+{len(new_rows)})\n\n")
        f.write("## Per-division patch log\n\n")
        f.write("| event_id | year | division | slots | resolved | coverage |\n")
        f.write("|---|---|---|---|---|---|\n")
        for d in summary:
            f.write(f"| {d['event_id']} | {d['year']} | {d['div']} "
                    f"| {d['total']} | {d['resolved']} | {d['coverage']} |\n")

    print(f"  Summary: {SUMMARY_MD}")
    print("\nDone. Next: update pipeline config to use Placements_ByPerson_v71.csv")


if __name__ == "__main__":
    main()
