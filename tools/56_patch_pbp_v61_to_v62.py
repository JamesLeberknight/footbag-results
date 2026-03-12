#!/usr/bin/env python3
"""
56_patch_pbp_v61_to_v62.py

Generates PBP v62 by appending rows for all legitimate BLOCKER_GENUINE
(event_id, division_canon) pairs — divisions present in stage2 but entirely
absent from PBP v61.

Resolution strategy per player slot:
  1. Exact PT match (case-insensitive)               → resolved
  2. Normalised PT match (strip diacritics, punct)    → resolved
  3. player_names_seen / aliases in PT               → resolved
  4. Single-token name or clear non-person noise      → __NON_PERSON__
  5. Multi-token, no PT match                        → person_unresolved=1

Artifact division names are skipped:
  - "Unknown" (parser could not determine division)
  - Names shorter than 3 characters
  - Obvious sentence fragments (ends with ".", length > 20)

Output:
  inputs/identity_lock/Placements_ByPerson_v62.csv
  out/pbp_v62_patch_summary.md
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

REPO = Path(__file__).resolve().parent.parent
OUT  = REPO / "out"
LOCK = REPO / "inputs" / "identity_lock"

PBP_V61    = LOCK / "Placements_ByPerson_v61.csv"
PBP_V62    = LOCK / "Placements_ByPerson_v62.csv"
STAGE2     = OUT / "stage2_canonical_events.csv"
PT_CSV     = OUT / "Persons_Truth.csv"
BLOCKER_RPT= OUT / "final_validation" / "source_coverage_blocker_report.csv"
SUMMARY_MD = OUT / "pbp_v62_patch_summary.md"

# Stale v61 rows that were parsed incorrectly and must be removed from the base.
# Each entry: event_id → set of division_canon values to drop from v61.
V61_STALE_ROWS: dict[str, set] = {
    # 14th Bembel Cup 2024: parser concatenated tab-separated doubles names as
    # "Open Singles Freestyle"; corrected to "Open Doubles Net" via RESULTS_FILE_OVERRIDE.
    "1706536250": {"Open Singles Freestyle"},
    # 42nd IFPA Worlds 2023: organizer mislabeled Women's Doubles Net as second "Women's
    # Singles Net" block. v61 Circle Contest and Women's Singles Net have doubled/wrong
    # entries from the merged parse. Replace all three with corrected stage2 data.
    "1678957450": {"Circle Contest", "Women's Singles Net", "Women's Doubles Net"},
}

# Divisions to patch regardless of blocker report (v61 had wrong data, not absent data).
# These will be included in target_set even if PF appears to have coverage.
FORCE_PATCH: set[tuple[str, str]] = {
    ("1678957450", "Circle Contest"),
    ("1678957450", "Women's Singles Net"),
    ("1678957450", "Women's Doubles Net"),
}

# ── Normalisation helpers ────────────────────────────────────────────────────

_RE_NON_ALPHA = re.compile(r"[^a-z0-9 ]")

def _norm(s: str) -> str:
    """Lowercase + strip diacritics + remove non-alphanumeric."""
    s = s.lower().strip()
    nfd = unicodedata.normalize("NFD", s)
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return _RE_NON_ALPHA.sub("", s).strip()


def _norm_names_set(s: str) -> set:
    """Return set of normalised name variants from a pipe/comma/semicolon delimited string."""
    parts = re.split(r"[|,;]", (s or ""))
    return {_norm(p) for p in parts if p.strip()}


def _is_single_token(s: str) -> bool:
    return len(s.split()) == 1


def _looks_like_artifact(s: str) -> bool:
    """True if the string is clearly not a player name."""
    s = s.strip()
    # Location artifacts from comma-split: "OR -USA) 71", "BC -CANADA)", "PA -USA) 56"
    if re.search(r"[A-Z]{2}\s*-[A-Z]+\)?", s):
        return True
    # Score/numeric artifacts: "56", "71", "3,66"
    if re.match(r"^[\d,. ]+$", s):
        return True
    # Leading ) from HTML list item
    if s.startswith(")"):
        s = s.lstrip(")").strip()
        if not s:
            return True
    # Empty
    if not s or not any(c.isalpha() for c in s):
        return True
    return False


def _clean_player(s: str) -> str:
    """Strip common noise from a player name string."""
    s = (s or "").strip()
    # Strip leading ) from HTML list items
    s = s.lstrip(")").strip()
    # Strip trailing score numbers like "121 31 33 3,66" (Footbagmania score format)
    s = re.sub(r"\s+\d+[\d ,]+$", "", s).strip()
    # Strip trailing score/rank like "(12)" or "- 56"
    s = re.sub(r"\s*[-–]\s*\d+$", "", s).strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s).strip()
    return s


def _is_artifact_division(dc: str) -> bool:
    """True for division names we should skip (parsing noise)."""
    dc_stripped = dc.strip()
    if dc_stripped.lower() == "unknown":
        return True
    # Too short / clearly not a division
    if len(dc_stripped) < 3:
        return True
    # Sentence fragment (contains words like "Pro Footbag" after a period/comma)
    if dc_stripped.endswith(".") and len(dc_stripped) > 20:
        return True
    return False


# ── PT lookup ────────────────────────────────────────────────────────────────

def load_pt() -> tuple[dict, dict]:
    """
    Returns:
      norm_to_pt  — normalised name → PT row
      alias_to_pt — normalised alias / seen-name → PT row
    """
    norm_to_pt:  dict[str, dict] = {}
    alias_to_pt: dict[str, dict] = {}

    with open(PT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            canon = row.get("person_canon", "").strip()
            pid   = row.get("effective_person_id", "").strip()
            if not canon or not pid:
                continue
            row["_pid"] = pid
            row["_canon"] = canon
            norm_key = _norm(canon)
            norm_to_pt[norm_key] = row

            # Index all seen names and aliases
            for field in ("player_names_seen", "aliases"):
                for alt in _norm_names_set(row.get(field, "")):
                    if alt and alt not in norm_to_pt:
                        alias_to_pt[alt] = row

    return norm_to_pt, alias_to_pt


def pt_lookup(name: str, norm_to_pt: dict, alias_to_pt: dict) -> dict | None:
    """Return PT row if name matches, else None."""
    n = _norm(name)
    if n in norm_to_pt:
        return norm_to_pt[n]
    if n in alias_to_pt:
        return alias_to_pt[n]
    # Try reversed order (Last First → First Last)
    parts = n.split()
    if len(parts) == 2:
        rev = f"{parts[1]} {parts[0]}"
        if rev in norm_to_pt:
            return norm_to_pt[rev]
        if rev in alias_to_pt:
            return alias_to_pt[rev]
    return None


# ── Division classification ───────────────────────────────────────────────────

_FREESTYLE_KW = {
    "routine", "shred", "sick", "ironman", "freestyle", "circle",
    "battle", "trick", "combo", "request", "big 3", "big3",
    "rippin", "4-square", "2-square", "four square", "two square",
    "golf"  # footbag golf is its own category but share freestyle-adjacent
}
_NET_KW = {"net", "singles", "doubles", "mixed", "consecutive",
           "speed", "net five", "net fives", "golf par"}


def _division_category(dc: str) -> str:
    dcl = dc.lower()
    if any(k in dcl for k in ("net", "consecutive", "speed consecutive")):
        if "freestyle" not in dcl and "routine" not in dcl:
            return "net"
    if any(k in dcl for k in ("routine", "shred", "sick", "ironman", "freestyle",
                               "circle", "battle", "trick", "combo", "rippin",
                               "4-square", "2-square", "four square", "two square")):
        return "freestyle"
    if "golf" in dcl:
        return "golf"
    return "freestyle"  # default for ambiguous


def _competitor_type(dc: str) -> str:
    dcl = dc.lower()
    if re.search(r"\bdoubles\b|\bdouble\b", dcl):
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


def _team_key(eid: str, dc: str, place: int, p1_norm: str, p2_norm: str) -> str:
    raw = f"{eid}|{dc}|{place}|{min(p1_norm, p2_norm)}|{max(p1_norm, p2_norm)}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ── Stage2 loader ─────────────────────────────────────────────────────────────

def load_stage2_placements(target_pairs: set) -> dict:
    """
    Returns {(eid, div): [(place, p1, p2), ...]} for the requested pairs.
    """
    result = defaultdict(list)
    with open(STAGE2, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            eid = row["event_id"].strip()
            try:
                pls = json.loads(row.get("placements_json") or "[]")
            except Exception:
                pls = []
            for p in pls:
                dc  = (p.get("division_canon") or "").strip()
                if (eid, dc) not in target_pairs:
                    continue
                place = p.get("place")
                p1    = (p.get("player1_name") or "").strip()
                p2    = (p.get("player2_name") or "").strip()
                if place is not None and p1:
                    try:
                        result[(eid, dc)].append((int(float(place)), p1, p2))
                    except (ValueError, TypeError):
                        pass
    return result


# ── Year lookup ───────────────────────────────────────────────────────────────

def load_event_years() -> dict:
    years = {}
    with open(STAGE2, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            years[row["event_id"].strip()] = (row.get("year") or "").strip()
    return years


# ── PBP schema ────────────────────────────────────────────────────────────────

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]


def make_row(eid, year, dc, cat, place, ctype, tpk,
             pid, canon, tdisplay, cflag, unresolved, norm_val) -> dict:
    return {
        "event_id":         eid,
        "year":             year,
        "division_canon":   dc,
        "division_category": cat,
        "place":            str(place),
        "competitor_type":  ctype,
        "person_id":        pid,
        "team_person_key":  tpk,
        "person_canon":     canon,
        "team_display_name": tdisplay,
        "coverage_flag":    cflag,
        "person_unresolved": unresolved,
        "norm":             norm_val,
        "division_raw":     dc,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading PT …")
    norm_to_pt, alias_to_pt = load_pt()
    print(f"  PT entries: {len(norm_to_pt)}  aliases: {len(alias_to_pt)}")

    print("Loading BLOCKER_GENUINE pairs …")
    genuine_pairs: list[tuple[str,str]] = []
    with open(BLOCKER_RPT, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["severity"] != "BLOCKER_GENUINE":
                continue
            dc = r["division_canon"]
            if _is_artifact_division(dc):
                continue
            genuine_pairs.append((r["event_id"], dc))

    target_set = set(genuine_pairs) | FORCE_PATCH
    if FORCE_PATCH:
        print(f"  Force-patched pairs:     {len(FORCE_PATCH)}")
    print(f"  Total target pairs:      {len(target_set)}")

    print("Loading stage2 placements for target pairs …")
    s2_pls = load_stage2_placements(target_set)
    years  = load_event_years()

    print("Generating new PBP rows …")
    new_rows: list[dict] = []
    stats = {"resolved": 0, "non_person": 0, "unresolved": 0, "artifact": 0}
    event_div_summary: list[dict] = []

    for (eid, dc) in sorted(target_set):
        placements = s2_pls.get((eid, dc), [])
        if not placements:
            continue

        year      = years.get(eid, "")
        cat       = _division_category(dc)
        ctype     = _competitor_type(dc)
        is_team   = (ctype == "team")

        div_rows: list[dict] = []
        resolved_cnt  = 0
        total_slots   = 0

        # Deduplicate placements by place (stage2 may have duplicates)
        seen_places: set = set()
        for place, p1_raw, p2_raw in sorted(placements, key=lambda x: x[0]):
            if place in seen_places:
                continue
            seen_places.add(place)

            p1 = _clean_player(p1_raw)
            p2 = _clean_player(p2_raw)

            # Skip artifactual player names
            if _looks_like_artifact(p1):
                stats["artifact"] += 1
                continue

            if is_team:
                # Doubles: one row per player, shared team_person_key
                p2_clean = p2 if p2 and not _looks_like_artifact(p2) else ""

                pt1  = pt_lookup(p1, norm_to_pt, alias_to_pt)
                pt2  = pt_lookup(p2_clean, norm_to_pt, alias_to_pt) if p2_clean else None

                tpk = _team_key(eid, dc, place,
                                pt1["_canon"] if pt1 else _norm(p1),
                                pt2["_canon"] if pt2 else _norm(p2_clean))

                # Display name
                n1 = pt1["_canon"] if pt1 else (p1 if p1 else "?")
                n2 = pt2["_canon"] if pt2 else (p2_clean if p2_clean else "?")
                tdisplay = f"{n1} / {n2}" if p2_clean else n1

                for pt_row, raw_name in [(pt1, p1), (pt2, p2_clean)]:
                    if not raw_name:
                        continue
                    total_slots += 1
                    if pt_row:
                        pid    = pt_row["_pid"]
                        canon  = pt_row["_canon"]
                        unres  = ""
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
                        pid, canon, tdisplay,
                        "",  # coverage_flag set after loop
                        unres, _norm(canon)
                    ))

            else:
                # Singles
                total_slots += 1
                pt_row = pt_lookup(p1, norm_to_pt, alias_to_pt)
                if pt_row:
                    pid    = pt_row["_pid"]
                    canon  = pt_row["_canon"]
                    unres  = ""
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
                    pid, canon, "",
                    "",  # coverage_flag set after loop
                    unres, _norm(canon)
                ))

        # Assign coverage_flag
        cflag = _coverage_flag(resolved_cnt, total_slots)
        for r in div_rows:
            r["coverage_flag"] = cflag

        new_rows.extend(div_rows)
        event_div_summary.append({
            "event_id":  eid,
            "year":      year,
            "div":       dc,
            "total":     total_slots,
            "resolved":  resolved_cnt,
            "coverage":  cflag,
        })

    print(f"  New rows generated: {len(new_rows)}")
    print(f"  Resolved:           {stats['resolved']}")
    print(f"  __NON_PERSON__:     {stats['non_person']}")
    print(f"  Unresolved:         {stats['unresolved']}")
    print(f"  Artifact-skipped:   {stats['artifact']}")

    # Write PBP v62 = v61 base + new rows (always rebuild from v61 for clean output)
    # Rows in the base whose (event_id, division_canon) are in target_set are excluded
    # (they were wrong in v61 and are being replaced by the patched rows above).
    base_file = PBP_V61
    print(f"Writing {PBP_V62} (base: {base_file.name}) …")
    # Read base rows into memory first (input and output may be same file)
    with open(base_file, newline="", encoding="utf-8") as fin:
        base_reader = csv.DictReader(fin)
        base_data = list(base_reader)
    # Exclude stale v61 rows: (a) pairs being patched, (b) explicit stale overrides
    def _is_stale(r: dict) -> bool:
        if (r["event_id"], r["division_canon"]) in target_set:
            return True
        stale_divs = V61_STALE_ROWS.get(r["event_id"])
        if stale_divs and r["division_canon"] in stale_divs:
            return True
        return False

    base_data_filtered = [r for r in base_data if not _is_stale(r)]
    excluded_base = len(base_data) - len(base_data_filtered)
    base_rows = len(base_data_filtered)
    with open(PBP_V62, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=PBP_FIELDS)
        writer.writeheader()
        for row in base_data_filtered:
            writer.writerow({f: row.get(f, "") for f in PBP_FIELDS})
        writer.writerows(new_rows)
    if excluded_base:
        print(f"  Excluded {excluded_base} stale v61 rows replaced by patch")

    total_rows = base_rows + len(new_rows)
    print(f"  Base rows: {base_rows}")
    print(f"  New rows:  {len(new_rows)}")
    print(f"  v62 total: {total_rows}")

    # Summary report
    v61_rows = base_rows  # for reporting
    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("# PBP v61 → v62 Patch Summary\n\n")
        f.write(f"Source: {len(target_set)} BLOCKER_GENUINE (event, division) pairs\n")
        f.write(f"(Artifact division names filtered before processing)\n\n")
        f.write("## Resolution statistics\n\n")
        f.write(f"| Category | Count |\n|---|---|\n")
        f.write(f"| Resolved (PT match) | {stats['resolved']} |\n")
        f.write(f"| __NON_PERSON__ (single-name) | {stats['non_person']} |\n")
        f.write(f"| Unresolved (multi-token, no PT match) | {stats['unresolved']} |\n")
        f.write(f"| Artifact-skipped (location/score noise) | {stats['artifact']} |\n")
        f.write(f"| **Total new rows** | **{len(new_rows)}** |\n\n")
        f.write(f"Base (v62): {base_rows} rows → v62 final: {total_rows} rows "
                f"(+{len(new_rows)})\n\n")
        f.write("## Per-division patch log\n\n")
        f.write("| event_id | year | division | slots | resolved | coverage |\n")
        f.write("|---|---|---|---|---|---|\n")
        for d in event_div_summary:
            f.write(f"| {d['event_id']} | {d['year']} | {d['div']} "
                    f"| {d['total']} | {d['resolved']} | {d['coverage']} |\n")

    print(f"  Summary: {SUMMARY_MD}")
    print()
    print("Done. Next step: update pipeline config to use Placements_ByPerson_v62.csv")


if __name__ == "__main__":
    main()
