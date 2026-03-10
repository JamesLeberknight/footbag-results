"""
tools/53_rebuild_pbp_from_stage2.py

Systematic tool to fix PBP rows for events where:
  - PBP has duplicate (division_canon, place) pairs (garbled from old stage2)
  - Current stage2 has NO such duplicates for those same divisions

In these cases the current stage2 is authoritative and PBP is stale.
We replace the garbled PBP divisions with freshly resolved rows from stage2.

Person resolution order:
  1. Exact match on person_canon in PT (by norm)
  2. Exact match in existing PBP (by norm)
  3. Mark as partial/unresolved

Usage:
  python tools/53_rebuild_pbp_from_stage2.py [--apply] [--event EVENT_ID ...]

  Without --apply:  scan and report only (dry run)
  With    --apply:  write Placements_ByPerson_vNEW.csv

  --event EID ...   restrict to specific event IDs
"""

import argparse, csv, json, pathlib, re, unicodedata
from collections import defaultdict

ROOT       = pathlib.Path(__file__).parent.parent
STAGE2_CSV = ROOT / "out" / "stage2_canonical_events.csv"
PT_CSV     = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v36.csv"

# Determine current PBP version automatically
import glob as _glob
_pbp_files = sorted(_glob.glob(str(ROOT / "inputs/identity_lock/Placements_ByPerson_v*.csv")))
IN_PBP     = pathlib.Path(_pbp_files[-1])
_ver       = int(re.search(r'v(\d+)', IN_PBP.name).group(1))
OUT_PBP    = ROOT / "inputs" / "identity_lock" / f"Placements_ByPerson_v{_ver+1}.csv"

csv.field_size_limit(10**7)

# Events with known legitimate duplicates — skip these
# (real ties already correctly represented in PBP)
SKIP_EVENTS = {
    "1378666423",  # Danish Open 2013 — fixed in v45 (RC ties)
    "941418343",   # 2000 Worlds — handled separately (Women's div split)
    "915561090",   # 1999 Worlds — recovered via legacy file
    "1035277529",  # 2003 Worlds — recovered via legacy file
    "1741024635",  # 2025 Worlds — fixed in v46
}

# Manual per-event division overrides where stage2 is also wrong
# format: event_id -> {division_canon: [(place, player1_name, player2_name), ...]}
MANUAL_OVERRIDES: dict[str, dict] = {
    # SOUF 2004: stage2 misparsed Big3 as a second Shred30
    "1076952530": {
        "Routines": [
            ("1", "Scott Bevier",       ""),
            ("2", "Andrew Coleman",     ""),
            ("3", "Jonathan Schneider", ""),
        ],
        "Shred 30": [
            ("1", "Josh Benham",    ""),
            ("2", "Scott Bevier",   ""),
            ("3", "Andrew Coleman", ""),
        ],
        "Big 3": [
            ("1", "Scott Bevier",   ""),
            ("2", "Andrew Coleman", ""),
            ("3", "Josh Benham",    ""),
        ],
    },
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def load_pt(pt_csv: pathlib.Path) -> dict[str, str]:
    """norm -> person_id mapping from Persons_Truth."""
    result = {}
    with open(pt_csv, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid   = row["effective_person_id"]
            canon = row["person_canon"]
            if pid and canon:
                result[_norm(canon)] = pid
                result[canon.lower()] = pid
    return result


def load_stage2(csv_path: pathlib.Path) -> dict[str, list[dict]]:
    """event_id -> list of placement dicts from stage2."""
    result = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            eid = row["event_id"]
            placements = json.loads(row.get("placements_json", "[]"))
            result[eid] = placements
    return result


def has_stage2_duplicates(placements: list[dict], divisions: set[str]) -> bool:
    """Return True if stage2 has any (div, place) duplicates in the given divisions."""
    seen: set = set()
    for p in placements:
        div = p["division_canon"]
        if div not in divisions:
            continue
        key = (div, str(p["place"]))
        if key in seen:
            return True
        seen.add(key)
    return False


def resolve_person(name: str, pt_norms: dict, pbp_norms: dict) -> tuple[str, str, str]:
    """
    Return (person_id, person_canon, coverage_flag).
    Tries PT first, then existing PBP norms.
    """
    if not name or name.startswith("__"):
        return ("", "__NON_PERSON__", "complete")
    n = _norm(name)
    if n in pt_norms:
        pid = pt_norms[n]
        # find canon from reverse lookup
        canon = _PT_ID_TO_CANON.get(pid, name)
        return (pid, canon, "complete")
    if n in pbp_norms:
        pid, canon = pbp_norms[n]
        return (pid, canon, "complete")
    return ("", name, "partial")


# populated lazily
_PT_ID_TO_CANON: dict[str, str] = {}


def build_replacement_rows(
    eid: str,
    year: str,
    stage2_placements: list[dict],
    divs_to_replace: set[str],
    template_row: dict,
    pt_norms: dict,
    pbp_norms: dict,
    fieldnames: list[str],
) -> list[dict]:
    """Build clean PBP rows for the given divisions from stage2 data."""
    rows = []
    for p in stage2_placements:
        div = p["division_canon"]
        if div not in divs_to_replace:
            continue
        cat  = p.get("division_category", "freestyle")
        comp = p.get("competitor_type", "player")
        p1   = p.get("player1_name", "")
        p2   = p.get("player2_name", "")

        if comp == "team" and p1 and p2:
            pid1, can1, _ = resolve_person(p1, pt_norms, pbp_norms)
            pid2, can2, _ = resolve_person(p2, pt_norms, pbp_norms)
            # Build team row
            tpk  = "|".join(sorted(filter(None, [pid1, pid2])))
            disp = f"{can1} / {can2}" if can1 and can2 else (p1 + " / " + p2)
            r = template_row.copy()
            r["event_id"]         = eid
            r["year"]             = year
            r["division_canon"]   = div
            r["division_category"]= cat
            r["place"]            = str(p["place"])
            r["competitor_type"]  = "team"
            r["person_id"]        = ""
            r["team_person_key"]  = tpk
            r["person_canon"]     = "__NON_PERSON__"
            r["team_display_name"]= disp
            r["coverage_flag"]    = "complete" if pid1 and pid2 else "partial"
            r["person_unresolved"]= ""
            r["norm"]             = ""
            rows.append(r)
        else:
            # Single player
            pid, canon, cflag = resolve_person(p1, pt_norms, pbp_norms)
            r = template_row.copy()
            r["event_id"]         = eid
            r["year"]             = year
            r["division_canon"]   = div
            r["division_category"]= cat
            r["place"]            = str(p["place"])
            r["competitor_type"]  = "player"
            r["person_id"]        = pid
            r["team_person_key"]  = ""
            r["person_canon"]     = canon
            r["team_display_name"]= ""
            r["coverage_flag"]    = cflag
            r["person_unresolved"]= "" if pid else "1"
            r["norm"]             = canon.lower()
            rows.append(r)
    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply",  action="store_true")
    parser.add_argument("--event",  nargs="*", default=[])
    args = parser.parse_args()

    restrict = set(args.event) if args.event else set()

    print(f"Loading PBP:    {IN_PBP.name}")
    print(f"Loading stage2: {STAGE2_CSV.name}")
    print(f"Loading PT:     {PT_CSV.name}")

    with open(IN_PBP, newline="", encoding="utf-8") as fh:
        reader   = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        pbp_rows = list(reader)

    pt_norms = load_pt(PT_CSV)

    # Build reverse id→canon from PT
    with open(PT_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            _PT_ID_TO_CANON[row["effective_person_id"]] = row["person_canon"]

    # Build pbp_norms: norm(player_name) -> (person_id, person_canon)
    pbp_norms: dict[str, tuple[str, str]] = {}
    for row in pbp_rows:
        pid, canon = row["person_id"], row["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    stage2 = load_stage2(STAGE2_CSV)

    # ── Find garbled events ────────────────────────────────────────────────
    # Group PBP by (event_id, div, place)
    pbp_by_eid: dict[str, list[dict]] = defaultdict(list)
    for row in pbp_rows:
        pbp_by_eid[row["event_id"]].append(row)

    # Find duplicate (div, place) per event
    fixable: dict[str, set[str]] = {}   # event_id -> set of divisions to replace
    manual:  dict[str, set[str]] = {}   # event_id -> set of divisions to replace via MANUAL_OVERRIDES

    for eid, rows in pbp_by_eid.items():
        if eid in SKIP_EVENTS:
            continue
        if restrict and eid not in restrict:
            continue

        counts = defaultdict(list)
        for r in rows:
            counts[(r["division_canon"], r["place"])].append(r["person_canon"])

        dup_divs = {div for (div, _), persons in counts.items() if len(persons) > 1}
        if not dup_divs:
            continue

        # Check if this event has a manual override
        if eid in MANUAL_OVERRIDES:
            manual[eid] = set(MANUAL_OVERRIDES[eid].keys())
            continue

        # Check stage2 — if stage2 is also duplicated for these divs, skip
        s2_placements = stage2.get(eid, [])
        if has_stage2_duplicates(s2_placements, dup_divs):
            continue  # stage2 also garbled — skip for now

        # Check stage2 actually has data for these divisions
        s2_divs = {p["division_canon"] for p in s2_placements}
        overlap = dup_divs & s2_divs
        if not overlap:
            continue

        fixable[eid] = overlap

    total_fixable = len(fixable) + len(manual)
    print(f"\nEvents with fixable PBP duplicates: {total_fixable}")
    print(f"  Stage2-derived fixes: {len(fixable)}")
    print(f"  Manual overrides:     {len(manual)}")

    # Summary per event
    all_fix_eids = sorted(fixable) + sorted(manual)
    dropped_total = 0
    added_total   = 0

    event_info: dict[str, dict] = {}
    with open(STAGE2_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            event_info[row["event_id"]] = {"name": row["event_name"], "year": row["year"]}

    for eid in all_fix_eids:
        name = event_info.get(eid, {}).get("name", "?")
        year = event_info.get(eid, {}).get("year", "?")
        if eid in fixable:
            divs = sorted(fixable[eid])
        else:
            divs = sorted(manual[eid])
        pbp_count = sum(1 for r in pbp_by_eid[eid] if r["division_canon"] in divs)
        if eid in manual:
            new_count = sum(len(v) for v in MANUAL_OVERRIDES[eid].values())
        else:
            new_count = sum(
                1 for p in stage2.get(eid, []) if p["division_canon"] in divs
            )
        dropped_total += pbp_count
        added_total   += new_count
        print(f"\n  {eid} ({year}) {name}")
        for d in divs:
            print(f"    {d}")
        print(f"    PBP rows: {pbp_count} → {new_count}")

    print(f"\nTotal PBP change: −{dropped_total} dropped, +{added_total} added")
    print(f"Net: {len(pbp_rows)} → {len(pbp_rows) - dropped_total + added_total}")

    if not args.apply:
        print("\nDry run. Use --apply to write output.")
        return

    # ── Apply fixes ────────────────────────────────────────────────────────
    print(f"\nApplying fixes → {OUT_PBP.name}")

    out_rows: list[dict] = []
    template_row = {fn: "" for fn in fieldnames}

    for row in pbp_rows:
        eid = row["event_id"]
        div = row["division_canon"]

        if eid in fixable and div in fixable[eid]:
            continue   # drop; will be replaced below
        if eid in manual and div in manual[eid]:
            continue

        out_rows.append(row)

    # Append stage2-derived replacement rows
    for eid, divs in fixable.items():
        tmpl = {fn: "" for fn in fieldnames}
        tmpl["event_id"] = eid
        tmpl["year"]     = event_info.get(eid, {}).get("year", "")

        new_rows = build_replacement_rows(
            eid, tmpl["year"],
            stage2.get(eid, []), divs,
            tmpl, pt_norms, pbp_norms, fieldnames,
        )
        out_rows.extend(new_rows)

    # Append manual-override replacement rows
    for eid, div_map in MANUAL_OVERRIDES.items():
        if eid not in manual:
            continue
        tmpl = {fn: "" for fn in fieldnames}
        tmpl["event_id"] = eid
        year = event_info.get(eid, {}).get("year", "")
        tmpl["year"] = year
        for div, entries in div_map.items():
            for (place, p1, p2) in entries:
                if p2:
                    pid1, can1, _ = resolve_person(p1, pt_norms, pbp_norms)
                    pid2, can2, _ = resolve_person(p2, pt_norms, pbp_norms)
                    tpk  = "|".join(sorted(filter(None, [pid1, pid2])))
                    disp = f"{can1} / {can2}"
                    r = tmpl.copy()
                    r["division_canon"]    = div
                    r["division_category"] = "freestyle"
                    r["place"]             = place
                    r["competitor_type"]   = "team"
                    r["team_person_key"]   = tpk
                    r["person_canon"]      = "__NON_PERSON__"
                    r["team_display_name"] = disp
                    r["coverage_flag"]     = "complete" if (pid1 and pid2) else "partial"
                    out_rows.append(r)
                else:
                    pid, canon, cflag = resolve_person(p1, pt_norms, pbp_norms)
                    r = tmpl.copy()
                    r["division_canon"]    = div
                    r["division_category"] = "freestyle"
                    r["place"]             = place
                    r["competitor_type"]   = "player"
                    r["person_id"]         = pid
                    r["person_canon"]      = canon
                    r["coverage_flag"]     = cflag
                    r["person_unresolved"] = "" if pid else "1"
                    r["norm"]              = canon.lower()
                    out_rows.append(r)

    with open(OUT_PBP, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Written: {OUT_PBP.name}  ({len(out_rows):,} rows)")


if __name__ == "__main__":
    main()
