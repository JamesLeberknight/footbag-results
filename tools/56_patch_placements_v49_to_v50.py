"""
tools/56_patch_placements_v49_to_v50.py
Patch Placements_ByPerson v49 → v50.

Event 947196813 — New Zealand Footbag Championships 2000.

PBP had two garbled divisions:
  - "Open Mens Singles Open Womens Singles" (17 rows, concatenated from
    Net Open Mens Singles + Freestyle Under 13 Boys + Freestyle Open Mens Singles)
  - "Footbag Consecutive" (4 rows, collapsed from 5 separate consecutive divisions)

Stage2 correctly parses all divisions with proper names. Replace by
dropping all PBP rows for this event and rebuilding entirely from stage2.
"""

import argparse, csv, json, pathlib, re, unicodedata

ROOT       = pathlib.Path(__file__).parent.parent
IN_FILE    = ROOT / "inputs/identity_lock/Placements_ByPerson_v49.csv"
OUT_FILE   = ROOT / "inputs/identity_lock/Placements_ByPerson_v50.csv"
STAGE2_CSV = ROOT / "out" / "stage2_canonical_events.csv"
PT_CSV     = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v36.csv"

csv.field_size_limit(10**7)

EID = "947196813"

# The noise row in stage2 we don't want
_SKIP_DIVS = {"Unknown"}

_PT_ID_TO_CANON: dict[str, str] = {}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _clean_name(s: str) -> str:
    """Strip tie markers (= ) and location suffixes (City) from stage2 names."""
    s = s.strip()
    s = re.sub(r"^=\s*", "", s)          # leading "= " tie marker
    s = re.sub(r"\s*\(.*\)\s*$", "", s)  # trailing "(location)"
    return s.strip()


def load_pt():
    result = {}
    with open(PT_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid, canon = row["effective_person_id"], row["person_canon"]
            if pid and canon:
                result[_norm(canon)] = pid
                _PT_ID_TO_CANON[pid] = canon
    return result


def resolve_person(name, pt_norms, pbp_norms):
    if not name or name.startswith("__"):
        return ("", "__NON_PERSON__", "complete")
    n = _norm(name)
    if n in pt_norms:
        pid = pt_norms[n]
        return (pid, _PT_ID_TO_CANON.get(pid, name), "complete")
    if n in pbp_norms:
        pid, canon = pbp_norms[n]
        return (pid, canon, "complete")
    return ("", name, "partial")


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    pt_norms = load_pt()

    pbp_norms = {}
    for row in rows:
        pid, canon = row["person_id"], row["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    # Load stage2 for this event
    s2_placements = []
    year = ""
    with open(STAGE2_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] == EID:
                s2_placements = json.loads(row.get("placements_json", "[]"))
                year = row["year"]
                break

    # Template from any existing event row
    template = {fn: "" for fn in fieldnames}
    for row in rows:
        if row["event_id"] == EID:
            template = row.copy()
            break

    # Drop all existing rows for this event
    out_rows = [r for r in rows if r["event_id"] != EID]
    dropped = len(rows) - len(out_rows)
    print(f"Dropped {dropped} existing rows for event {EID}")

    # Rebuild from stage2
    added = []
    for p in s2_placements:
        div = p["division_canon"]
        if div in _SKIP_DIVS:
            continue
        cat   = p.get("division_category", "freestyle")
        comp  = p.get("competitor_type", "player")
        p1    = p.get("player1_name", "")
        p2    = p.get("player2_name", "")

        r = template.copy()
        r["event_id"]          = EID
        r["year"]              = year
        r["division_canon"]    = div
        r["division_category"] = cat
        r["place"]             = str(p["place"])

        if comp == "team" and (p1 or p2):
            p1 = _clean_name(p1); p2 = _clean_name(p2)
            pid1, can1, _ = resolve_person(p1, pt_norms, pbp_norms)
            pid2, can2, _ = resolve_person(p2, pt_norms, pbp_norms)
            tpk  = "|".join(sorted(filter(None, [pid1, pid2])))
            disp = f"{can1} / {can2}" if can1 and can2 else f"{p1} / {p2}"
            r["competitor_type"]   = "team"
            r["person_id"]         = ""
            r["team_person_key"]   = tpk
            r["person_canon"]      = "__NON_PERSON__"
            r["team_display_name"] = disp
            r["coverage_flag"]     = "complete" if (pid1 and pid2) else "partial"
            r["person_unresolved"] = ""
            r["norm"]              = ""
        else:
            p1 = _clean_name(p1)
            pid, canon, cflag = resolve_person(p1, pt_norms, pbp_norms)
            r["competitor_type"]   = "player"
            r["person_id"]         = pid
            r["team_person_key"]   = ""
            r["person_canon"]      = canon
            r["team_display_name"] = ""
            r["coverage_flag"]     = cflag
            r["person_unresolved"] = "" if pid else "1"
            r["norm"]              = canon.lower()

        out_rows.append(r)
        added.append(f"  ADD {div} p{p['place']} {p1}{' / '+p2 if p2 else ''}")

    print(f"Added {len(added)} rows from stage2")
    print(f"Net change: {len(rows):,} → {len(out_rows):,}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Written: {OUT_FILE.name}")

    # Show unresolved
    print("\nPartial/unresolved names:")
    for r in out_rows:
        if r["event_id"] == EID and r["person_unresolved"] == "1":
            print(f"  p{r['place']} {r['division_canon']}: {r['person_canon']}")


if __name__ == "__main__":
    main()
