"""
tools/57_patch_placements_v50_to_v51.py
Patch Placements_ByPerson v50 → v51.

Full-rebuild from stage2 for 10 events where PBP has garbled division names
but stage2 has correct, clean data under proper division names.

Root cause: PBP was built from an older stage2 run that used bare/garbled
division labels (e.g. "Shred 30", "Sick Trick" instead of "Open Shred 30",
"Intermediate Shred 30"). Current stage2 correctly separates divisions.

Strategy: drop ALL PBP rows for each event and rebuild entirely from stage2.
Legitimate ties (same place, same division, different people) are preserved.

Events fixed:
  884112176  (1998) Fighting Illini: "Open Doubles - 1St..." → proper division names
  909186885  (1999) Southern Regional: bare "Open/Intermediate/Novice" → labeled
  1044624680 (2003) Southeastern Regional: merged Open/Int/Novice → separated
  1070954568 (2004) Colorado Shred 5: bare "Shred 30/Sick Trick/Intermediate" → labeled
  1096695238 (2004) UMaine: "2-Minute Freestyle Shred/Sick 3" → proper names
  1111735438 (2005) Rochester: bare "Routines/Shred 30" → Open/Intermediate labeled
  1133888413 (2006) Valentines Day Massacre: "Freestyle Sick 3 - 6 Competing" → clean
  1208408135 (2008) ShrEdmonton 2008: "Inter Sick3/Open Sick1" → proper names
  1286381206 (2010) Copa X-PRO: "Unknown" noise → proper division names
  1645621833 (2022) Basque Tournament: "Unknown" noise → proper division names
"""

import csv, json, pathlib, re, unicodedata

ROOT       = pathlib.Path(__file__).parent.parent
IN_FILE    = ROOT / "inputs/identity_lock/Placements_ByPerson_v50.csv"
OUT_FILE   = ROOT / "inputs/identity_lock/Placements_ByPerson_v51.csv"
STAGE2_CSV = ROOT / "out" / "stage2_canonical_events.csv"
PT_CSV     = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v36.csv"

csv.field_size_limit(10**7)

TARGET_EIDS = {
    "884112176", "909186885", "1044624680", "1070954568", "1096695238",
    "1111735438", "1133888413", "1208408135", "1286381206", "1645621833",
}

# Skip stage2 noise divisions when rebuilding
_SKIP_DIVS = {"Unknown"}

_PT_ID_TO_CANON: dict[str, str] = {}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _clean_name(s: str) -> str:
    """Strip tie markers (= ) and location suffixes (City) from stage2 names."""
    s = s.strip()
    s = re.sub(r"^=\s*", "", s)
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)
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


def build_rows_from_s2(eid, year, s2_placements, template, fieldnames,
                       pt_norms, pbp_norms):
    rows = []
    for p in s2_placements:
        div = p["division_canon"]
        if div in _SKIP_DIVS:
            continue
        cat  = p.get("division_category", "freestyle")
        comp = p.get("competitor_type", "player")
        p1   = _clean_name(p.get("player1_name", ""))
        p2   = _clean_name(p.get("player2_name", ""))

        r = template.copy()
        r["event_id"]          = eid
        r["year"]              = year
        r["division_canon"]    = div
        r["division_category"] = cat
        r["place"]             = str(p["place"])

        if comp == "team" and (p1 or p2):
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
            pid, canon, cflag = resolve_person(p1, pt_norms, pbp_norms)
            r["competitor_type"]   = "player"
            r["person_id"]         = pid
            r["team_person_key"]   = ""
            r["person_canon"]      = canon
            r["team_display_name"] = ""
            r["coverage_flag"]     = cflag
            r["person_unresolved"] = "" if pid else "1"
            r["norm"]              = canon.lower()

        rows.append(r)
    return rows


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows):,} rows from {IN_FILE.name}")

    pt_norms = load_pt()

    pbp_norms: dict[str, tuple] = {}
    for row in rows:
        pid, canon = row["person_id"], row["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    # Load stage2 for target events
    s2_data: dict[str, dict] = {}
    with open(STAGE2_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] in TARGET_EIDS:
                s2_data[row["event_id"]] = {
                    "placements": json.loads(row.get("placements_json", "[]")),
                    "year": row["year"],
                    "name": row["event_name"],
                }

    # Template per event
    templates: dict[str, dict] = {fn: "" for fn in fieldnames}
    for row in rows:
        if row["event_id"] in TARGET_EIDS and row["event_id"] not in templates:
            templates[row["event_id"]] = row.copy()
    for eid in TARGET_EIDS:
        if eid not in templates or not isinstance(templates.get(eid), dict):
            templates[eid] = {fn: "" for fn in fieldnames}
            templates[eid]["event_id"] = eid

    # Drop all target event rows, keep the rest
    out_rows = [r for r in rows if r["event_id"] not in TARGET_EIDS]
    dropped_total = len(rows) - len(out_rows)
    print(f"\nDropped {dropped_total} rows across {len(TARGET_EIDS)} events")

    # Rebuild each event from stage2
    added_total = 0
    for eid in sorted(TARGET_EIDS):
        s2 = s2_data.get(eid, {})
        s2p = s2.get("placements", [])
        year = s2.get("year", "")
        tmpl = templates.get(eid, {fn: "" for fn in fieldnames})

        new_rows = build_rows_from_s2(eid, year, s2p, tmpl, fieldnames,
                                      pt_norms, pbp_norms)
        out_rows.extend(new_rows)
        added_total += len(new_rows)

        unresolved = sum(1 for r in new_rows if r.get("person_unresolved") == "1")
        print(f"  {eid} ({year}) {s2.get('name','')[:45]}")
        print(f"    → {len(new_rows)} rows added ({unresolved} unresolved)")

    print(f"\nTotal added: {added_total}")
    print(f"Net change: {len(rows):,} → {len(out_rows):,}")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"\nWritten: {OUT_FILE.name}")


if __name__ == "__main__":
    main()
