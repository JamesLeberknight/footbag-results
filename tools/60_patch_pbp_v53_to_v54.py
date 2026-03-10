"""
tools/60_patch_pbp_v53_to_v54.py
Patch PBP v53 → v54. Full rebuild from stage2 for 2 garbled-label events.

1036298726 (Colorado Shred Symposium 4, 2003):
  PBP had 3 garbled divisions: "Big 3 Round 2", "Intermediate Shred :30",
  "Women's Shred :30" (16+5+25=46 rows, 12 dup pairs).
  Stage2 has 6 correct divisions (41 rows total).

1262288759 (2010 US Open Net):
  PBP had trailing colons on all 7 division names, causing 2 dup pairs.
  Stage2 has clean division names (50 rows total).
"""

import csv, json, pathlib, re, unicodedata

ROOT   = pathlib.Path(__file__).parent.parent
PBP_IN = ROOT / "inputs/identity_lock/Placements_ByPerson_v53.csv"
PBP_OUT= ROOT / "inputs/identity_lock/Placements_ByPerson_v54.csv"
STAGE2 = ROOT / "out/stage2_canonical_events.csv"
PT_CSV = ROOT / "inputs/identity_lock/Persons_Truth_Final_v37.csv"

TARGET_EIDS = {"1036298726", "1262288759"}
csv.field_size_limit(10**7)
_PT_ID_TO_CANON: dict[str, str] = {}


def _norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())

def _clean(s):
    s = re.sub(r"^=\s*", "", s.strip())
    return re.sub(r"\s*\(.*?\)\s*$", "", s).strip()

def load_pt():
    pt_norms, pt_alias = {}, {}
    with open(PT_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid, canon = row["effective_person_id"], row["person_canon"]
            if not pid or not canon: continue
            _PT_ID_TO_CANON[pid] = canon
            pt_norms[_norm(canon)] = pid
            for a in row["player_ids_seen"].split("|"):
                a = a.strip()
                if a: pt_alias[a] = pid
    return pt_norms, pt_alias

def resolve(name, pid_hint, pt_norms, pt_alias, pbp_norms):
    if not name or name.startswith("__"):
        return "", "__NON_PERSON__", "complete"
    if pid_hint and pid_hint in pt_alias:
        pid = pt_alias[pid_hint]
        return pid, _PT_ID_TO_CANON.get(pid, name), "complete"
    n = _norm(name)
    if n in pt_norms:
        pid = pt_norms[n]
        return pid, _PT_ID_TO_CANON.get(pid, name), "complete"
    if n in pbp_norms:
        pid, canon = pbp_norms[n]
        return pid, canon, "complete"
    return "", name, "partial"

def build_rows(eid, year, s2p, template, fieldnames, pt_norms, pt_alias, pbp_norms):
    rows = []
    for p in s2p:
        div  = p["division_canon"]
        cat  = p.get("division_category", "freestyle")
        comp = p.get("competitor_type", "player")
        p1   = _clean(p.get("player1_name", ""))
        p2   = _clean(p.get("player2_name", ""))
        id1  = p.get("player1_id", "")
        id2  = p.get("player2_id", "")

        r = template.copy()
        r["event_id"] = eid; r["year"] = year
        r["division_canon"] = div; r["division_category"] = cat
        r["place"] = str(p["place"])

        if comp == "team" and (p1 or p2):
            pid1, c1, _ = resolve(p1, id1, pt_norms, pt_alias, pbp_norms)
            pid2, c2, _ = resolve(p2, id2, pt_norms, pt_alias, pbp_norms)
            tpk = "|".join(sorted(filter(None, [pid1, pid2])))
            disp = f"{c1} / {c2}" if c1 and c2 else f"{p1} / {p2}"
            r["competitor_type"]="team"; r["person_id"]=""
            r["team_person_key"]=tpk; r["person_canon"]="__NON_PERSON__"
            r["team_display_name"]=disp
            r["coverage_flag"]="complete" if (pid1 and pid2) else "partial"
            r["person_unresolved"]=""; r["norm"]=""
        else:
            pid, canon, cflag = resolve(p1, id1, pt_norms, pt_alias, pbp_norms)
            r["competitor_type"]="player"; r["person_id"]=pid
            r["team_person_key"]=""; r["person_canon"]=canon
            r["team_display_name"]=""
            r["coverage_flag"]=cflag
            r["person_unresolved"]="" if pid else "1"
            r["norm"]=canon.lower()
        rows.append(r)
    return rows

def main():
    with open(PBP_IN, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        all_rows = list(reader)
    print(f"Loaded {len(all_rows):,} rows")

    pt_norms, pt_alias = load_pt()
    keep = [r for r in all_rows if r["event_id"] not in TARGET_EIDS]
    dropped = len(all_rows) - len(keep)
    print(f"Dropped {dropped} rows across {len(TARGET_EIDS)} events")

    pbp_norms = {}
    for r in keep:
        pid, canon = r["person_id"], r["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    # templates
    templates = {}
    for r in all_rows:
        if r["event_id"] in TARGET_EIDS and r["event_id"] not in templates:
            templates[r["event_id"]] = r.copy()

    s2_data = {}
    with open(STAGE2, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] in TARGET_EIDS:
                s2_data[row["event_id"]] = {
                    "placements": json.loads(row.get("placements_json", "[]")),
                    "year": row["year"], "name": row["event_name"],
                }

    added_total = 0
    for eid in sorted(TARGET_EIDS):
        s2 = s2_data.get(eid, {})
        tmpl = templates.get(eid, {fn: "" for fn in fieldnames})
        new_rows = build_rows(eid, s2["year"], s2["placements"], tmpl,
                              fieldnames, pt_norms, pt_alias, pbp_norms)
        keep.extend(new_rows)
        added_total += len(new_rows)
        unres = sum(1 for r in new_rows if r.get("person_unresolved") == "1")
        print(f"  {eid} ({s2['year']}) {s2['name'][:40]}")
        print(f"    → {len(new_rows)} rows ({unres} unresolved)")

    print(f"\nNet: {len(all_rows):,} → {len(keep):,}")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(keep)
    print(f"Written: {PBP_OUT.name}")

if __name__ == "__main__":
    main()
