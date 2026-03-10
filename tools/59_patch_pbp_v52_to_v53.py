"""
tools/59_patch_pbp_v52_to_v53.py
Patch PBP v52 → v53.

Full rebuild from stage2 for event 980969461
(2001 WORLD FOOTBAG CHAMPIONSHIPS).

Root cause: mirror had pool+finals results merged (7 divisions, 18 dup pairs,
147 affected rows). Fixed in stage2 via RESULTS_FILE_OVERRIDE pointing to
legacy_data/event_results/980969461.txt (complete final standings from the
mirror's "Manually Entered Results" section).
"""

import csv, json, pathlib, re, unicodedata

ROOT      = pathlib.Path(__file__).parent.parent
PBP_IN    = ROOT / "inputs/identity_lock/Placements_ByPerson_v52.csv"
PBP_OUT   = ROOT / "inputs/identity_lock/Placements_ByPerson_v53.csv"
STAGE2    = ROOT / "out/stage2_canonical_events.csv"
PT_CSV    = ROOT / "inputs/identity_lock/Persons_Truth_Final_v37.csv"

EID = "980969461"
csv.field_size_limit(10**7)

_PT_ID_TO_CANON: dict[str, str] = {}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _clean_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^=\s*", "", s)
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)
    return s.strip()


def load_pt():
    pt_norms = {}
    pt_alias_to_pid = {}
    with open(PT_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid, canon = row["effective_person_id"], row["person_canon"]
            if not pid or not canon:
                continue
            _PT_ID_TO_CANON[pid] = canon
            pt_norms[_norm(canon)] = pid
            for alias in row["player_ids_seen"].split("|"):
                alias = alias.strip()
                if alias:
                    pt_alias_to_pid[alias] = pid
    return pt_norms, pt_alias_to_pid


def resolve(name, player_id, pt_norms, pt_alias_to_pid, pbp_norms):
    if not name or name.startswith("__"):
        return "", "__NON_PERSON__", "complete"
    if player_id and player_id in pt_alias_to_pid:
        pid = pt_alias_to_pid[player_id]
        return pid, _PT_ID_TO_CANON.get(pid, name), "complete"
    n = _norm(name)
    if n in pt_norms:
        pid = pt_norms[n]
        return pid, _PT_ID_TO_CANON.get(pid, name), "complete"
    if n in pbp_norms:
        pid, canon = pbp_norms[n]
        return pid, canon, "complete"
    return "", name, "partial"


def main():
    with open(PBP_IN, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        all_rows = list(reader)

    print(f"Loaded {len(all_rows):,} rows from {PBP_IN.name}")

    keep = [r for r in all_rows if r["event_id"] != EID]
    dropped = len(all_rows) - len(keep)
    print(f"Dropped {dropped} rows for event {EID}")

    pt_norms, pt_alias_to_pid = load_pt()

    pbp_norms = {}
    for r in keep:
        pid, canon = r["person_id"], r["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    template = next((r.copy() for r in all_rows if r["event_id"] == EID),
                    {fn: "" for fn in fieldnames})

    s2_placements, year = [], ""
    with open(STAGE2, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] == EID:
                s2_placements = json.loads(row.get("placements_json", "[]"))
                year = row["year"]
                break

    new_rows = []
    unresolved = 0
    for p in s2_placements:
        div  = p["division_canon"]
        cat  = p.get("division_category", "net")
        comp = p.get("competitor_type", "player")
        p1   = _clean_name(p.get("player1_name", ""))
        p2   = _clean_name(p.get("player2_name", ""))
        id1  = p.get("player1_id", "")
        id2  = p.get("player2_id", "")

        r = template.copy()
        r["event_id"]          = EID
        r["year"]              = year
        r["division_canon"]    = div
        r["division_category"] = cat
        r["place"]             = str(p["place"])

        if comp == "team" and (p1 or p2):
            pid1, can1, _ = resolve(p1, id1, pt_norms, pt_alias_to_pid, pbp_norms)
            pid2, can2, _ = resolve(p2, id2, pt_norms, pt_alias_to_pid, pbp_norms)
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
            pid, canon, cflag = resolve(p1, id1, pt_norms, pt_alias_to_pid, pbp_norms)
            r["competitor_type"]   = "player"
            r["person_id"]         = pid
            r["team_person_key"]   = ""
            r["person_canon"]      = canon
            r["team_display_name"] = ""
            r["coverage_flag"]     = cflag
            r["person_unresolved"] = "" if pid else "1"
            r["norm"]              = canon.lower()
            if not pid:
                unresolved += 1
                print(f"  UNRESOLVED: {div} p{p['place']}: {p1}")

        new_rows.append(r)

    print(f"\nAdded {len(new_rows)} rows ({unresolved} unresolved)")

    out_rows = keep + new_rows
    print(f"Net: {len(all_rows):,} → {len(out_rows):,}")

    with open(PBP_OUT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Written: {PBP_OUT.name}")


if __name__ == "__main__":
    main()
