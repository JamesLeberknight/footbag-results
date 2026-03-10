"""
tools/58_patch_v51_to_v52_859787898.py
Patch PT v36 → v37 and PBP v51 → v52 for event 859787898
(HEART OF FOOTBAG Freestyle Tournament, Portland OR, Feb 1997).

Three fixes:
  1. PT: move alias ff3160f6 from Tuan Vu (28565dd0) → Tu Vu (c50fb80d).
     Evidence: Open Doubles p2 lists "Tu Vu / Tuan Vu" as a doubles pair —
     they cannot be the same person.
  2. PBP: full rebuild of event 859787898 from stage2.
     After PT fix, ff3160f6 resolves correctly to Tu Vu.
  3. PBP: rename division "Shred Skills Contest Final Results" (p1 only)
     → "Shred Skills Contest" so p1-8 appear as one division.
"""

import csv, json, pathlib, re, unicodedata

ROOT      = pathlib.Path(__file__).parent.parent
PT_IN     = ROOT / "inputs/identity_lock/Persons_Truth_Final_v36.csv"
PT_OUT    = ROOT / "inputs/identity_lock/Persons_Truth_Final_v37.csv"
PBP_IN    = ROOT / "inputs/identity_lock/Placements_ByPerson_v51.csv"
PBP_OUT   = ROOT / "inputs/identity_lock/Placements_ByPerson_v52.csv"
STAGE2    = ROOT / "out/stage2_canonical_events.csv"

EID             = "859787898"
ALIAS_TO_MOVE   = "ff3160f6-d4cc-5f15-8d85-16e1b3cc299d"
TUAN_VU_PID     = "28565dd0-2196-5404-bf23-6cf0617ce79b"
TU_VU_PID       = "c50fb80d-aa35-5154-be01-19817c3b84d2"

csv.field_size_limit(10**7)


# ── helpers ──────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _clean_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^=\s*", "", s)
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)
    return s.strip()


# ── Step 1: patch PT ─────────────────────────────────────────────────────────

def patch_pt():
    with open(PT_IN, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    changed = 0
    for row in rows:
        pid_col = row["player_ids_seen"]
        ids = [x.strip() for x in pid_col.split("|") if x.strip()]

        if row["effective_person_id"] == TUAN_VU_PID and ALIAS_TO_MOVE in ids:
            ids.remove(ALIAS_TO_MOVE)
            row["player_ids_seen"] = " | ".join(ids)
            print(f"  Removed {ALIAS_TO_MOVE} from Tuan Vu")
            changed += 1

        if row["effective_person_id"] == TU_VU_PID and ALIAS_TO_MOVE not in ids:
            ids.append(ALIAS_TO_MOVE)
            row["player_ids_seen"] = " | ".join(ids)
            print(f"  Added   {ALIAS_TO_MOVE} to Tu Vu")
            changed += 1

    assert changed == 2, f"Expected 2 changes, got {changed}"

    with open(PT_OUT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Written: {PT_OUT.name}  ({len(rows)} rows)")


# ── Step 2: rebuild PBP for event 859787898 ──────────────────────────────────

def _build_lookups(pt_path, pbp_rows):
    """Build PT and PBP norm → (pid, canon) lookups using the NEW PT."""
    pt_id_to_canon = {}
    pt_norms = {}
    with open(pt_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pid, canon = row["effective_person_id"], row["person_canon"]
            if pid and canon:
                pt_id_to_canon[pid] = canon
                # index by every player_id alias
                for alias in row["player_ids_seen"].split("|"):
                    alias = alias.strip()
                    if alias:
                        pt_norms[alias] = pid
                # also index by canon norm
                pt_norms[_norm(canon)] = pid

    pbp_norms = {}
    for row in pbp_rows:
        pid, canon = row["person_id"], row["person_canon"]
        if pid and canon and canon not in ("__NON_PERSON__", ""):
            pbp_norms[_norm(canon)] = (pid, canon)

    return pt_norms, pt_id_to_canon, pbp_norms


def resolve(name, player_id, pt_norms, pt_id_to_canon, pbp_norms):
    if not name or name.startswith("__"):
        return "", "__NON_PERSON__", "complete"
    # resolve via stage2 player_id alias first (most reliable)
    if player_id and player_id in pt_norms:
        pid = pt_norms[player_id]
        return pid, pt_id_to_canon.get(pid, name), "complete"
    # fallback: canon norm
    n = _norm(name)
    if n in pt_norms:
        pid = pt_norms[n]
        return pid, pt_id_to_canon.get(pid, name), "complete"
    if n in pbp_norms:
        pid, canon = pbp_norms[n]
        return pid, canon, "complete"
    return "", name, "partial"


_DIV_RENAME = {"Shred Skills Contest Final Results": "Shred Skills Contest"}


def rebuild_event(s2_placements, year, template, fieldnames,
                  pt_norms, pt_id_to_canon, pbp_norms):
    rows = []
    for p in s2_placements:
        div = _DIV_RENAME.get(p["division_canon"], p["division_canon"])
        cat  = p.get("division_category", "freestyle")
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
            pid1, can1, _ = resolve(p1, id1, pt_norms, pt_id_to_canon, pbp_norms)
            pid2, can2, _ = resolve(p2, id2, pt_norms, pt_id_to_canon, pbp_norms)
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
            pid, canon, cflag = resolve(p1, id1, pt_norms, pt_id_to_canon, pbp_norms)
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


def patch_pbp():
    with open(PBP_IN, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        all_rows = list(reader)

    print(f"  Loaded {len(all_rows):,} PBP rows")

    keep = [r for r in all_rows if r["event_id"] != EID]
    dropped = len(all_rows) - len(keep)
    print(f"  Dropped {dropped} rows for event {EID}")

    # template from existing row (or blank)
    template = next((r.copy() for r in all_rows if r["event_id"] == EID),
                    {fn: "" for fn in fieldnames})

    pt_norms, pt_id_to_canon, pbp_norms = _build_lookups(PT_OUT, keep)

    # load stage2 placements for this event
    s2_placements, year = [], ""
    with open(STAGE2, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["event_id"] == EID:
                s2_placements = json.loads(row.get("placements_json", "[]"))
                year = row["year"]
                break

    new_rows = rebuild_event(s2_placements, year, template, fieldnames,
                             pt_norms, pt_id_to_canon, pbp_norms)
    print(f"  Rebuilt {len(new_rows)} rows from stage2")

    # show the Shred Skills and Vu rows for verification
    print("\n  Shred Skills / Vu rows:")
    for r in new_rows:
        if "Shred" in r["division_canon"] or "Vu" in r["person_canon"]:
            print(f"    {r['division_canon']} p{r['place']}: {r['person_canon']} ({r['person_id'][:8] if r['person_id'] else 'unresolved'})")

    out_rows = keep + new_rows
    print(f"\n  Net: {len(all_rows):,} → {len(out_rows):,}")

    with open(PBP_OUT, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"  Written: {PBP_OUT.name}")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n── Step 1: Patch PT v36 → v37 ──")
    patch_pt()

    print("\n── Step 2: Patch PBP v51 → v52 ──")
    patch_pbp()

    print("\nDone.")


if __name__ == "__main__":
    main()
