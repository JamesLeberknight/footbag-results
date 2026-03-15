#!/usr/bin/env python3
"""
Patch PBP v65 → v66: fill in 19 recovered doubles-partner names.

Source: mirror HTML for 12 events contained the full partner information
in "manually entered results" sections that the parser failed to extract.
The "()" placeholder person_canon is replaced with the actual name.

Partners with existing person_ids (resolved in PT/PBP):
  Jonathan Poirier       a07a9941-2dae-5a30-8abe-888107dfda97
  Anderson Carrero       5409d10e-6c5f-567c-a9a0-f61f5876885c
  Grant Hayes            0a497c97-bf82-5bd1-a319-298d00821134  (appears twice)
  Nick Rettinger         0533d6e2-42c6-56c6-9771-5606e02a09d1
  Katie Storment         65110afa-b4a8-5b55-9b73-b2ef3b2716c9  (appears twice)
  Jason Hicks            7f773521-a6ed-5973-bcfb-8adc379fcd5c
  Anneleissa Cohen       008f279b-0172-5793-868e-70f7b4c45ac7  (source: "Coen")
  Jean-Marie Letort      43ac12c4-b074-55ec-be51-6858775ac7c7  (source: "JM Letort")

New unresolved partners (person_unresolved=1, person_id=''):
  Daniel Dleon, Nick Faust, Jesus Manuel Rivero, Sain Palacios,
  Gustavo Arteaga, Jessica Milangela, Tanya Jeliazkova,
  Ivaylo Yordanov, Iguaraya Delgado

For 2002 Worlds (1013283071): 3 remaining "()" entries are unrecoverable —
the source page itself shows "()" with no partner data.

Net: 19 "()" person_canon values replaced; 19 team_display_name cells updated
on "()" rows; 19 team_display_name cells updated on the matching known-partner
rows. Row count unchanged (38510).
"""

import csv
import sys
import unicodedata
from pathlib import Path

IN_FILE  = Path("inputs/identity_lock/Placements_ByPerson_v65.csv")
OUT_FILE = Path("inputs/identity_lock/Placements_ByPerson_v66.csv")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()
    return " ".join(s.lower().split())


# Each entry: (event_id, division_canon, place, known_fragment,
#              recovered_canon, recovered_pid)
# known_fragment: substring of the known partner's person_canon (for matching)
# recovered_pid:  empty string if new/unresolved
RECOVERIES = [
    ("1301837824", "Open Doubles Net",         "9",  "Marc Girard",
     "Jonathan Poirier",   "a07a9941-2dae-5a30-8abe-888107dfda97"),

    ("1314328055", "Open Doubles Net",         "6",  "Ronald Varillas",
     "Anderson Carrero",   "5409d10e-6c5f-567c-a9a0-f61f5876885c"),

    ("1348929718", "Open Doubles Net",         "10", "Alberto Perez",
     "Daniel Dleon",       ""),

    ("1357101984", "Intermediate Doubles Net", "2",  "Steve Femmel",
     "Grant Hayes",        "0a497c97-bf82-5bd1-a319-298d00821134"),

    ("1357101984", "Intermediate Doubles Net", "4",  "Paul Vorvick",
     "Nick Rettinger",     "0533d6e2-42c6-56c6-9771-5606e02a09d1"),

    ("1357101984", "Intermediate Doubles Net", "5",  "Jesse Ross",
     "Nick Faust",         ""),

    ("1357101984", "Open Mixed Doubles Net",   "9",  "Jasper Shults",
     "Katie Storment",     "65110afa-b4a8-5b55-9b73-b2ef3b2716c9"),

    ("1357101984", "Open Mixed Doubles Net",   "11", "Gaylene Grossen",
     "Grant Hayes",        "0a497c97-bf82-5bd1-a319-298d00821134"),

    ("1361239371", "Open Doubles Net",         "7",  "Diego Gamboa",
     "Jesus Manuel Rivero", ""),

    ("1368559562", "Open Doubles Net",         "15", "Carlos Medina",
     "Sain Palacios",      ""),

    ("1368559562", "Open Doubles Net",         "16", "Eric Castro",
     "Gustavo Arteaga",    ""),

    ("1368559562", "Open Doubles Net",         "17", "Wilmer Rojas",
     "Jessica Milangela",  ""),

    ("1378821205", "Open Doubles Net",         "9",  "Miquel Clemente",
     "Jean-Marie Letort",  "43ac12c4-b074-55ec-be51-6858775ac7c7"),

    ("1420195881", "Open Mixed Doubles Net",   "6",  "Ivan Stanev",
     "Tanya Jeliazkova",   ""),

    ("1421069713", "Open Doubles Net",         "4",  "Dinko Dimitrov",
     "Ivaylo Yordanov",    ""),

    ("1423020874", "Open Doubles Net",         "16", "Francisco Moreno",
     "Iguaraya Delgado",   ""),

    # 1425372477 — Leanne Makcrow in two separate divisions
    ("1425372477", "Open Mixed Doubles Net",   "4",  "Leanne Makcrow",
     "Jason Hicks",        "7f773521-a6ed-5973-bcfb-8adc379fcd5c"),

    ("1425372477", "Women's Doubles Net",      "2",  "Leanne Makcrow",
     "Anneleissa Cohen",   "008f279b-0172-5793-868e-70f7b4c45ac7"),

    ("1425372477", "Open Mixed Doubles Net",   "6",  "Jasper Shults",
     "Katie Storment",     "65110afa-b4a8-5b55-9b73-b2ef3b2716c9"),
]


def main() -> None:
    if not IN_FILE.exists():
        sys.exit(f"ERROR: {IN_FILE} not found")

    rows = list(csv.DictReader(IN_FILE.open(encoding="utf-8")))
    fieldnames = list(rows[0].keys())

    # ── Step 1: resolve each recovery to a team_person_key ───────────────────
    # Key: (event_id, division_canon, place) → tpk
    tpk_map: dict[tuple, str] = {}

    for eid, div, place, frag, rec_canon, rec_pid in RECOVERIES:
        key = (eid, div, place)
        if key in tpk_map:
            continue  # already resolved (shouldn't happen with our data)
        for r in rows:
            if (r["event_id"] == eid
                    and r["division_canon"] == div
                    and r["place"] == place
                    and frag.lower() in r["person_canon"].lower()
                    and r["person_canon"] != "()"
                    and r.get("team_person_key")):
                tpk_map[key] = r["team_person_key"]
                break
        else:
            print(f"  WARNING: no match for {eid} {div!r} p{place} frag={frag!r}")

    # ── Step 2: build update instructions keyed by (event_id, tpk, is_empty) ─
    # For the "()" row: update person_canon, person_id, norm
    # For BOTH rows: update team_display_name

    # Build team_display_name: strip "()" from existing known-partner display
    # e.g. "Leanne Makcrow (Canada) / ()" → "Leanne Makcrow (Canada) / Jason Hicks"
    new_display: dict[tuple, str] = {}   # (event_id, tpk) → new team_display_name
    new_person: dict[tuple, tuple] = {}  # (event_id, tpk) → (canon, pid)

    for eid, div, place, frag, rec_canon, rec_pid in RECOVERIES:
        key = (eid, div, place)
        tpk = tpk_map.get(key)
        if not tpk:
            continue
        # Find the known-partner row to get its current team_display_name
        for r in rows:
            if (r["event_id"] == eid
                    and r["team_person_key"] == tpk
                    and r["person_canon"] != "()"):
                old_td = r["team_display_name"]
                # Replace trailing "()" with recovered name
                new_td = old_td.replace("/ ()", f"/ {rec_canon}").replace("() /", f"{rec_canon} /")
                new_display[(eid, tpk)] = new_td
                new_person[(eid, tpk)] = (rec_canon, rec_pid)
                break

    # ── Step 3: apply updates ─────────────────────────────────────────────────
    out_rows: list[dict] = []
    stats = {"empty_replaced": 0, "display_updated": 0}

    for r in rows:
        eid = r["event_id"]
        tpk = r.get("team_person_key", "")
        dk  = (eid, tpk)

        if dk in new_display:
            r = dict(r)
            old_td = r["team_display_name"]
            r["team_display_name"] = new_display[dk]
            if old_td != r["team_display_name"]:
                stats["display_updated"] += 1

            if r["person_canon"] == "()":
                rec_canon, rec_pid = new_person[dk]
                r["person_canon"]     = rec_canon
                r["person_id"]        = rec_pid
                r["norm"]             = _norm(rec_canon)
                # coverage_flag and person_unresolved: keep unresolved=1 for new
                # names; for resolved (has pid), mark as resolved
                if rec_pid:
                    r["person_unresolved"] = ""
                    r["coverage_flag"]     = "complete"
                stats["empty_replaced"] += 1

        out_rows.append(r)

    with OUT_FILE.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Rows in:  {len(rows)}")
    print(f"Rows out: {len(out_rows)}")
    print(f"  () rows replaced:       {stats['empty_replaced']}")
    print(f"  team_display_name fixed: {stats['display_updated']}")
    print(f"Written: {OUT_FILE}")


if __name__ == "__main__":
    main()
