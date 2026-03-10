#!/usr/bin/env python3
"""
tools/49_patch_placements_v42_to_v43.py — PBP v42 → v43

Adds 6 events that have stage2 placements but were never in PBP:
  1200325415 — 2008 Greater Rochester Area Shred Symposium
  1360305756 — 2013 U.S. Open Freestyle Footbag Championships
  1401624489 — 2014 5th annual Helsinki Open Footbag Tournament
  1408070192 — 2015 New Year's Footbag Jam
  1447494731 — 2016 Lake Erie Footbag Tournament
  1664206719 — 2023 US Open

Reads:  inputs/identity_lock/Placements_ByPerson_v42.csv
        inputs/identity_lock/Persons_Truth_Final_v36.csv
Writes: inputs/identity_lock/Placements_ByPerson_v43.csv
"""

from __future__ import annotations

import csv
import sys
import unicodedata
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LOCK = REPO / "inputs" / "identity_lock"

IN_PBP  = LOCK / "Placements_ByPerson_v42.csv"
OUT_PBP = LOCK / "Placements_ByPerson_v43.csv"
PT_CSV  = LOCK / "Persons_Truth_Final_v36.csv"

csv.field_size_limit(10_000_000)

# ── Name aliases: source name (lowercase) → PT person_canon (lowercase) ────────
_NAME_ALIASES: dict[str, str] = {
    "zack forth":                  "zach forth",
    "mathew kemmer":               "matt kemmer",
    "daniel greer":                "dan greer",
    "anthony ritz":                "anthony tony ritz",
    "robert mccloskey":            "rob mccloskey",
    "chris siebert":               "christopher michael siebert",
    "luka weyler-lavallée":        "luka weyler",
    "luka weyler-lavallee":        "luka weyler",
    "mark hunsbuger":              "mark hunsburger",
    "mark huntsbuger":             "mark hunsburger",
    "tuomas kärki":                "tuomas karki",
    "piia tantarimäki":            "piia tantarimaki",
    "jani säntti":                 "jani santti",
    "oskari forstén":              "oskari forsten",
    "léa lespérance":              "léa lespérance",       # exact PT match
    "lea lesperance":              "léa lespérance",
    "léa l\u00e8sp\u00e9rance":   "léa lespérance",       # encoding corruption variant
    # Resolved via background agent PT search
    "antoine the rocks-godin":     "antoine desrochers godin",   # a6723fe0
    "sammy hogan":                 "samuel hogan",               # 211b46a9
    "jesse ruotsalainen":          "jesse ruotsolainen",          # 69c15972
    "nathan pipenburg":            "nathan pipenberg",            # eae790d9
    "khoa nyguyen":                "khoa nguyen",                 # f06b66e5 (legacy file typo)
}

# S = singles player, T = team entry
# (div_canon, div_cat, place, comp_type, p1_name, p2_name_or_None)
CORRECT_PLACEMENTS: dict[str, list[tuple]] = {
    "1200325415": [
        ("Routines",   "freestyle", 1, "S", "Gordon Bevier",   None),
        ("Routines",   "freestyle", 2, "S", "Jay Boychuk",     None),
        ("Routines",   "freestyle", 3, "S", "Zach Forth",      None),
        ("Sick 1",     "freestyle", 1, "S", "Jorden Moir",     None),
        ("Sick 1",     "freestyle", 2, "S", "Will Digges",     None),
        ("Sick 1",     "freestyle", 3, "S", "Zach Forth",      None),
        ("Sick 3",     "freestyle", 1, "S", "Will Digges",     None),
        ("Sick 3",     "freestyle", 2, "S", "Jorden Moir",     None),
        ("Sick 3",     "freestyle", 3, "S", "Matt Kemmer",     None),
        ("Ten",        "freestyle", 1, "S", "Will Digges",     None),
        ("Ten",        "freestyle", 2, "S", "Tom Mosher",      None),
        ("Ten",        "freestyle", 3, "S", "Jay Boychuk",     None),
        ("Circle",     "freestyle", 1, "S", "Jorden Moir",     None),
        ("Circle",     "freestyle", 2, "S", "Gordon Bevier",   None),
        ("Circle",     "freestyle", 3, "S", "Ianek Regimbald", None),
    ],
    "1360305756": [
        ("Open Singles Routines", "freestyle", 1, "S", "Ken Somolinos",       None),
        ("Open Singles Routines", "freestyle", 2, "S", "Nick Landes",         None),
        ("Open Singles Routines", "freestyle", 3, "S", "Brian Sherrill",      None),
        ("Open Singles Routines", "freestyle", 4, "S", "Rory Dawson",         None),
        ("Open Singles Routines", "freestyle", 5, "S", "Chris Dean",          None),
        ("Open Singles Routines", "freestyle", 6, "S", "Dustin Rhodes",       None),
        ("Open Singles Routines", "freestyle", 7, "S", "Alex Dworetzky",      None),
        ("Open Singles Routines", "freestyle", 7, "S", "Gordon Bevier",       None),
        ("Open Singles Routines", "freestyle", 9, "S", "Larry Workman",       None),
        ("Open Circle Contest",   "freestyle", 1, "S", "Nick Landes",         None),
        ("Open Circle Contest",   "freestyle", 2, "S", "Ken Somolinos",       None),
        ("Open Circle Contest",   "freestyle", 3, "S", "Chris Dean",          None),
        ("Open Circle Contest",   "freestyle", 4, "S", "Brian Sherrill",      None),
        ("Open Circle Contest",   "freestyle", 5, "S", "Alex Dworetzky",      None),
        ("Open Circle Contest",   "freestyle", 5, "S", "Dustin Rhodes",       None),
        ("Open Circle Contest",   "freestyle", 7, "S", "Joshua Munstermann",  None),
    ],
    "1401624489": [
        ("Open Singles Net",  "net", 1, "S", "Tuomas Kärki",    None),
        ("Open Singles Net",  "net", 2, "S", "Matti Pohjola",   None),
        ("Open Singles Net",  "net", 3, "S", "Janne Uusitalo",  None),
        ("Open Singles Net",  "net", 4, "S", "Piia Tantarimäki",None),
        ("Open Singles Net",  "net", 5, "S", "Jyri Ilama",      None),
        ("Open Singles Net",  "net", 6, "S", "Evgeni Shiryaev", None),
        ("Open Doubles Net",  "net", 1, "T", "Matti Pohjola",   "Janne Uusitalo"),
        ("Open Doubles Net",  "net", 2, "T", "Jani Säntti",     "Oskari Forstén"),
        ("Open Doubles Net",  "net", 3, "T", "Tuomas Kärki",    "Jyri Ilama"),
        ("Open Doubles Net",  "net", 4, "T", "Evgeni Shiryaev", "Alexander Smirnov"),
        ("Open Doubles Net",  "net", 5, "T", "Jesse Ruotsalainen", "Piia Tantarimäki"),
        ("Open Doubles Net",  "net", 6, "T", "Aleksi Airinen",  "Samu Ahola"),
    ],
    "1408070192": [
        ("Int Circle",   "freestyle", 1, "S", "David Moutard",  None),
        ("Int Circle",   "freestyle", 2, "S", "Ryan Morris",    None),
        ("Int Circle",   "freestyle", 3, "S", "Ben Baybak",     None),
        ("Open Circle",  "freestyle", 1, "S", "Evan Gatesman",  None),
        ("Open Circle",  "freestyle", 2, "S", "Nick Landes",    None),
        ("Open Circle",  "freestyle", 3, "S", "Nathan Pipenburg",None),
        ("Int Request",  "freestyle", 1, "S", "Khoa Nguyen",    None),
        ("Open Request", "freestyle", 1, "S", "Evan Gatesman",  None),
        ("Open Shred",   "freestyle", 1, "S", "Brian Sherrill", None),
        ("Ironman",      "freestyle", 1, "S", "Matt Kemmer",    None),
    ],
    "1447494731": [
        ("Open Singles Net",    "net", 1, "S", "Daniel Greer",     None),
        ("Open Singles Net",    "net", 2, "S", "Anthony Ritz",     None),
        ("Open Singles Net",    "net", 3, "S", "Jim Hogan",        None),
        ("Open Singles Net",    "net", 4, "S", "Robert McCloskey", None),
        ("Open Singles Net",    "net", 5, "S", "Steve Richardson", None),
        ("Open Singles Net",    "net", 6, "S", "Dan Johnson",      None),
        ("Open Doubles Routines", "freestyle", 1, "T", "Daniel Greer",   "Robert McCloskey"),
        ("Open Doubles Routines", "freestyle", 2, "T", "Anthony Ritz",   "Steve Richardson"),
        ("Open Doubles Routines", "freestyle", 3, "T", "Jim Hogan",      "Dan Johnson"),
    ],
    "1664206719": [
        ("Open Singles Net",       "net",       1, "S", "Luka Weyler-Lavallée",    None),
        ("Open Singles Net",       "net",       2, "S", "Chris Siebert",           None),
        ("Open Singles Net",       "net",       3, "S", "Daniel Greer",            None),
        ("Open Singles Net",       "net",       4, "S", "Jim Hogan",               None),
        ("Open Singles Net",       "net",       5, "S", "Antoine The Rocks-Godin", None),
        ("Open Singles Net",       "net",       6, "S", "Anthony Ritz",            None),
        ("Open Singles Net",       "net",       7, "S", "Mark Hunsbuger",          None),
        ("Open Singles Net",       "net",       8, "S", "Rob McCloskey",           None),
        ("Open Singles Net",       "net",       9, "S", "Sammy Hogan",             None),
        ("Open Doubles Net",       "net",       1, "T", "Luka Weyler-Lavallée",    "Emmanuel Bouchard"),
        ("Open Doubles Net",       "net",       2, "T", "Daniel Greer",            "Anthony Ritz"),
        ("Open Doubles Net",       "net",       3, "T", "Chris Siebert",           "Rob McCloskey"),
        ("Open Doubles Net",       "net",       4, "T", "Léa Lespérance",          "Antoine The Rocks-Godin"),
        ("Open Doubles Net",       "net",       5, "T", "Mark Hunsbuger",          "Jim Hogan"),
        ("Intermediate Singles Net","net",      1, "S", "Lyric Ester",             None),
        ("Intermediate Singles Net","net",      2, "S", "Dan Johnson",             None),
        ("Intermediate Singles Net","net",      3, "S", "Benjamin Babiak",         None),
        ("Intermediate Singles Net","net",      4, "S", "Kyle",                    None),
        ("Intermediate Doubles Net","net",      1, "T", "Benjamin Babiak",         "Joe"),
        ("Intermediate Doubles Net","net",      2, "T", "Kyle",                    "Lyric Ester"),
        ("Circle Contest",         "freestyle", 1, "S", "Mathew Kemmer",           None),
        ("Circle Contest",         "freestyle", 2, "S", "Kevin Hogan",             None),
        ("Circle Contest",         "freestyle", 3, "S", "Peter Bowler",            None),
        ("Circle Contest",         "freestyle", 4, "S", "Josh Gayhart",            None),
        ("Open Routines",          "freestyle", 1, "S", "Brian Sherrill",          None),
        ("Open Routines",          "freestyle", 2, "S", "Christian Britting",      None),
        ("Open Routines",          "freestyle", 3, "S", "Kevin Hogan",             None),
        ("Open Routines",          "freestyle", 4, "S", "Benjamin Barrows",        None),
        ("Intermediate Routines",  "freestyle", 1, "S", "Ken Moller",              None),
        ("Intermediate Routines",  "freestyle", 2, "S", "Lyric Ester",             None),
    ],
}

# Year for each event
_EVENT_YEAR: dict[str, str] = {
    "1200325415": "2008",
    "1360305756": "2013",
    "1401624489": "2014",
    "1408070192": "2015",
    "1447494731": "2016",
    "1664206719": "2023",
}


def _strip_accents(s: str) -> str:
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _load_pt(pt_csv: Path) -> dict[str, tuple[str, str]]:
    """Return {lower_name: (uuid, canon)}"""
    pt: dict[str, tuple[str, str]] = {}
    with open(pt_csv, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            uid   = row["effective_person_id"]
            canon = row["person_canon"]
            if not uid or not canon:
                continue
            for field in ("person_canon", "player_names_seen", "aliases"):
                for raw in row.get(field, "").split(" | "):
                    name = raw.strip()
                    if not name:
                        continue
                    key = name.lower()
                    if key not in pt:
                        pt[key] = (uid, canon)
                    # also stripped-accent version
                    key2 = _strip_accents(key)
                    if key2 not in pt:
                        pt[key2] = (uid, canon)
    return pt


def _resolve(name: str, pt: dict) -> tuple[str, str] | None:
    """Return (uuid, canon) or None."""
    key = _NAME_ALIASES.get(name.lower(), name.lower())
    if key in pt:
        return pt[key]
    # try accent-stripped
    stripped = _strip_accents(key)
    if stripped in pt:
        return pt[stripped]
    return None


def _make_team_person_key(ids: list[str]) -> str:
    return "|".join(i for i in ids if i)


def _uuid5_team(event_id: str, div: str, place: int, p1: str, p2: str) -> str:
    """Stable UUID for a team entry using UUID5 on a deterministic string."""
    seed = f"{event_id}|{div}|{place}|{p1}|{p2}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def _norm(name: str) -> str:
    return name.lower().strip()


def main() -> int:
    print(f"Loading PT: {PT_CSV.name}")
    pt = _load_pt(PT_CSV)

    print(f"Loading PBP: {IN_PBP.name}")
    with open(IN_PBP, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        existing = list(reader)

    new_rows: list[dict] = []
    partial_persons: list[str] = []

    for eid, placements in CORRECT_PLACEMENTS.items():
        year = _EVENT_YEAR[eid]
        for (div_canon, div_cat, place, comp_type, p1_raw, p2_raw) in placements:
            row: dict = {f: "" for f in fieldnames}
            row["event_id"]         = eid
            row["year"]             = year
            row["division_canon"]   = div_canon
            row["division_category"]= div_cat
            row["place"]            = str(place)
            row["competitor_type"]  = "player" if comp_type == "S" else "team"

            # Resolve names
            r1 = _resolve(p1_raw, pt)
            r2 = _resolve(p2_raw, pt) if p2_raw else None

            # Singles
            if comp_type == "S":
                if r1:
                    uid1, canon1 = r1
                    row["person_id"]     = uid1
                    row["person_canon"]  = canon1
                    row["coverage_flag"] = "complete"
                    row["norm"]          = _norm(canon1)
                else:
                    partial_persons.append(f"{eid} {div_canon} p{place}: {p1_raw}")
                    row["person_id"]     = ""
                    row["person_canon"]  = "__NON_PERSON__"
                    row["coverage_flag"] = "partial"
                    row["team_display_name"] = p1_raw
                    row["norm"]          = ""

            # Team
            else:
                ids = []
                canons = []
                display_parts = []
                all_resolved = True
                for raw, res in [(p1_raw, r1), (p2_raw, r2)]:
                    if raw is None:
                        continue
                    display_parts.append(raw)
                    if res:
                        ids.append(res[0])
                        canons.append(res[1])
                    else:
                        all_resolved = False
                        partial_persons.append(f"{eid} {div_canon} p{place}: {raw}")

                row["team_person_key"]   = _make_team_person_key(ids) if ids else \
                                           _uuid5_team(eid, div_canon, place, p1_raw, p2_raw or "")
                row["person_id"]         = ""
                row["person_canon"]      = "__NON_PERSON__"
                row["team_display_name"] = " / ".join(display_parts)
                row["coverage_flag"]     = "complete" if all_resolved else "partial"
                row["norm"]              = ""

            new_rows.append(row)

    all_rows = existing + new_rows
    with open(OUT_PBP, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nNew rows added: {len(new_rows)}")
    print(f"  by event:")
    for eid, pls in CORRECT_PLACEMENTS.items():
        print(f"    {eid} ({_EVENT_YEAR[eid]}): {len(pls)} rows")
    print(f"\nPartial-coverage entries ({len(partial_persons)}):")
    for p in partial_persons:
        print(f"  {p}")
    print(f"\nTotal rows: {len(all_rows)}")
    print(f"Written: {OUT_PBP.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
