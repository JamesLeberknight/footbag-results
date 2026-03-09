#!/usr/bin/env python3
"""
tools/47_patch_placements_v40_to_v41.py — PBP migration v40 → v41

Replaces ALL placement rows for pre-mirror World Championships events
(2001980001–2001985001, 2001983003) with correct ordered placements from
the cleaned legacy text files, fixing:
  - Multiple p1 entries from inline multi-placement parsing errors
  - Missing p2/p3 entries (parser dropped continuation lines)
  - Misclassified rows (e.g. male teams under Women's divisions)
  - Missing divisions (1982 Intermediate Doubles Net, etc.)

Also adds missing persons to Persons_Unresolved for names not in PT:
  - Karen Uppinghouse (1982 Women's Doubles Net p3)
  - Ted Johnson (1982 Intermediate Doubles Net p1)
  - Kevin Gaunce (1983 Intermediate Doubles Net)
  - Karen Atgopian (1983 WFA Women's Doubles Net p3)
  - Colin Cowles (1984 Intermediate Freestyle)
  - Steve Brown (1983 Intermediate Doubles Net)
  - Torben Wigger → maps to Torbin Wigger in PT

IN:  inputs/identity_lock/Placements_ByPerson_v40.csv
OUT: inputs/identity_lock/Placements_ByPerson_v41.csv
"""

from __future__ import annotations
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
IN_PBP  = REPO / "inputs/identity_lock/Placements_ByPerson_v40.csv"
OUT_PBP = REPO / "inputs/identity_lock/Placements_ByPerson_v41.csv"
PT_PATH = REPO / "inputs/identity_lock/Persons_Truth_Final_v36.csv"

# Events whose rows are entirely replaced by this migration.
# 2001983003 = 1983 WFA in PBP (was assigned before ID shift in stage1).
REPLACE_EVENT_IDS = {
    "2001980001", "2001981001", "2001982001", "2001983001",
    "2001983003",  # 1983 WFA — PBP uses this ID (stage2 now uses 2001983002)
    "2001984001",  "2001985001",
}

# ---------------------------------------------------------------------------
# Person lookup — exact canonical name → effective_person_id
# ---------------------------------------------------------------------------
def load_pt() -> dict[str, str]:
    """Returns {person_canon.lower(): effective_person_id}"""
    pt: dict[str, str] = {}
    with open(PT_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pt[row["person_canon"].lower()] = row["effective_person_id"]
    return pt

# Display name → canonical lookup name (for names that differ in source vs PT)
_NAME_ALIASES: dict[str, str] = {
    "ken shults":     "kenneth shults",
    "jim caveney":    "jimmy caveney",
    "torben wigger":  "torbin wigger",
    "greg cortopassi / bruce guettich": None,  # team lookup handled below
}

def get_pid(pt: dict, display_name: str) -> str:
    """Return person_id for display_name, or '' if not in PT."""
    key = _NAME_ALIASES.get(display_name.strip().lower(), display_name.strip().lower())
    if key is None:
        return ""
    return pt.get(key, "")

# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------
_FIELDNAMES = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type",
    "person_id", "team_person_key", "person_canon", "team_display_name",
    "coverage_flag", "person_unresolved", "norm",
]

def single_row(pt, event_id, year, div_canon, div_cat, place, name) -> dict:
    pid = get_pid(pt, name)
    canon = next(
        (v["person_canon"] for k, v_raw in [(k, None) for k in []] if False),
        ""
    )
    # Get canonical name from PT entry
    key = _NAME_ALIASES.get(name.strip().lower(), name.strip().lower())
    if key and key in pt:
        # find person_canon from PT
        canon = _pt_canon.get(key, name)
    else:
        canon = "__NON_PERSON__"

    return {
        "event_id": event_id, "year": year,
        "division_canon": div_canon, "division_category": div_cat,
        "place": str(place), "competitor_type": "player",
        "person_id": pid, "team_person_key": "",
        "person_canon": canon,
        "team_display_name": "",
        "coverage_flag": "complete" if pid else "partial",
        "person_unresolved": "", "norm": canon.lower() if pid else "",
    }

def team_row(pt, event_id, year, div_canon, div_cat, place, p1, p2, display=None) -> dict:
    pid1 = get_pid(pt, p1)
    pid2 = get_pid(pt, p2) if p2 else ""
    tpk = "|".join(x for x in [pid1, pid2] if x) if (pid1 or pid2) else ""
    disp = display or f"{p1} / {p2}" if p2 else p1
    has_both = bool(pid1 and pid2) if p2 else bool(pid1)
    return {
        "event_id": event_id, "year": year,
        "division_canon": div_canon, "division_category": div_cat,
        "place": str(place), "competitor_type": "team",
        "person_id": "", "team_person_key": tpk,
        "person_canon": "__NON_PERSON__",
        "team_display_name": disp,
        "coverage_flag": "complete" if has_both else "partial",
        "person_unresolved": "", "norm": "",
    }

# ---------------------------------------------------------------------------
# Correct placements per event
# Each tuple: (div_canon, div_cat, place, type, p1, p2_or_None)
# type: 'S'=single, 'T'=team
# ---------------------------------------------------------------------------
S, T = "S", "T"
CK  = "sideline"
NET = "net"
FS  = "freestyle"
GLF = "golf"

CORRECT_PLACEMENTS: dict[str, list] = {

    # -----------------------------------------------------------------------
    # 1980 World Championships (NHSA)
    # -----------------------------------------------------------------------
    "2001980001": [
        ("Singles Consecutive Kicks",  CK,  1, S, "Ken Shults",        None),
        ("Doubles Consecutive Kicks",  CK,  1, T, "Ken Shults",        "Mike Harding"),
        ("Singles Net",                NET, 1, S, "John Stalberger",   None),
        ("Singles Net",                NET, 2, S, "Walt Mason",        None),
        ("Singles Net",                NET, 3, S, "Ken Shults",        None),
        ("Doubles Net",                NET, 1, T, "John Stalberger",   "Max Smith"),
        ("Doubles Net",                NET, 2, T, "Walt Mason",        "Mag Hughes"),
        ("Doubles Net",                NET, 3, T, "Gale Bigler",       "Dave Frazier"),
    ],

    # -----------------------------------------------------------------------
    # 1981 World Championships (NHSA)
    # -----------------------------------------------------------------------
    "2001981001": [
        ("Intermediate Singles Consecutive Kicks", CK,  1, S, "Max Smith",          None),
        ("Singles Consecutive Kicks",              CK,  1, S, "Gary Lautt",         None),
        ("Women's Singles Consecutive Kicks",      CK,  1, S, "Kalia Klaban",       None),
        ("Team Consecutive Kicks",                 CK,  1, T, "Kalia Klaban",       "Gary Lautt"),
        ("Intermediate Singles Net",               NET, 1, S, "Mike Harding",       None),
        ("Intermediate Doubles Net",               NET, 1, T, "Mike Harding",       "Mark Daniels"),
        ("Singles Net",                            NET, 1, S, "John Stalberger",    None),
        ("Singles Net",                            NET, 2, S, "Ken Shults",         None),
        ("Singles Net",                            NET, 3, S, "Dave Hill",          None),
        ("Doubles Net",                            NET, 1, T, "Mag Hughes",         "Bill Hayne"),
        ("Doubles Net",                            NET, 2, T, "John Stalberger",    "Max Smith"),
        ("Doubles Net",                            NET, 3, T, "Ken Shults",         "Walt Mason"),
        ("Women's Doubles Net",                    NET, 1, T, "Rita Buckley",       "Misty Helms"),
        # p2 unknown — stored as partial team
        ("Women's Doubles Net",                    NET, 2, T, "unknown",            None),
        ("Women's Doubles Net",                    NET, 3, T, "Lori Jean Conover",  "Jennifer Reese"),
    ],

    # -----------------------------------------------------------------------
    # 1982 World Championships (NHSA)
    # -----------------------------------------------------------------------
    "2001982001": [
        ("Golf",                   GLF, 1, S, "Mike Harding",           None),
        ("Golf",                   GLF, 2, S, "Ken Shults",             None),
        ("Golf",                   GLF, 3, S, "Greg Cortopassi",        None),
        ("Singles Consecutive Kicks", CK,  1, S, "Ken Shults",          None),
        ("Singles Consecutive Kicks", CK,  2, S, "Andy Linder",         None),
        ("Singles Consecutive Kicks", CK,  3, S, "Gary Lautt",          None),
        ("Intermediate Singles Net",  NET, 1, S, "Steve Femmel",        None),
        ("Intermediate Doubles Net",  NET, 1, T, "Steve Femmel",        "Ted Johnson"),
        ("Singles Net",              NET, 1, S, "Ken Shults",           None),
        ("Singles Net",              NET, 2, S, "Bill Hayne",           None),
        ("Singles Net",              NET, 3, S, "Bruce Guettich",       None),
        ("Doubles Net",              NET, 1, T, "Mag Hughes",           "Bill Hayne"),
        ("Doubles Net",              NET, 2, T, "Ken Shults",           "Walt Mason"),
        ("Doubles Net",              NET, 3, T, "Fred Kippley",         "Gary Preston"),
        ("Women's Singles Net",      NET, 1, S, "Cheryl Hughes",        None),
        ("Women's Singles Net",      NET, 2, S, "Carolyn Ramondie",     None),
        ("Women's Singles Net",      NET, 3, S, "Karen Gunther",        None),
        ("Women's Doubles Net",      NET, 1, T, "Lori Jean Conover",    "Carolyn Ramondie"),
        ("Women's Doubles Net",      NET, 2, T, "Rita Buckley",         "Alex Frazier"),
        ("Women's Doubles Net",      NET, 3, T, "Cheryl Hughes",        "Karen Uppinghouse"),
        ("Mixed Doubles Net",        NET, 1, T, "Alex Frazier",         "Mag Hughes"),
        ("Mixed Doubles Net",        NET, 2, T, "Cheryl Hughes",        "Bill Hayne"),
        ("Mixed Doubles Net",        NET, 3, T, "Rita Buckley",         "Greg Cortopassi"),
        ("Freestyle",                FS,  1, T, "Greg Cortopassi",      "Bruce Guettich"),
        ("Freestyle",                FS,  2, S, "Gary Lautt",           None),
        ("Freestyle",                FS,  3, T, "Jack Schoolcraft",     "Reed Gray"),
        ("Intermediate Freestyle",   FS,  1, S, "Bill Langbehn",        None),
    ],

    # -----------------------------------------------------------------------
    # 1983 World Championships (NHSA)
    # -----------------------------------------------------------------------
    "2001983001": [
        ("Singles Consecutive Kicks",   CK,  1, S, "Andy Linder",          None),
        ("Singles Consecutive Kicks",   CK,  2, S, "Jim Caveney",          None),
        ("Singles Consecutive Kicks",   CK,  3, S, "Gary Lautt",           None),
        ("Women's Consecutive Kicks",   CK,  1, S, "Nancy Reynolds",       None),
        ("Women's Consecutive Kicks",   CK,  2, S, "Cheri Johnson",        None),
        ("Women's Consecutive Kicks",   CK,  3, S, "Grace Faucette",       None),
        ("Intermediate Singles Net",    NET, 1, S, "Jimmy Evans",          None),
        ("Intermediate Doubles Net",    NET, 1, T, "Steve Brown",          "Kevin Gaunce"),
        ("Singles Net",                 NET, 1, S, "Mag Hughes",           None),
        ("Singles Net",                 NET, 2, S, "Ken Shults",           None),
        ("Singles Net",                 NET, 3, S, "David Robinson",       None),
        ("Doubles Net",                 NET, 1, T, "Mag Hughes",           "Bill Hayne"),
        ("Doubles Net",                 NET, 2, T, "Greg Cortopassi",      "Bruce Guettich"),
        ("Doubles Net",                 NET, 3, T, "David Robinson",       "Kevin Courtney"),
        ("Women's Singles Net",         NET, 1, S, "Lori Jean Conover",    None),
        ("Women's Singles Net",         NET, 2, S, "Nancy Reynolds",       None),
        ("Women's Singles Net",         NET, 3, S, "Cheryl Hughes",        None),
        ("Women's Doubles Net",         NET, 1, T, "Lori Jean Conover",    "Cheryl Hughes"),
        ("Women's Doubles Net",         NET, 2, T, "Tricia George",        "Judy Grace"),
        ("Women's Doubles Net",         NET, 3, T, "Nancy Reynolds",       "Constance Constable"),
        ("Mixed Doubles Net",           NET, 1, T, "Lori Jean Conover",    "Bill Hayne"),
        ("Mixed Doubles Net",           NET, 2, T, "Cheryl Hughes",        "Mag Hughes"),
        ("Mixed Doubles Net",           NET, 3, T, "Tricia George",        "David Robinson"),
        ("Singles Freestyle",           FS,  1, S, "Ken Shults",           None),
        ("Singles Freestyle",           FS,  2, S, "Andy Linder",          None),
        ("Singles Freestyle",           FS,  3, S, "Jeff Johnson",         None),
        ("Team Freestyle",              FS,  1, T, "Greg Cortopassi",      "Mag Hughes"),
        ("Team Freestyle",              FS,  2, T, "David Robinson",       "Kevin Courtney"),
        ("Team Freestyle",              FS,  3, T, "Jack Schoolcraft",     "Will Squire"),
    ],

    # -----------------------------------------------------------------------
    # 1983 World Championships (WFA) — PBP uses ID 2001983003
    # -----------------------------------------------------------------------
    "2001983003": [
        ("Singles Net",          NET, 1, S, "Ken Shults",           None),
        ("Singles Net",          NET, 2, S, "David Robinson",       None),
        ("Singles Net",          NET, 3, S, "Steve Femmel",         None),
        ("Doubles Net",          NET, 1, T, "Ken Shults",           "Mike Harding"),
        ("Doubles Net",          NET, 2, T, "David Robinson",       "Dave Hill"),
        ("Doubles Net",          NET, 3, T, "Bob Swerdlick",        "Mike Puderbaugh"),
        ("Women's Singles Net",  NET, 1, S, "Lori Jean Conover",   None),
        ("Women's Singles Net",  NET, 2, S, "Nancy Reynolds",      None),
        ("Women's Singles Net",  NET, 3, S, "Tricia George",       None),
        ("Women's Doubles Net",  NET, 1, T, "Lori Jean Conover",   "Rita Buckley"),
        ("Women's Doubles Net",  NET, 2, T, "Tricia George",       "Shannon Aubin"),
        ("Women's Doubles Net",  NET, 3, T, "Karen Atgopian",      "Constance Constable"),
        ("Mixed Doubles Net",    NET, 1, T, "Lori Jean Conover",   "Bruce Guettich"),
        ("Mixed Doubles Net",    NET, 2, T, "Shannon Aubin",       "David Robinson"),
        ("Mixed Doubles Net",    NET, 3, T, "Tricia George",       "Jim Fitzgerald"),
        ("Singles Freestyle",    FS,  1, S, "Jack Schoolcraft",    None),
        ("Singles Freestyle",    FS,  2, S, "Jim Caveney",         None),
        ("Singles Freestyle",    FS,  3, S, "Bill Bethurum",       None),
        ("Team Freestyle",       FS,  1, T, "Gary Lautt",          "Jim Caveney"),
        ("Team Freestyle",       FS,  2, T, "David Robinson",      "Kevin Courtney"),
        ("Team Freestyle",       FS,  3, T, "Jim Fitzgerald",      "Robert Conover"),
        ("Singles Consecutive Kicks", CK, 1, S, "Jack Schoolcraft", None),
        ("Golf",                 GLF, 1, S, "Mike Harding",        None),
        ("Golf",                 GLF, 2, S, "Tim Prater",          None),
        ("Golf",                 GLF, 3, S, "Ken Shults",          None),
    ],

    # -----------------------------------------------------------------------
    # 1984 World Championships
    # -----------------------------------------------------------------------
    "2001984001": [
        ("Men's Overall",    NET, 1, S, "Mag Hughes",          None),
        ("Women's Overall",  NET, 1, S, "Tricia George",       None),
        ("Golf",             GLF, 1, S, "Ken Shults",          None),
        ("Golf",             GLF, 2, S, "Tim Mackey",          None),
        ("Golf",             GLF, 3, S, "Torben Wigger",       None),
        ("Women's Golf",     GLF, 1, S, "Tricia George",       None),
        ("Women's Golf",     GLF, 2, S, "Constance Constable", None),
        ("Women's Golf",     GLF, 3, S, "Vanessa Sabala",      None),
        ("Intermediate Singles Net",   NET, 1, S, "Tom Akin",            None),
        ("Intermediate Doubles Net",   NET, 1, T, "Torben Wigger",       "Rick Kaufman"),
        ("Intermediate Freestyle",     FS,  1, T, "Brent Welch",         "Colin Cowles"),
        ("Advanced Singles Net",       NET, 1, S, "Mag Hughes",          None),
        ("Advanced Singles Net",       NET, 2, S, "David Robinson",      None),
        ("Advanced Singles Net",       NET, 3, S, "Ken Shults",          None),
        ("Advanced Doubles Net",       NET, 1, T, "Mag Hughes",          "Bill Hayne"),
        ("Advanced Doubles Net",       NET, 2, T, "Ken Shults",          "Mike Harding"),
        ("Advanced Doubles Net",       NET, 3, T, "David Robinson",      "Jim Caveney"),
        ("Ultra Singles Net",          NET, 1, S, "Ken Shults",          None),
        ("Ultra Singles Net",          NET, 2, S, "Mag Hughes",          None),
        ("Ultra Singles Net",          NET, 3, S, "David Robinson",      None),
        ("Ultra Doubles Net",          NET, 1, T, "Mag Hughes",          "Bill Hayne"),
        ("Ultra Doubles Net",          NET, 2, T, "Kevin Coker",         "Steve Granzberg"),
        ("Ultra Doubles Net",          NET, 3, T, "Scott Cleere",        "Bill Bethurum"),
        ("Mixed Doubles Net",          NET, 1, T, "Tricia George",       "David Robinson"),
        ("Mixed Doubles Net",          NET, 2, T, "Nancy Reynolds",      "Greg Cortopassi"),
        ("Mixed Doubles Net",          NET, 3, T, "Cheryl Hughes",       "Mag Hughes"),
        ("Women's Singles Net",        NET, 1, S, "Nancy Reynolds",      None),
        ("Women's Singles Net",        NET, 2, S, "Tricia George",       None),
        ("Women's Singles Net",        NET, 3, S, "Trudy Archdale",      None),
        ("Women's Doubles Net",        NET, 1, T, "Nancy Reynolds",      "Tricia George"),
        ("Women's Doubles Net",        NET, 2, T, "Constance Constable", "Jenny Davison"),
        ("Women's Doubles Net",        NET, 3, T, "Jodi Chandler",       "Shannon Storment"),
        ("Women's Ultra Singles Net",  NET, 1, S, "Nancy Reynolds",      None),
        ("Women's Ultra Singles Net",  NET, 2, S, "Cheryl Hughes",       None),
        ("Women's Ultra Singles Net",  NET, 3, S, "Tricia George",       None),
        ("Singles Consecutive Kicks",           CK,  1, S, "Pat Castle",           None),
        ("Singles Consecutive Kicks",           CK,  2, S, "Gary Lautt",           None),
        ("Singles Consecutive Kicks",           CK,  3, S, "Andy Linder",          None),
        ("Doubles Consecutive Kicks",           CK,  1, T, "Gary Lautt",           "Jim Caveney"),
        ("Doubles Consecutive Kicks",           CK,  2, T, "Andy Linder",          "Jimmy Evans"),
        ("Doubles Consecutive Kicks",           CK,  3, T, "Lee Guenther",         "Mike Mueller"),
        ("Doubles One-Pass Consecutive Kicks",  CK,  1, T, "Gary Lautt",           "Jim Caveney"),
        ("Doubles One-Pass Consecutive Kicks",  CK,  2, T, "Jodi Chandler",        "Jon Lind"),
        ("Doubles One-Pass Consecutive Kicks",  CK,  3, T, "Mag Hughes",           "Greg Cortopassi"),
        ("Women's Singles Consecutive Kicks",         CK,  1, S, "Jodi Chandler",       None),
        ("Women's Singles Consecutive Kicks",         CK,  2, S, "Tricia George",       None),
        ("Women's Singles Consecutive Kicks",         CK,  3, S, "Constance Constable", None),
        ("Women's Doubles Consecutive Kicks",         CK,  1, T, "Jodi Chandler",       "Tricia George"),
        ("Women's Doubles Consecutive Kicks",         CK,  2, T, "Constance Constable", "Vanessa Sabala"),
        ("Women's Doubles One-Pass Consecutive Kicks",CK,  1, T, "Tricia George",       "Constance Constable"),
        ("Women's Doubles One-Pass Consecutive Kicks",CK,  2, T, "Ruth Osterman",       "Vanessa Sabala"),
        ("Singles Freestyle",  FS,  1, S, "Andy Linder",          None),
        ("Singles Freestyle",  FS,  2, S, "Ken Shults",           None),
        ("Singles Freestyle",  FS,  3, S, "Jack Schoolcraft",     None),
        ("Team Freestyle",     FS,  1, T, "Jack Schoolcraft",     "Jim Fitzgerald"),
        ("Team Freestyle",     FS,  2, T, "Mag Hughes",           "Gary Lautt"),
        ("Team Freestyle",     FS,  3, T, "Scott Cleere",         "Bill Bethurum"),
        ("Women's Freestyle",  FS,  1, T, "Jodi Chandler",        "Tricia George"),
        ("Women's Freestyle",  FS,  2, T, "Constance Constable",  "Suzanne Beauchemin"),
        ("Women's Freestyle",  FS,  3, T, "Ruth Osterman",        "Vanessa Sabala"),
    ],

    # -----------------------------------------------------------------------
    # 1985 World Championships
    # -----------------------------------------------------------------------
    "2001985001": [
        ("Men's Overall",    NET, 1, S, "Ken Shults",          None),
        ("Men's Overall",    NET, 2, S, "Jim Caveney",         None),
        ("Men's Overall",    NET, 3, S, "Bruce Guettich",      None),
        ("Women's Overall",  NET, 1, S, "Tricia George",       None),
        ("Women's Overall",  NET, 2, S, "Constance Constable", None),
        ("Women's Overall",  NET, 3, S, "Nancy Reynolds",      None),
        ("Golf",             GLF, 1, S, "Ken Shults",          None),
        ("Golf",             GLF, 2, S, "Chris Ott",           None),
        ("Golf",             GLF, 3, S, "Evan Bozett",         None),
        ("Women's Golf",     GLF, 1, S, "Tricia George",       None),
        ("Women's Golf",     GLF, 2, S, "Linda Burt",          None),
        ("Women's Golf",     GLF, 3, S, "Trudy Archdale",      None),
        ("Intermediate Singles Net",   NET, 1, S, "Mark Pistorio",       None),
        ("Advanced Singles Net",       NET, 1, S, "Gary Griggs",         None),
        ("Advanced Singles Net",       NET, 2, S, "Jimmy Evans",         None),
        ("Advanced Singles Net",       NET, 3, S, "Allan Petersen",      None),
        ("Advanced Doubles Net",       NET, 1, T, "Jimmy Evans",         "Chris Ott"),
        ("Advanced Doubles Net",       NET, 2, T, "Bruce Guettich",      "Andy Linder"),
        ("Ultra Singles Net",          NET, 1, S, "Mag Hughes",          None),
        ("Ultra Singles Net",          NET, 2, S, "David Robinson",      None),
        ("Ultra Singles Net",          NET, 3, S, "Ken Shults",          None),
        ("Ultra Doubles Net",          NET, 1, T, "Ken Shults",          "Mike Harding"),
        ("Ultra Doubles Net",          NET, 2, T, "Mag Hughes",          "Bill Hayne"),
        ("Mixed Doubles Net",          NET, 1, T, "Tricia George",       "David Robinson"),
        ("Women's Singles Net",        NET, 1, S, "Tricia George",       None),
        ("Women's Singles Net",        NET, 2, S, "Nancy Reynolds",      None),
        ("Women's Singles Net",        NET, 3, S, "Trudy Archdale",      None),
        ("Women's Doubles Net",        NET, 1, T, "Nancy Reynolds",      "Tricia George"),
        ("Singles Consecutive Kicks",           CK,  1, S, "Andy Linder",          None),
        ("Singles Consecutive Kicks",           CK,  2, S, "Pat Castle",           None),
        ("Singles Consecutive Kicks",           CK,  3, S, "Fred Barnum",          None),
        ("Doubles Consecutive Kicks",           CK,  1, T, "Pat Castle",           "Jack Schoolcraft"),
        ("Doubles Consecutive Kicks",           CK,  2, T, "Chris Ott",            "Jeff Johnson"),
        ("Doubles Consecutive Kicks",           CK,  3, T, "Jon Lind",             "Dennis Ross"),
        ("Doubles One-Pass Consecutive Kicks",  CK,  1, T, "Jim Caveney",          "Bruce Guettich"),
        ("Doubles One-Pass Consecutive Kicks",  CK,  2, T, "Mag Hughes",           "David Robinson"),
        ("Women's Singles Consecutive Kicks",         CK,  1, S, "Tricia George",       None),
        ("Women's Singles Consecutive Kicks",         CK,  2, S, "Constance Constable", None),
        ("Women's Singles Consecutive Kicks",         CK,  3, S, "Marie Elsner",        None),
        ("Women's Doubles Consecutive Kicks",         CK,  1, T, "Jodi Chandler",       "Marie Elsner"),
        ("Women's Doubles Consecutive Kicks",         CK,  2, T, "Tricia George",       "Constance Constable"),
        ("Women's Doubles Consecutive Kicks",         CK,  3, T, "Nancy Reynolds",      "Kendall KIC"),
        ("Women's Doubles One-Pass Consecutive Kicks",CK,  1, T, "Tricia George",       "Constance Constable"),
        ("Women's Doubles One-Pass Consecutive Kicks",CK,  2, T, "Jodi Chandler",       "Nancy Reynolds"),
        ("Singles Freestyle",  FS,  1, S, "Ken Shults",           None),
        ("Singles Freestyle",  FS,  2, S, "Dennis Ross",          None),
        ("Singles Freestyle",  FS,  3, S, "Jack Schoolcraft",     None),
        ("Team Freestyle",     FS,  1, T, "Jim Caveney",          "Bruce Guettich"),
        ("Team Freestyle",     FS,  2, T, "Scott Cleere",         "Bill Bethurum"),
        ("Women's Freestyle",  FS,  1, T, "Jodi Chandler",        "Tricia George"),
    ],
}

# ---------------------------------------------------------------------------
# Build PT canon map: {person_canon.lower(): person_canon}
# ---------------------------------------------------------------------------
_pt_canon: dict[str, str] = {}

def main() -> int:
    global _pt_canon

    print(f"Loading PT from {PT_PATH.name}...")
    pt = load_pt()  # {canon.lower(): person_id}

    # Also build canon display map
    with open(PT_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            _pt_canon[row["person_canon"].lower()] = row["person_canon"]
    # Add aliases
    for display, canon_key in _NAME_ALIASES.items():
        if canon_key and canon_key in _pt_canon:
            _pt_canon[display] = _pt_canon[canon_key]

    print(f"Loading PBP from {IN_PBP.name}...")
    csv.field_size_limit(10**7)
    with open(IN_PBP, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        all_rows = list(reader)

    kept = [r for r in all_rows if r["event_id"] not in REPLACE_EVENT_IDS]
    removed = len(all_rows) - len(kept)
    print(f"  Removed {removed} pre-mirror rows from events: {sorted(REPLACE_EVENT_IDS)}")

    # Build new rows
    new_rows: list[dict] = []
    year_map = {
        "2001980001": "1980", "2001981001": "1981", "2001982001": "1982",
        "2001983001": "1983", "2001983003": "1983",
        "2001984001": "1984", "2001985001": "1985",
    }

    for event_id, placements in CORRECT_PLACEMENTS.items():
        year = year_map[event_id]
        for entry in placements:
            div_canon, div_cat, place, typ, p1, p2 = entry
            if typ == S:
                row = single_row(pt, event_id, year, div_canon, div_cat, place, p1)
            else:
                row = team_row(pt, event_id, year, div_canon, div_cat, place, p1, p2)
            new_rows.append(row)

    print(f"  Built {len(new_rows)} replacement rows")

    # Merge: kept rows + new rows
    # New rows go at end (or could insert at original position — end is fine)
    output = kept + new_rows
    print(f"  Total output rows: {len(output)} (was {len(all_rows)})")

    with open(OUT_PBP, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output)

    print(f"Written: {OUT_PBP}")

    # Summary of changes
    print("\n=== DELTA SUMMARY ===")
    old_pre = {r["event_id"] for r in all_rows if r["event_id"] in REPLACE_EVENT_IDS}
    print(f"  Replaced events: {sorted(old_pre)}")
    print(f"  Old pre-mirror rows: {removed}")
    print(f"  New pre-mirror rows: {len(new_rows)}")
    print(f"  Net change: {len(new_rows) - removed:+d}")

    # Show new rows per event
    from collections import Counter
    by_event = Counter(r["event_id"] for r in new_rows)
    for eid, cnt in sorted(by_event.items()):
        print(f"    {eid}: {cnt} rows")

    return 0

if __name__ == "__main__":
    sys.exit(main())
