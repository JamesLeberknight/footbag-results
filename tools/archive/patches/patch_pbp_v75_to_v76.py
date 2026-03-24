#!/usr/bin/env python3
"""
patch_pbp_v75_to_v76.py

Fixes two Montreal-area 2001 events with city-as-partner parsing artifacts and
missing placements. Both events used formats that caused spurious team rows and
dropped players.

EVENT 972427576 — L'Hivernal Windchill Tournament 2001, Montreal
  Problems:
  - "(City, Province)" format caused city to be parsed as doubles partner in
    singles/freestyle divisions → spurious old-format team rows in every division
  - "T2." tied-place prefix caused p2 (Sébastien Desgens + Simon Kolodenchuk)
    to be dropped from Routines Intermediate Freestyle
  - OSN p5 parsed as "Jean" (truncation of "Jean-Francois Lemieux")
  - ODN p5 + p7 had spurious "Jean" player rows
  - Routines Open Freestyle missing p4 (Samuel Jobin) and p5 (Danny Cardonne)
  - Shred Contest had Hugues Veillette with city in name (p11)
  - Phat Combo Contest Winner (Yacine Merzouk) missing entirely
  Fix: Full replacement — 77 rows removed, 62 rows inserted.

EVENT 991285803 — Montreal International Footbag Net Championships 2001, Montreal
  Problems:
  - Unlabeled section after Open Singles p19 ("1. Benoit Guillemette / 2. Ted Fritch")
    was merged into Open Singles, creating two p1 entries and two p2 entries
  - Open Doubles had old-format team rows (2 rows per team member) + spurious
    individual player rows (one per team) → 3 rows per team, should be 1
  - Spurious "Jean" player rows in Open Doubles (Jean-Francois Lemieux artifact)
  Fix: Full replacement — 65 rows removed, 40 rows inserted.
    - Benoit Guillemette + Ted Fritch moved to new "Intermediate Singles" division
    - Open Doubles converted to new piped-UUID team format (1 row per team)

Person resolutions:
  972427576:
    Tim Melnyk             → e2ab463f (PT)
    Jean-Philippe Rochefort → unresolved (not in PT; stage2 UUID: 6583dfa3)
    Joel Dion              → c2b71e3c (PT)
    Fabien Eggena          → 0759c6a8 (PT)
    Alexandre Bélanger     → 96b72fe6 (PT)
    Yves Archambault       → 691f48a0 (PT)
    Martin Cote            → 6d50650f (PT)
    Chris Seibert          → bd1ac97c (PT)
    Martin Graton          → d0fd4b0a (PT)
    Jean-Francois Lemieux  → 184a06bb (PT)
    Robert Lavigne         → f569a985 (PT)
    Renaud Fanoni          → bb4a96be (PT)
    Mario Vaillancourt     → 2caf7286 (PT)
    Stephane Comeau        → d7ee4909 (PT)
    Marilyn Demuy          → 40a3babb (PT)
    Olivier Tronchon       → 169a272b (PT)
    Philippe Larose        → 4184f9cc (PT)
    Benjamin Rochon        → c312c02d (PT)
    Éric Côté              → 68829443 (PT)
    Sebastien Verdy        → c0ba05a1 (PT)
    Emmanuel Bouchard      → 3ef63282 (PT)
    Hugues Oli Veillette   → 480189b0 (PT; source: "Hugues Veillette")
    Sébastien Desgens      → 2a875256 (PT)
    Simon Kolodenchuk      → unresolved (not in PT)
    Gabriel Gaudette       → 079f9c62 (PT)
    Antonin Leclair        → 5983d5d0 (PT)
    Marilou Ouimet         → 000a2598 (PT)
    Adam Pirog             → 5a74ce40 (PT)
    Caroline Bourgoin      → 13f2e206 (PT)
    Marc-André Veillette   → 0f1fdcb8 (PT)
    François Perreault     → ec487576 (PT)
    Luce Gaudette          → f9bd712d (PT)
    Yacine Merzouk         → 97d60d0e (PT)
    Kavin Thiffault        → 5d187fce (PT)
    Sebastien Duchesne     → 6dd904b7 (PT)
    Samuel Jobin           → c57afdbe (PT)
    Danny Cardonne         → d34a5fa9 (PT)
    Mark Edward Leeman     → 8072eba9 (PT)
    Diego La Manna         → 748a4598 (PT)

  991285803:
    Emmanuel Bouchard      → 3ef63282 (PT)
    Yves Archambault       → 691f48a0 (PT)
    Martin Graton          → d0fd4b0a (PT)
    David Butcher          → c1f6d696 (PT)
    Rob Adams              → 20d639b1 (PT)
    John Leys              → 3b938feb (PT)
    Jean-Francois Lemieux  → 184a06bb (PT)
    Patrick Asswad         → 202607a4 (PT)
    Jon Hayduk             → ceb19bce (PT; note: different from John Hayduk 5d4624fd)
    Stephane Comeau        → d7ee4909 (PT)
    Marilyn Demuy          → 40a3babb (PT)
    Chris Seibert          → bd1ac97c (PT; source: "Chris Siebert")
    Benjamin Rochon        → c312c02d (PT)
    Patrick Keehan         → a885c64b (PT; source: "Pat Keehan")
    Renaud Fanoni          → bb4a96be (PT)
    Philippe Lessard       → 87216aed (PT)
    Philippe Larose        → 4184f9cc (PT)
    Éric Côté              → 68829443 (PT; source: "Eric Cote")
    Jean-Philippe Rochefort → unresolved (stage2 UUID: 6583dfa3)
    Alexandre Bélanger     → 96b72fe6 (PT; source: "Alexandre Belanger")
    Natasha Mercure        → c3d1ffd9 (PT)
    Benoit Guillemette     → 19d5198c (PT)
    Theodore Fritsch       → 738cbf71 (PT; source: "Ted Fritch")
    Mathieu Vaillancourt   → 10a39ec4 (PT)
    Rémi ?                 → unresolved (person_unresolved=1)
    Katia Dignard          → 92fc06e1 (PT)
    Martin Cote            → 6d50650f (PT)
    Alexis Deschenes       → 4f763ef4 (PT)
    John Hayduk            → 5d4624fd (PT; source: "Andy Ronalds" partner)
    Andy Ronald            → 39bc6c51 (PT; source: "Andy Ronalds")
    Maude Landreville      → f2ce846c (PT)
    Robert Lavigne         → f569a985 (PT)
    Stephane Comeau        → d7ee4909 (PT)
    Natasha Mercure        → c3d1ffd9 (PT)
    Katia Dignard          → 92fc06e1 (PT)
    Mathieu Vaillancourt   → 10a39ec4 (PT)

Row counts:
  972427576 before: 77 rows → after: 62 rows (delta: -15)
  991285803 before: 65 rows → after: 40 rows (delta: -25)
  Net PBP change: -40 rows (28,864 → 28,824)

Output: inputs/identity_lock/Placements_ByPerson_v76.csv
"""

import csv
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
LOCK    = REPO / "inputs" / "identity_lock"
PBP_IN  = LOCK / "Placements_ByPerson_v75.csv"
PBP_OUT = LOCK / "Placements_ByPerson_v76.csv"

EVENT_A = "972427576"   # L'Hivernal 2001
EVENT_B = "991285803"   # Montreal Championships 2001

PBP_FIELDS = [
    "event_id", "year", "division_canon", "division_category",
    "place", "competitor_type", "person_id", "team_person_key",
    "person_canon", "team_display_name", "coverage_flag",
    "person_unresolved", "norm", "division_raw",
]


def player(event_id, year, place, pid, canon, div, div_cat, div_raw, cflag, unres=""):
    return {
        "event_id":          event_id,
        "year":              str(year),
        "division_canon":    div,
        "division_category": div_cat,
        "place":             str(place),
        "competitor_type":   "player",
        "person_id":         pid,
        "team_person_key":   "",
        "person_canon":      canon,
        "team_display_name": "",
        "coverage_flag":     cflag,
        "person_unresolved": unres,
        "norm":              canon.lower() if pid else "",
        "division_raw":      div_raw,
    }


def team(event_id, year, place, key, display, div, div_cat, div_raw, cflag):
    """New-format piped team row (1 row per team)."""
    return {
        "event_id":          event_id,
        "year":              str(year),
        "division_canon":    div,
        "division_category": div_cat,
        "place":             str(place),
        "competitor_type":   "team",
        "person_id":         "",
        "team_person_key":   key,
        "person_canon":      "__NON_PERSON__",
        "team_display_name": display,
        "coverage_flag":     cflag,
        "person_unresolved": "",
        "norm":              "",
        "division_raw":      div_raw,
    }


# ── UUIDs ──────────────────────────────────────────────────────────────────────

# 972427576 persons
TIM_MELNYK       = "e2ab463f-b112-5fdb-8d33-f9880800f358"
JOEL_DION        = "c2b71e3c-4ffb-5b75-b92f-cfbb4f0c9bb4"
FABIEN_EGGENA    = "0759c6a8-0e46-5a84-adc8-595351945fd6"
ALEX_BELANGER    = "96b72fe6-ed2f-5d70-8a49-2e2d38b31e5f"
YVES_ARCH        = "691f48a0-1dbd-5ef5-99ea-13615a7437d2"
MARTIN_COTE      = "6d50650f-7f41-5484-894f-74986085f48b"
CHRIS_SEIBERT    = "bd1ac97c-6c20-5bcd-9fbd-f5279f4c22bf"
MARTIN_GRATON    = "d0fd4b0a-a59e-525d-a918-eefb16c70e80"
JF_LEMIEUX       = "184a06bb-be96-5120-9a7c-1676f2b01a2a"
ROBERT_LAVIGNE   = "f569a985-6548-5b3d-9322-5e2c764bcc11"
RENAUD_FANONI    = "bb4a96be-f5b0-518a-a83d-2512486db5a6"
MARIO_VAILLANT   = "2caf7286-2a00-5527-8ec3-313132cf469b"
STEPH_COMEAU     = "d7ee4909-a76d-5639-aa82-ce4a8a7a53ba"
MARILYN_DEMUY    = "40a3babb-8d9d-522c-8cbf-9f221bbc5903"
OLIVIER_TRONCHN  = "169a272b-2931-5aa0-a245-cd495ad4891b"
PH_LAROSE        = "4184f9cc-a307-5b20-8676-670e751d7654"
BENJ_ROCHON      = "c312c02d-8a8c-5c73-8b68-65fc9e3fa453"
ERIC_COTE        = "68829443-e056-5536-a236-83479656d2cc"
SEB_VERDY        = "c0ba05a1-16a9-51e0-bcf5-7f6b879710a9"
EMMAN_BOUCHARD   = "3ef63282-9e9c-5f57-94c5-e1b5c4fe8c3c"
HUGUES_VEILLETTE = "480189b0-4ac3-5085-bcc4-f381cd90e39f"
SEB_DESGENS      = "2a875256-44b3-5eec-b0f5-185e5ca69b9f"
GABRIEL_GAUD     = "079f9c62-8932-55ad-8ae9-204ed637793f"
ANTONIN_LECLAIR  = "5983d5d0-01dc-5383-a6a3-ba0dfbf01b75"
MARILOU_OUIMET   = "000a2598-ea7a-5f0b-8298-049bbc38efa4"
ADAM_PIROG       = "5a74ce40-c264-5136-b408-4ca3be3a99f1"
CAROLINE_BOURG   = "13f2e206-6318-590d-b2a3-e9b90fc94c3c"
MARC_A_VEIL      = "0f1fdcb8-a37c-5328-a3f6-695533a75683"
FRANC_PERREAULT  = "ec487576-891e-5f91-8652-420c0930a1d1"
LUCE_GAUDETTE    = "f9bd712d-f282-5cff-95df-869ee1ff5759"
YACINE_MERZOUK   = "97d60d0e-3503-5814-8b1d-3fe6bb6bff86"
KAVIN_THIFFAULT  = "5d187fce-0ade-59a6-8ec3-e2ebae0c259c"
SEB_DUCHESNE     = "6dd904b7-11c6-5d62-af34-dad61040e67e"
SAMUEL_JOBIN     = "c57afdbe-f73a-5740-a2d3-0ec71764ee18"
DANNY_CARDONNE   = "d34a5fa9-fcb9-5031-9e74-8a2434f9cae8"
MARK_E_LEEMAN    = "8072eba9-8af3-5d7d-b783-7632b8c41dc3"
DIEGO_LA_MANNA   = "748a4598-18c4-58e0-b5d8-b5595798b16e"
# Unresolved — stage2 UUID used in piped team key
JPH_ROCHEFORT_S2 = "6583dfa3-f2fb-5eaf-9f76-0f9fb3c2a007"

# 991285803 additional persons
DAVID_BUTCHER    = "c1f6d696-5444-5cd0-919f-9dd06ca54bea"
ROB_ADAMS        = "20d639b1-087f-5227-9eaa-6dd342d57439"
JOHN_LEYS        = "3b938feb-b4c7-59a1-929f-7b62be77c1ce"
PAT_ASSWAD       = "202607a4-85da-5231-957a-85eb5a4e3e76"
JON_HAYDUK       = "ceb19bce-3ebd-5c57-bebb-aa545ba79aa6"
JOHN_HAYDUK      = "5d4624fd-09c1-5936-ab08-91567df09453"
ANDY_RONALD      = "39bc6c51-d2e0-5930-8677-51828c12de14"
ALEX_BELANGER_2  = "96b72fe6-ed2f-5d70-8a49-2e2d38b31e5f"   # same as above
BENOIT_GUILLEM   = "19d5198c-021c-5296-b078-565588c467db"
THEO_FRITSCH     = "738cbf71-ad21-598f-a5b3-afdb8bdf543d"
MATHIEU_VAIL     = "10a39ec4-94c9-5c7b-8f48-7cc8d43ea954"
KATIA_DIGNARD    = "92fc06e1-f673-59f8-8752-416bc935a234"
PH_LESSARD       = "87216aed-3048-50f7-8c54-d7e9e7bb52f3"
NATASHA_MERCURE  = "c3d1ffd9-7617-5853-be2a-095e239a0256"
MAUDE_LANDREV    = "f2ce846c-fa31-52e4-a88e-d8f7bccbe92e"
ALEXIS_DESCH     = "4f763ef4-fb7b-5fcb-9883-988370e20b2e"
PAT_KEEHAN       = "a885c64b-88c5-5b90-a17e-598a5076fcd5"


# ── Event 972427576 replacement rows ──────────────────────────────────────────

def _p(pl, pid, canon, div, div_cat, raw, cf, unres=""):
    return player(EVENT_A, 2001, pl, pid, canon, div, div_cat, raw, cf, unres)

def _t(pl, key, display, div, div_cat, raw, cf):
    return team(EVENT_A, 2001, pl, key, display, div, div_cat, raw, cf)


# Intermediate Singles Net — 4/5 resolved (Jean-Philippe Rochefort unresolved)
ISN = "Intermediate Singles Net"
ISN_CF = "mostly_complete"
ROWS_A = [
    _p(1, TIM_MELNYK,    "Tim Melnyk",             ISN, "net", ISN, ISN_CF),
    _p(2, "",            "Jean-Philippe Rochefort", ISN, "net", ISN, ISN_CF, "1"),
    _p(3, JOEL_DION,     "Joel Dion",               ISN, "net", ISN, ISN_CF),
    _p(3, FABIEN_EGGENA, "Fabien Eggena",           ISN, "net", ISN, ISN_CF),
    _p(5, ALEX_BELANGER, "Alexandre Bélanger",      ISN, "net", ISN, ISN_CF),
]

# Intermediate Doubles Net — 1 team, both resolved
IDN = "Intermediate Doubles Net"
IDN_RAW = "Intermediate doubles net"
IDN_CF = "complete"
ROWS_A += [
    _t(1, f"{JOEL_DION}|{TIM_MELNYK}", "Joel Dion / Tim Melnyk", IDN, "net", IDN_RAW, IDN_CF),
]

# Open Singles Net — 14 players, all resolved (Jean-Francois Lemieux corrected from "Jean")
OSN = "Open Singles Net"
OSN_CF = "mostly_complete"   # match existing flag
ROWS_A += [
    _p(1,  YVES_ARCH,      "Yves Archambault",     OSN, "net", OSN, OSN_CF),
    _p(2,  MARTIN_COTE,    "Martin Cote",           OSN, "net", OSN, OSN_CF),
    _p(3,  CHRIS_SEIBERT,  "Chris Seibert",         OSN, "net", OSN, OSN_CF),
    _p(4,  MARTIN_GRATON,  "Martin Graton",         OSN, "net", OSN, OSN_CF),
    _p(5,  JF_LEMIEUX,     "Jean-Francois Lemieux", OSN, "net", OSN, OSN_CF),
    _p(6,  ROBERT_LAVIGNE, "Robert Lavigne",        OSN, "net", OSN, OSN_CF),
    _p(7,  RENAUD_FANONI,  "Renaud Fanoni",         OSN, "net", OSN, OSN_CF),
    _p(7,  MARIO_VAILLANT, "Mario Vaillancourt",    OSN, "net", OSN, OSN_CF),
    _p(9,  STEPH_COMEAU,   "Stephane Comeau",       OSN, "net", OSN, OSN_CF),
    _p(10, MARILYN_DEMUY,  "Marilyn Demuy",         OSN, "net", OSN, OSN_CF),
    _p(11, OLIVIER_TRONCHN,"Olivier Tronchon",      OSN, "net", OSN, OSN_CF),
    _p(12, PH_LAROSE,      "Philippe Larose",       OSN, "net", OSN, OSN_CF),
    _p(13, BENJ_ROCHON,    "Benjamin Rochon",       OSN, "net", OSN, OSN_CF),
    _p(13, ERIC_COTE,      "Éric Côté",             OSN, "net", OSN, OSN_CF),
]

# Open Doubles Net — 8 teams, 15/16 persons resolved (Jean-Philippe Rochefort unresolved)
ODN = "Open Doubles Net"
ODN_CF = "mostly_complete"
ROWS_A += [
    _t(1, f"{SEB_VERDY}|{EMMAN_BOUCHARD}",
       "Sebastien Verdy / Emmanuel Bouchard", ODN, "net", ODN, ODN_CF),
    _t(2, f"{YVES_ARCH}|{MARIO_VAILLANT}",
       "Yves Archambault / Mario Vaillancourt", ODN, "net", ODN, ODN_CF),
    _t(3, f"{CHRIS_SEIBERT}|{ROBERT_LAVIGNE}",
       "Chris Seibert / Robert Lavigne", ODN, "net", ODN, ODN_CF),
    _t(4, f"{MARTIN_COTE}|{MARTIN_GRATON}",
       "Martin Cote / Martin Graton", ODN, "net", ODN, ODN_CF),
    _t(5, f"{JF_LEMIEUX}|{BENJ_ROCHON}",
       "Jean-Francois Lemieux / Benjamin Rochon", ODN, "net", ODN, ODN_CF),
    _t(6, f"{MARILYN_DEMUY}|{RENAUD_FANONI}",
       "Marilyn Demuy / Renaud Fanoni", ODN, "net", ODN, ODN_CF),
    _t(7, f"{PH_LAROSE}|{STEPH_COMEAU}",
       "Philippe Larose / Stephane Comeau", ODN, "net", ODN, ODN_CF),
    _t(7, f"{JPH_ROCHEFORT_S2}|{OLIVIER_TRONCHN}",
       "Jean-Philippe Rochefort / Olivier Tronchon", ODN, "net", ODN, ODN_CF),
]

# Routines Intermediate Freestyle — 11/12 resolved (Simon Kolodenchuk unresolved)
RIF     = "Routines Intermediate Freestyle"
RIF_RAW = "Routines Intermediate freestyle"
RIF_CF  = "mostly_complete"
ROWS_A += [
    _p(1,  HUGUES_VEILLETTE, "Hugues Oli Veillette",  RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(2,  SEB_DESGENS,      "Sébastien Desgens",     RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(2,  "",               "Simon Kolodenchuk",     RIF, "freestyle", RIF_RAW, RIF_CF, "1"),
    _p(4,  GABRIEL_GAUD,     "Gabriel Gaudette",      RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(5,  JOEL_DION,        "Joel Dion",             RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(6,  ANTONIN_LECLAIR,  "Antonin Leclair",       RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(7,  MARILOU_OUIMET,   "Marilou Ouimet",        RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(8,  ADAM_PIROG,       "Adam Pirog",            RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(9,  CAROLINE_BOURG,   "Caroline Bourgoin",     RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(10, MARC_A_VEIL,      "Marc-André Veillette",  RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(11, FRANC_PERREAULT,  "François Perreault",    RIF, "freestyle", RIF_RAW, RIF_CF),
    _p(12, LUCE_GAUDETTE,    "Luce Gaudette",         RIF, "freestyle", RIF_RAW, RIF_CF),
]

# Routines Open Freestyle — 5/5 resolved
ROF     = "Routines Open Freestyle"
ROF_RAW = "Routines Open freestyle"
ROF_CF  = "complete"
ROWS_A += [
    _p(1, YACINE_MERZOUK,  "Yacine Merzouk",      ROF, "freestyle", ROF_RAW, ROF_CF),
    _p(2, KAVIN_THIFFAULT, "Kavin Thiffault",      ROF, "freestyle", ROF_RAW, ROF_CF),
    _p(3, SEB_DUCHESNE,    "Sebastien Duchesne",   ROF, "freestyle", ROF_RAW, ROF_CF),
    _p(4, SAMUEL_JOBIN,    "Samuel Jobin",         ROF, "freestyle", ROF_RAW, ROF_CF),
    _p(5, DANNY_CARDONNE,  "Danny Cardonne",       ROF, "freestyle", ROF_RAW, ROF_CF),
]

# Shred Contest — 15/15 resolved (Hugues Veillette corrected from city-artifact)
SHR     = "Shred Contest"
SHR_CF  = "complete"
ROWS_A += [
    _p(1,  YACINE_MERZOUK,  "Yacine Merzouk",        SHR, "freestyle", SHR, SHR_CF),
    _p(2,  SEB_DUCHESNE,    "Sebastien Duchesne",    SHR, "freestyle", SHR, SHR_CF),
    _p(3,  KAVIN_THIFFAULT, "Kavin Thiffault",       SHR, "freestyle", SHR, SHR_CF),
    _p(4,  SAMUEL_JOBIN,    "Samuel Jobin",          SHR, "freestyle", SHR, SHR_CF),
    _p(5,  GABRIEL_GAUD,    "Gabriel Gaudette",      SHR, "freestyle", SHR, SHR_CF),
    _p(6,  MARK_E_LEEMAN,   "Mark Edward Leeman",    SHR, "freestyle", SHR, SHR_CF),
    _p(7,  DANNY_CARDONNE,  "Danny Cardonne",        SHR, "freestyle", SHR, SHR_CF),
    _p(8,  MARC_A_VEIL,     "Marc-André Veillette",  SHR, "freestyle", SHR, SHR_CF),
    _p(9,  SEB_DESGENS,     "Sébastien Desgens",     SHR, "freestyle", SHR, SHR_CF),
    _p(10, DIEGO_LA_MANNA,  "Diego La Manna",        SHR, "freestyle", SHR, SHR_CF),
    _p(11, HUGUES_VEILLETTE,"Hugues Oli Veillette",  SHR, "freestyle", SHR, SHR_CF),
    _p(12, ANTONIN_LECLAIR, "Antonin Leclair",       SHR, "freestyle", SHR, SHR_CF),
    _p(13, LUCE_GAUDETTE,   "Luce Gaudette",         SHR, "freestyle", SHR, SHR_CF),
    _p(14, ADAM_PIROG,      "Adam Pirog",            SHR, "freestyle", SHR, SHR_CF),
    _p(15, FRANC_PERREAULT, "François Perreault",    SHR, "freestyle", SHR, SHR_CF),
]

# Phat Trick Contest — Gabriel Gaudette (preserving existing norm with trick name)
PTC     = "Phat Trick Contest Winner"
ROWS_A += [
    {
        "event_id": EVENT_A, "year": "2001",
        "division_canon": PTC, "division_category": "freestyle",
        "place": "1", "competitor_type": "player",
        "person_id": GABRIEL_GAUD, "team_person_key": "",
        "person_canon": "Gabriel Gaudette", "team_display_name": "",
        "coverage_flag": "complete", "person_unresolved": "",
        "norm": "gabriel gaudette with pixie ducking butterfly",
        "division_raw": PTC,
    },
]

# Phat Combo Contest — Yacine Merzouk (was missing from PBP)
PCC     = "Phat Combo Contest"
ROWS_A += [
    _p(1, YACINE_MERZOUK, "Yacine Merzouk", PCC, "freestyle", PCC, "complete"),
]


# ── Event 991285803 replacement rows ──────────────────────────────────────────

def _p2(pl, pid, canon, div, div_cat, raw, cf, unres=""):
    return player(EVENT_B, 2001, pl, pid, canon, div, div_cat, raw, cf, unres)

def _t2(pl, key, display, div, div_cat, raw, cf):
    return team(EVENT_B, 2001, pl, key, display, div, div_cat, raw, cf)


# Open Singles — 20/21 resolved (Jean-Philippe Rochefort unresolved)
OS     = "Open Singles"
OS_CF  = "mostly_complete"
ROWS_B = [
    _p2(1,  EMMAN_BOUCHARD,  "Emmanuel Bouchard",     OS, "net", OS, OS_CF),
    _p2(2,  YVES_ARCH,       "Yves Archambault",      OS, "net", OS, OS_CF),
    _p2(3,  MARTIN_GRATON,   "Martin Graton",         OS, "net", OS, OS_CF),
    _p2(4,  DAVID_BUTCHER,   "David Butcher",         OS, "net", OS, OS_CF),
    _p2(5,  ROB_ADAMS,       "Rob Adams",             OS, "net", OS, OS_CF),
    _p2(6,  JOHN_LEYS,       "John Leys",             OS, "net", OS, OS_CF),
    _p2(7,  JF_LEMIEUX,      "Jean-Francois Lemieux", OS, "net", OS, OS_CF),
    _p2(8,  PAT_ASSWAD,      "Patrick Asswad",        OS, "net", OS, OS_CF),
    _p2(9,  JON_HAYDUK,      "Jon Hayduk",            OS, "net", OS, OS_CF),
    _p2(10, STEPH_COMEAU,    "Stephane Comeau",       OS, "net", OS, OS_CF),
    _p2(11, MARILYN_DEMUY,   "Marilyn Demuy",         OS, "net", OS, OS_CF),
    _p2(12, CHRIS_SEIBERT,   "Chris Seibert",         OS, "net", OS, OS_CF),
    _p2(13, BENJ_ROCHON,     "Benjamin Rochon",       OS, "net", OS, OS_CF),
    _p2(13, PAT_KEEHAN,      "Patrick Keehan",        OS, "net", OS, OS_CF),
    _p2(15, RENAUD_FANONI,   "Renaud Fanoni",         OS, "net", OS, OS_CF),
    _p2(16, PH_LESSARD,      "Philippe Lessard",      OS, "net", OS, OS_CF),
    _p2(17, PH_LAROSE,       "Philippe Larose",       OS, "net", OS, OS_CF),
    _p2(18, ERIC_COTE,       "Éric Côté",             OS, "net", OS, OS_CF),
    _p2(19, "",              "Jean-Philippe Rochefort", OS, "net", OS, OS_CF, "1"),
    _p2(19, ALEX_BELANGER,   "Alexandre Bélanger",    OS, "net", OS, OS_CF),
    _p2(19, NATASHA_MERCURE, "Natasha Mercure",       OS, "net", OS, OS_CF),
]

# Intermediate Singles — new division, both resolved
IS_DIV  = "Intermediate Singles"
IS_CF   = "complete"
ROWS_B += [
    _p2(1, BENOIT_GUILLEM, "Benoit Guillemette", IS_DIV, "net", IS_DIV, IS_CF),
    _p2(2, THEO_FRITSCH,   "Theodore Fritsch",   IS_DIV, "net", IS_DIV, IS_CF),
]

# Novice Singles — 2/3 resolved (Rémi ? unresolved)
NS_DIV = "Novice Singles"
NS_CF  = "complete"   # all 3 source placements captured; Rémi is unresolved identity
ROWS_B += [
    _p2(1, MATHIEU_VAIL, "Mathieu Vaillancourt", NS_DIV, "net", NS_DIV, NS_CF),
    _p2(2, "",           "Rémi ?",               NS_DIV, "net", NS_DIV, NS_CF, "1"),
    _p2(3, KATIA_DIGNARD,"Katia Dignard",         NS_DIV, "net", NS_DIV, NS_CF),
]

# Open Doubles — 14 teams, all persons resolved
OD     = "Open Doubles"
OD_RAW = "Open doubles"
OD_CF  = "complete"
ROWS_B += [
    _t2(1,  f"{EMMAN_BOUCHARD}|{PAT_ASSWAD}",
        "Emmanuel Bouchard / Patrick Asswad",       OD, "net", OD_RAW, OD_CF),
    _t2(2,  f"{YVES_ARCH}|{ALEXIS_DESCH}",
        "Yves Archambault / Alexis Deschenes",       OD, "net", OD_RAW, OD_CF),
    _t2(3,  f"{JOHN_HAYDUK}|{ANDY_RONALD}",
        "John Hayduk / Andy Ronald",                 OD, "net", OD_RAW, OD_CF),
    _t2(4,  f"{MARTIN_COTE}|{MARTIN_GRATON}",
        "Martin Cote / Martin Graton",               OD, "net", OD_RAW, OD_CF),
    _t2(5,  f"{DAVID_BUTCHER}|{JOHN_LEYS}",
        "David Butcher / John Leys",                 OD, "net", OD_RAW, OD_CF),
    _t2(6,  f"{JF_LEMIEUX}|{BENJ_ROCHON}",
        "Jean-Francois Lemieux / Benjamin Rochon",   OD, "net", OD_RAW, OD_CF),
    _t2(7,  f"{ROBERT_LAVIGNE}|{ROB_ADAMS}",
        "Robert Lavigne / Rob Adams",                OD, "net", OD_RAW, OD_CF),
    _t2(8,  f"{PAT_KEEHAN}|{CHRIS_SEIBERT}",
        "Patrick Keehan / Chris Seibert",            OD, "net", OD_RAW, OD_CF),
    _t2(9,  f"{RENAUD_FANONI}|{PH_LAROSE}",
        "Renaud Fanoni / Philippe Larose",           OD, "net", OD_RAW, OD_CF),
    _t2(10, f"{MARILYN_DEMUY}|{MAUDE_LANDREV}",
        "Marilyn Demuy / Maude Landreville",         OD, "net", OD_RAW, OD_CF),
    _t2(11, f"{BENOIT_GUILLEM}|{THEO_FRITSCH}",
        "Benoit Guillemette / Theodore Fritsch",     OD, "net", OD_RAW, OD_CF),
    _t2(12, f"{PH_LESSARD}|{ALEX_BELANGER}",
        "Philippe Lessard / Alexandre Bélanger",     OD, "net", OD_RAW, OD_CF),
    _t2(13, f"{STEPH_COMEAU}|{NATASHA_MERCURE}",
        "Stephane Comeau / Natasha Mercure",         OD, "net", OD_RAW, OD_CF),
    _t2(14, f"{KATIA_DIGNARD}|{MATHIEU_VAIL}",
        "Katia Dignard / Mathieu Vaillancourt",      OD, "net", OD_RAW, OD_CF),
]


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Reading {PBP_IN} …")
    with open(PBP_IN, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  Loaded {len(rows):,} rows")

    removed_a = [r for r in rows if r["event_id"] == EVENT_A]
    removed_b = [r for r in rows if r["event_id"] == EVENT_B]
    kept      = [r for r in rows if r["event_id"] not in {EVENT_A, EVENT_B}]

    print(f"\nEvent {EVENT_A}: removing {len(removed_a)} rows")
    print(f"Event {EVENT_B}: removing {len(removed_b)} rows")

    all_rows = kept + ROWS_A + ROWS_B

    print(f"\nEvent {EVENT_A}: inserting {len(ROWS_A)} rows")
    for r in ROWS_A:
        unres = " [unresolved]" if r.get("person_unresolved") else ""
        print(f"  {r['division_canon']:40s}  p{r['place']}  {r['person_canon']}{unres}")

    print(f"\nEvent {EVENT_B}: inserting {len(ROWS_B)} rows")
    for r in ROWS_B:
        unres = " [unresolved]" if r.get("person_unresolved") else ""
        print(f"  {r['division_canon']:40s}  p{r['place']}  {r['person_canon']}{unres}")

    print(f"\nWriting {PBP_OUT} …")
    with open(PBP_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PBP_FIELDS)
        w.writeheader()
        for r in all_rows:
            w.writerow({fld: r.get(fld, "") for fld in PBP_FIELDS})

    delta = len(all_rows) - len(rows)
    print(f"\n  v76 total: {len(all_rows):,} rows  (delta: {delta:+d})")
    print(f"  Expected delta: -40  (972427576: {len(ROWS_A) - len(removed_a):+d},  "
          f"991285803: {len(ROWS_B) - len(removed_b):+d})")
    print("\nDone.")


if __name__ == "__main__":
    main()
