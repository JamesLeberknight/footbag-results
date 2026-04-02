"""
patch_pbp_v87_to_v88.py

Targeted patch for Polish Championships cluster — 3 events:
  1376036018 — 13th Polish Footbag Championships 2013 (Wrocław)
  1406219099 — 14th Polish Footbag Championships 2014 (Lubaczów)
  1485863923 — 17th Polish Footbag Championships 2017 (Lublin)

Fix type: A only — add missing person_id to player rows where person_canon is
already the correct PT canonical form. No name changes, no heuristics.

Skipped rows (not included in this patch):
  1376036018 / Open Singles Net  / p=1  / 'Wiktor D\ufffdbski.'     — FFFD + trailing period
  1376036018 / Request Contest   / p=4  / 'Kszysztof Sob\ufffftka'  — FFFD + typo
  1406219099 / Open Sick Trick   / p=3  / 'Maciek Niczyporuk (Poland)' — no PT exact match
  1406219099 / Open Singles Routines / p=7 / 'Filip Wójcik (Poland)'  — no PT exact match
"""

import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v87.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v88.csv"

# ---------------------------------------------------------------------------
# Person-id additions per event
# Each entry: (event_id, division_canon, place, person_canon, person_id)
# ---------------------------------------------------------------------------
ADDITIONS = [
    # -----------------------------------------------------------------------
    # Event 1376036018 — 13th Polish Championships 2013
    # -----------------------------------------------------------------------
    (1376036018, "Circle Contest",          3,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Circle Contest",          5,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Circle Contest",          5,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1376036018, "Circle Contest",          9,  "Dawid Michalowicz",  "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1376036018, "Circle Contest",          9,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Circle Contest",          9,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1376036018, "Footbag Golf",            1,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1376036018, "Intermediate Singles Net",1,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Intermediate Singles Net",2,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1376036018, "Intermediate Singles Net",5,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Intermediate Singles Net",7,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Open Singles Net",        6,  "Marcin Staron",      "21154756-b8d4-512e-872a-c5b206fac8d3"),
    (1376036018, "Open Singles Net",        8,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1376036018, "Open Singles Routines",   2,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Open Singles Routines",   5,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Open Singles Routines",   6,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Request Contest",         4,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Request Contest",         4,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Shred30",                 4,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Shred30",                 5,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Shred30",                 7,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1376036018, "Shred30",                 9,  "Dawid Michalowicz",  "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1376036018, "Shred30",                11,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Sick 3",                  1,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Sick 3",                  1,  "Szymon Kalwak",      "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1376036018, "Sick 3",                  2,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Sick 3",                  3,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1376036018, "Sick 3",                  4,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Sick 3",                  4,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1376036018, "Sick 3",                  5,  "Dawid Michalowicz",  "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1376036018, "Sick 3",                  5,  "Filip Wojcik",       "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1376036018, "Sick 3",                  5,  "Krzysztof Sobótka",  "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1376036018, "Sick 3",                  6,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1376036018, "Sick 3",                  6,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1376036018, "Sick 3",                  9,  "Dawid Michalowicz",  "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1376036018, "Sick 3",                  9,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1376036018, "Sick 3",                  9,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1376036018, "Sick 3",                 11,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),

    # -----------------------------------------------------------------------
    # Event 1406219099 — 14th Polish Championships 2014
    # -----------------------------------------------------------------------
    (1406219099, "Open 2-Square",           1,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1406219099, "Open 2-Square",           2,  "Michal Ostrowski",   "da3fbe76-ba2d-51ee-a019-97e7ba93089a"),
    (1406219099, "Open 2-Square",           3,  "Evan Gatesman",      "be7a8e7f-f052-5c31-b27b-47d4fee9a265"),
    (1406219099, "Open 2-Square",           4,  "Damian Budzik",      "a3781ac2-c529-5316-b22b-2bdcfa67bb67"),
    (1406219099, "Open Circle Contest",     1,  "Evan Gatesman",      "be7a8e7f-f052-5c31-b27b-47d4fee9a265"),
    (1406219099, "Open Circle Contest",     4,  "Mariusz Wilk",       "4ad82639-801d-5c4b-aae6-f07e5dfe67d2"),
    (1406219099, "Open Rippin' Run",        1,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1406219099, "Open Rippin' Run",        2,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1406219099, "Open Rippin' Run",        3,  "Mariusz Wilk",       "4ad82639-801d-5c4b-aae6-f07e5dfe67d2"),
    (1406219099, "Open Sick Trick",         1,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1406219099, "Open Sick Trick",         2,  "Rados Turek",        "2002626c-155f-5447-8115-bba010c6a418"),
    (1406219099, "Open Sick Trick",         4,  "Jakub Worek",        "0da84e00-c9f5-5d23-82b6-9ec1e759c74c"),
    (1406219099, "Open Sick Trick",         5,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1406219099, "Open Sick Trick",         6,  "Evan Gatesman",      "be7a8e7f-f052-5c31-b27b-47d4fee9a265"),
    (1406219099, "Open Singles Net",        1,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1406219099, "Open Singles Net",        2,  "Wojciech Jamski",    "2fe08509-fcb3-5ecd-8828-40411bc4a036"),
    (1406219099, "Open Singles Net",        3,  "Jakub Grabarczyk",   "3568ce18-bf1b-55b6-aefd-61f65f55b08c"),
    (1406219099, "Open Singles Routines",   1,  "Marcin Bujko",       "4f0fcdbc-8dfe-51fc-b92c-b3cda4dd0c00"),
    (1406219099, "Open Singles Routines",   2,  "Evan Gatesman",      "be7a8e7f-f052-5c31-b27b-47d4fee9a265"),
    (1406219099, "Open Singles Routines",   3,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1406219099, "Open Singles Routines",   4,  "Arkadiusz Stanek",   "8859edda-82f6-5013-97d0-1057cb8bdd3b"),
    (1406219099, "Open Singles Routines",   5,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1406219099, "Open Singles Routines",   6,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1406219099, "Open Singles Routines",   8,  "Pawel Nowak",        "9fccb3a3-3408-518a-aacb-43d9678fcce4"),
    (1406219099, "Women's Singles Routines",1,  "Caroline Birch",     "a6500854-2f53-5a6f-b63f-a3c072838ebd"),
    (1406219099, "Women's Singles Routines",2,  "Dorota Wojtasiuk",   "7a8c8b83-9ae8-5b65-8f1f-6923058cc237"),

    # -----------------------------------------------------------------------
    # Event 1485863923 — 17th Polish Championships 2017
    # -----------------------------------------------------------------------
    (1485863923, "2 Square",                1,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "2 Square",                2,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1485863923, "2 Square",                4,  "Marcin Staron",      "21154756-b8d4-512e-872a-c5b206fac8d3"),
    (1485863923, "2 Square",                7,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "2 Square",                7,  "Lukasz Domin",       "d65e2ac9-c615-5f25-b58f-14493a27af47"),
    (1485863923, "Circle Contest",          1,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Circle Contest",          5,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Footbag Golf",            3,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Footbag Golf",            5,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1485863923, "Footbag Golf",            7,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Footbag Golf",            9,  "Lukasz Domin",       "d65e2ac9-c615-5f25-b58f-14493a27af47"),
    (1485863923, "Open Battle",             3,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Open Battle",             4,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Open Singles Net",        3,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1485863923, "Open Singles Net",        6,  "Michal Rog",         "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1485863923, "Open Singles Net",        8,  "Marcin Staron",      "21154756-b8d4-512e-872a-c5b206fac8d3"),
    (1485863923, "Open Singles Net",       11,  "Lukasz Domin",       "d65e2ac9-c615-5f25-b58f-14493a27af47"),
    (1485863923, "Open Singles Routines",   1,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Open Singles Routines",   6,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Open Singles Routines",   9,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1485863923, "Request Contest",         3,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Request Contest",         5,  "Wiktor Debski",      "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1485863923, "Request Contest",         7,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Shred 30",                1,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1485863923, "Sick 3",                  1,  "Rafal Kaleta",       "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1485863923, "Sick 3",                  2,  "Jakub Mosciszewski", "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
]


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    changes = 0
    per_event = {}

    for eid, div, place, canon, pid in ADDITIONS:
        m = (
            (df["event_id"] == eid) &
            (df["division_canon"] == div) &
            (df["place"] == place) &
            (df["person_canon"] == canon) &
            (df["person_id"].isna() | (df["person_id"] == ""))
        )
        n = m.sum()
        if n == 1:
            df.loc[m, "person_id"] = pid
            changes += 1
            per_event[eid] = per_event.get(eid, 0) + 1
        elif n == 0:
            # Check if already patched or genuinely missing
            m_any = (
                (df["event_id"] == eid) &
                (df["division_canon"] == div) &
                (df["place"] == place) &
                (df["person_canon"] == canon)
            )
            if m_any.sum() == 0:
                print(f"  WARNING: row not found — eid={eid} div={div!r} p={place} canon={canon!r}")
            else:
                print(f"  NOTE: already has person_id — eid={eid} div={div!r} p={place} canon={canon!r}")
        else:
            print(f"  WARNING: {n} rows matched — eid={eid} div={div!r} p={place} canon={canon!r}")

    for eid in [1376036018, 1406219099, 1485863923]:
        print(f"  Event {eid}: {per_event.get(eid, 0)} person_ids added")

    print(f"\nTotal changes: {changes} rows across 3 events")
    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
