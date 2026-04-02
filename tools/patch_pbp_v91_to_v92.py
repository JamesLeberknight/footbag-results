"""
patch_pbp_v91_to_v92.py

Recovery Batch 7 — Polish/UK/French cluster (6 events).

Events patched:
  1219405634 — Polish Footbag Open 2012              (11 rows)
  1336988107 — Polish Footbag Open 2014              (12 rows)
  1694170899 — Polish Footbag Open 2023              (12 rows)
  1061453080 — South African Footbag Open 2003        (9 rows)
  1519215398 — RNH / World Footbag 2018              ( 9 rows)
  1547415984 — Polish Footbag Open 2019              (10 rows)

Fix type: A only — add missing person_id where person_canon already matches PT
exactly. No name changes, no heuristics.

SKIPPED rows (deferred):
  1219405634 / Circle Contest      / p=?  / 'Bartosz Rząsa'        — no PT entry
  1219405634 / *                   / p=?  / 'Gadziński Marcin'     — no PT entry
  1219405634 / *                   / p=?  / 'Cisek Paweł'          — no PT entry
  1219405634 / *                   / p=?  / 'Piotr Bałtrukiewicz'  — no PT entry
  1219405634 / *                   / p=?  / 'Olędzka Gosia'        — no PT entry
  1336988107 / *                   / p=?  / 'Małgorzata Dębska'    — no PT entry
  1336988107 / *                   / p=?  / 'Paula Brzezińska'     — no PT entry
  1336988107 / *                   / p=?  / 'Radosław Turek'       — no PT exact match
  1336988107 / *                   / p=?  / 'Małgorzata Olędzka'   — no PT entry
  1694170899 / *                   / p=?  / 'Konrad'               — mononym
  1694170899 / *                   / p=?  / 'Mikołaj'              — mononym
  1694170899 / *                   / p=?  / 'Michał'               — mononym
  1694170899 / *                   / p=?  / 'Kamil'                — mononym
  1519215398 / Open Singles Net    / p=?  / 'Sébastien Maillet'    — FFFD artifact (Type B, deferred)
  1519215398 / *                   / p=?  / 'Barthélemy Meridjen'  — FFFD artifact (Type B, deferred)
  1519215398 / *                   / p=?  / 'Gregor Morel'         — no PT entry
  1547415984 / *                   / p=?  / 'Przemysław Pietrzycki'— FFFD artifact (Type B, deferred)
  1547415984 / *                   / p=?  / 'Piia Tantarimäki'     — FFFD artifact (Type B, deferred)
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v91.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v92.csv"

# (event_id, division_canon, place, person_canon, person_id)
ADDITIONS = [
    # -----------------------------------------------------------------------
    # Event 1219405634 — Polish Footbag Open 2012 (11 rows)
    # -----------------------------------------------------------------------
    (1219405634, "Circle Contest",           1,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1219405634, "Circle Contest",           5,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1219405634, "Circle Contest",           9,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1219405634, "Circle Contest",           9,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1219405634, "Circle Contest",          12,  "Pawel Fraczek",       "9a45d2ec-cf90-5686-9e97-65d0b06918ae"),
    (1219405634, "Open Freestyle Routines",  8,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1219405634, "Open Freestyle Routines", 10,  "Pawel Fraczek",       "9a45d2ec-cf90-5686-9e97-65d0b06918ae"),
    (1219405634, "Open Freestyle Routines", 16,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1219405634, "Open Golf",                2,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1219405634, "Shred30",                  7,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1219405634, "Sick3",                    3,  "Pawel Fraczek",       "9a45d2ec-cf90-5686-9e97-65d0b06918ae"),

    # -----------------------------------------------------------------------
    # Event 1336988107 — Polish Footbag Open 2014 (12 rows)
    # -----------------------------------------------------------------------
    (1336988107, "2 Square",                 2,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1336988107, "Golf",                     5,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1336988107, "Golf",                    12,  "Dawid Michalowicz",   "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1336988107, "Intermediate Singles Net", 1,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1336988107, "Open Freestyle Routines",  7,  "Dawid Michalowicz",   "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1336988107, "Open Freestyle Routines",  8,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1336988107, "Shred 30",                 4,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1336988107, "Shred 30",                 6,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1336988107, "Shred 30",                 9,  "Dawid Michalowicz",   "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),
    (1336988107, "Shred 30",                12,  "Pawel Scierski",      "ac0d2fdd-9032-5edc-9023-e8d611dfc8c6"),
    (1336988107, "Sick 3",                   2,  "Krzysztof Sobótka",   "ddf2f704-a4d2-5653-a82a-45987d37645c"),
    (1336988107, "Sick 3",                   4,  "Dawid Michalowicz",   "5d240aa8-3e6f-5a7c-8ab6-5630620f6f6d"),

    # -----------------------------------------------------------------------
    # Event 1694170899 — Polish Footbag Open 2023 (12 rows)
    # -----------------------------------------------------------------------
    (1694170899, "Battles",                  5,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Footbag Golf",             1,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1694170899, "Freestyle Overall",        5,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Last Man Standing",        3,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Request Contest",          5,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Sick3",                    1,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1694170899, "Sick3",                    2,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Sick3",                    5,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Sick3",                    8,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1694170899, "Sick3",                   10,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1694170899, "Singles Net",              4,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1694170899, "Singles Net",              7,  "Lukasz Domin",        "d65e2ac9-c615-5f25-b58f-14493a27af47"),

    # -----------------------------------------------------------------------
    # Event 1061453080 — South African Footbag Open 2003 (9 rows)
    # -----------------------------------------------------------------------
    (1061453080, "Intermediate Shred 30",    1,  "Ian Pritchard",       "b39aea2d-6370-5381-88ec-ffc5a9d3f9de"),
    (1061453080, "Intermediate Shred 30",    2,  "Mark Roberts",        "a45f9d77-e16f-524c-99b4-d75cb23648db"),
    (1061453080, "Intermediate Shred 30",    3,  "Scott K",             "d3c8ebb4-8d08-5684-8f07-11c843e6c4f1"),
    (1061453080, "Intermediate Sick 3",      2,  "Dyalan Govender",     "1739ca69-640a-52e9-8266-1a96cc39ca49"),
    (1061453080, "Intermediate Sick 3",      3,  "Ian Pritchard",       "b39aea2d-6370-5381-88ec-ffc5a9d3f9de"),
    (1061453080, "Open Sick 3",              1,  "Lynton Stephens",     "8a740aac-66c5-5fe2-a482-2acd1d1cdc11"),
    (1061453080, "Open Sick 3",              2,  "Dan Ednie",           "cc144a17-3ed0-5c64-9305-e9a45fa00290"),
    (1061453080, "Open Sick 3",              3,  "Jeremy O'Wheel",      "5ee61cde-2c01-5c24-9f87-e4f7554a35c3"),
    (1061453080, "Open Sick Trick",          1,  "Brendan Erskine",     "2b72150a-23a0-5c22-864d-86e32a01f185"),

    # -----------------------------------------------------------------------
    # Event 1519215398 — RNH / World Footbag 2018 (9 rows)
    # -----------------------------------------------------------------------
    (1519215398, "Intermediate Singles Net", 2,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1519215398, "Open Circle",              5,  "Matthias Lino Schmidt","f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    (1519215398, "Open Circle",              9,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1519215398, "Open Freestyle Battles",   9,  "Matthias Lino Schmidt","f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    (1519215398, "Open Singles Net",        10,  "Luka Weyler",         "deb1a724-c5cd-5173-bd6e-71d8f1d585bf"),
    (1519215398, "Open Singles Net",        25,  "Marcin Staron",       "21154756-b8d4-512e-872a-c5b206fac8d3"),
    (1519215398, "Open Singles Net",        26,  "Robin Puchel",        "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    (1519215398, "Request 10%",              6,  "Vaclav Klouda",       "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    (1519215398, "Request 10%",              9,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),

    # -----------------------------------------------------------------------
    # Event 1547415984 — Polish Footbag Open 2019 (10 rows)
    # -----------------------------------------------------------------------
    (1547415984, "2 Square",                 1,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1547415984, "Battle Contest",           5,  "Rafal Kaleta",        "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1547415984, "Battle Contest",           9,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1547415984, "Battle Contest",           9,  "Matthias Lino Schmidt","f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    (1547415984, "Battle Contest",          17,  "Jakub Mosciszewski",  "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1547415984, "Footbag Golf",             2,  "Michal Rog",          "392ef8ff-d969-5917-8bc5-176805ecb787"),
    (1547415984, "Open Singles Net",         4,  "Tuomas Karki",        "e7937047-c7f1-5739-bb84-aceb23c17dd7"),
    (1547415984, "Shred 30",                 4,  "Jakub Mosciszewski",  "7a8314b4-2af9-584d-bd79-4af7a74d9c21"),
    (1547415984, "Shred 30",                 5,  "Filip Wojcik",        "6d948954-90cb-5446-9a60-9d4ca0ef4734"),
    (1547415984, "Shred 30",                 9,  "Matthias Lino Schmidt","f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
]

EXPECTED_PER_EVENT = {
    1219405634: 11,
    1336988107: 12,
    1694170899: 12,
    1061453080:  9,
    1519215398:  9,
    1547415984: 10,
}


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    changes = 0
    per_event = {eid: 0 for eid in EXPECTED_PER_EVENT}

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
            per_event[eid] += 1
        elif n == 0:
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

    print()
    all_ok = True
    for eid, expected in EXPECTED_PER_EVENT.items():
        actual = per_event[eid]
        status = "OK" if actual == expected else f"MISMATCH (expected {expected})"
        print(f"  Event {eid}: {actual} rows patched  [{status}]")
        if actual != expected:
            all_ok = False

    print(f"\nTotal changes: {changes} rows across {len(EXPECTED_PER_EVENT)} events")
    if not all_ok:
        print("  WARNING: one or more event counts did not match expected values")

    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
