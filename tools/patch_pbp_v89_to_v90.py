"""
patch_pbp_v89_to_v90.py

Recovery Batch 5 — Todexon / French Open / Swiss Championships cluster.

Events patched:
  1231432184 — Todexon 10, 2009, Prague         (22 rows)
  1262691385 — Todexon 11, 2010, Prague         ( 9 rows)
  1321869570 — Todexon 13, 2012, Prague         (12 rows)
  1354752474 — 13e Open de France, 2013, Paris  (18 rows)
  1329132977 — Swiss Championships, 2012, Zurich(16 rows)

Fix type: A only — add missing person_id where person_canon already matches PT
exactly. No name changes, no heuristics.

SKIPPED rows (deferred — documented per framework):
  'Hanna Mickiewicz (Poland)'      × 4  — near-match: PT has 'Hannia Mickiewicz'
  'Jind Smola (Czech republic)'    × 2  — abbreviation; co-present with 'Jindrich Smola' at same place
  'Bartek Bubula (Poland)'         × 1  — near-match: PT has 'Bartosz Bubula'
  'Krystof Malér (Czech Republic)' × 2  — no PT entry
  'Marcin Staroñ (Poland)'         × 1  — non-FFFD encoding artifact + paren suffix; novel combo
  'Tomasz Ostrowski (Poland)'      × 1  — ambiguous: verify vs 'Tomas Ostrowski' / 'Michal Ostrowski'
  'Renato Zülli (Switzerland)'     × 1  — near-match: PT canonical is 'Renatto Zülli' (double-t)
  'Maciek Niczyporuk'              × 1  — no PT entry
  'Maciek Niczyporuk (Poland)'     × 1  — no PT entry
  'Jean'                           × 2  — mononym, unresolvable
  'Dexter'                         × 1  — mononym, unresolvable
  'Christoph Larndorfer (Austria)' × 1  — no PT entry
  'Karel Hák (Czech Republic)'     × 1  — no PT entry
  'Flavio Lötscher (Switzerland)'  × 1  — no PT entry
  'Julien Apollonio (Switzerland)' × 1  — no PT entry
  'Medat Osmanoski (Switzerland)'  × 1  — no PT entry
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v89.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v90.csv"

# (event_id, division_canon, place, person_canon, person_id)
ADDITIONS = [
    # -----------------------------------------------------------------------
    # Event 1231432184 — Todexon 10, 2009 (22 rows)
    # -----------------------------------------------------------------------
    (1231432184, "Open Singles Routines",        1,  "Vaclav Klouda",       "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    (1231432184, "Open Singles Routines",        2,  "Milan Benda",         "0b3204c8-7e99-586e-aeb2-4186d0901fe3"),
    (1231432184, "Open Singles Routines",        3,  "Marcin Bujko",        "4f0fcdbc-8dfe-51fc-b92c-b3cda4dd0c00"),
    (1231432184, "Open Singles Routines",        4,  "Jorden Moir",         "16aae952-d265-5e4d-81b8-5474f6c43802"),
    (1231432184, "Open Singles Routines",        5,  "Damian Gielnicki",    "ac1268dc-a961-568f-860e-63e9ea815c01"),
    (1231432184, "Open Singles Routines",        7,  "Szymon Kalwak",       "4ae1304e-960b-554f-8ce7-c59e0d7607b2"),
    (1231432184, "Open Singles Routines",        8,  "Rafal Kaleta",        "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1231432184, "Open Singles Routines",        9,  "Dan Ednie",           "cc144a17-3ed0-5c64-9305-e9a45fa00290"),
    (1231432184, "Open Singles Routines",        9,  "Olaf Piwowar",        "70f7b4f9-e8e3-5a08-b540-0924c68a4b20"),
    (1231432184, "Open Singles Routines",        9,  "Tina Aeberli",        "6c84f5b5-7da4-5b72-bdd7-453421a3c3d8"),
    (1231432184, "Open Singles Routines",        9,  "Wiktor Debski",       "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    (1231432184, "Open Singles Routines",       13,  "Jan Struz",           "28ed65ae-ea5e-5e04-bff5-97b8f71e6696"),
    (1231432184, "Open Singles Routines",       13,  "Jindrich Smola",      "0c1d57ac-f8a6-5c3e-a66f-eac82c8a5df4"),
    (1231432184, "Open Singles Routines",       13,  "Michal Ostrowski",    "da3fbe76-ba2d-51ee-a019-97e7ba93089a"),
    (1231432184, "Open Singles Routines",       17,  "Alexander Trenner",   "24ffb4bb-a58e-54d8-b0d9-06b989c794cd"),
    (1231432184, "Open Singles Routines",       17,  "Tomas Solc",          "a9a940f8-ea34-598b-a8a7-b64548c7c5c9"),
    (1231432184, "Open Singles Routines",       21,  "Arek Hutnik",         "6640fdb5-ce0a-551f-ac7e-b5673e17e9a5"),
    (1231432184, "Open Singles Routines",       21,  "Pawel Cisek",         "603bbc2d-6488-5891-a942-b5c5c79e0018"),
    (1231432184, "Open Singles Routines",       21,  "Slava Sidorin",       "2862a91d-b120-5c46-a85d-1eb5efb6520e"),
    (1231432184, "Open Singles Routines",       24,  "Markus Hemmer",       "c0488adb-3715-53d6-ab5f-263f143604da"),
    (1231432184, "Women's Singles Routines",     1,  "Tina Aeberli",        "6c84f5b5-7da4-5b72-bdd7-453421a3c3d8"),
    (1231432184, "Women's Singles Routines",     2,  "Jana Riisalo",        "f91f76bc-1b7b-5965-a815-3d18ddc87759"),

    # -----------------------------------------------------------------------
    # Event 1262691385 — Todexon 11, 2010 (9 rows)
    # -----------------------------------------------------------------------
    (1262691385, "Open Singles Routines",        1,  "Milan Benda",         "0b3204c8-7e99-586e-aeb2-4186d0901fe3"),
    (1262691385, "Open Singles Routines",        2,  "Arkadiusz Dudzinski", "6513b145-8a4a-556a-a04f-981d95db2d70"),
    (1262691385, "Open Singles Routines",        3,  "Vaclav Klouda",       "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    (1262691385, "Open Singles Routines",        4,  "Marcin Bujko",        "4f0fcdbc-8dfe-51fc-b92c-b3cda4dd0c00"),
    (1262691385, "Open Singles Routines",        6,  "Patrik Cerny",        "700e0ec0-94b9-5de3-b9a9-8d40a3645de3"),
    (1262691385, "Open Singles Routines",        7,  "Alexander Trenner",   "24ffb4bb-a58e-54d8-b0d9-06b989c794cd"),
    (1262691385, "Open Singles Routines",        8,  "Rafal Kaleta",        "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1262691385, "Open Singles Routines",        9,  "Michal Ostrowski",    "da3fbe76-ba2d-51ee-a019-97e7ba93089a"),
    (1262691385, "Women's Singles Routines",     2,  "Malgorzata Ostrowska","617abb97-3969-5024-8c37-22923d84318d"),

    # -----------------------------------------------------------------------
    # Event 1321869570 — Todexon 13, 2012 (12 rows)
    # -----------------------------------------------------------------------
    (1321869570, "Open Singles Routines",        1,  "Milan Benda",         "0b3204c8-7e99-586e-aeb2-4186d0901fe3"),
    (1321869570, "Open Singles Routines",        2,  "Jan Weber",           "1e09ba09-ec4b-5961-8b95-e0d638dc4f5c"),
    (1321869570, "Open Singles Routines",        3,  "Vaclav Klouda",       "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    (1321869570, "Open Singles Routines",        4,  "Jindrich Smola",      "0c1d57ac-f8a6-5c3e-a66f-eac82c8a5df4"),
    (1321869570, "Open Singles Routines",        5,  "Pavel Motorov",       "9d12fe41-0154-57a6-b312-5f3b46f621b5"),
    (1321869570, "Open Singles Routines",        6,  "Rafal Kaleta",        "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1321869570, "Open Singles Routines",        8,  "Tuomas Riisalo",      "171e6321-c025-5483-af9d-c92b2ee04806"),
    (1321869570, "Open Singles Routines",        9,  "Rene Ruehr",          "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    (1321869570, "Open Singles Routines",       13,  "Andreas Nawrath",     "4beb2ce2-19e8-5443-9b5b-e221febea787"),
    (1321869570, "Open Singles Routines",       14,  "Jan Struz",           "28ed65ae-ea5e-5e04-bff5-97b8f71e6696"),
    (1321869570, "Women's Singles Routines",     1,  "Jana Riisalo",        "f91f76bc-1b7b-5965-a815-3d18ddc87759"),
    (1321869570, "Women's Singles Routines",     2,  "Paloma Mayo",         "26d70b1f-bca9-579f-b9fa-0c95c3dde86a"),

    # -----------------------------------------------------------------------
    # Event 1354752474 — 13e Open de France, 2013 (18 rows)
    # -----------------------------------------------------------------------
    (1354752474, "Open Circle Contest",          4,  "Aleksi Airinen",      "7c74c3f5-dc54-5e47-8365-e84497f4c943"),
    (1354752474, "Open Shred:30",                1,  "Aleksi Airinen",      "7c74c3f5-dc54-5e47-8365-e84497f4c943"),
    (1354752474, "Open Shred:30",                4,  "Jakob Wagner",        "c90a00eb-95d7-5375-a769-b71cbce34d87"),
    (1354752474, "Open Shred:30",                6,  "Jindrich Smola",      "0c1d57ac-f8a6-5c3e-a66f-eac82c8a5df4"),
    (1354752474, "Open Shred:30",                7,  "Rene Ruehr",          "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    (1354752474, "Open Shred:30",                8,  "Pawel Nowak",         "9fccb3a3-3408-518a-aacb-43d9678fcce4"),
    (1354752474, "Open Shred:30",                9,  "Rados Turek",         "2002626c-155f-5447-8115-bba010c6a418"),
    (1354752474, "Open Singles Routines",        1,  "Aleksi Airinen",      "7c74c3f5-dc54-5e47-8365-e84497f4c943"),
    (1354752474, "Open Singles Routines",        2,  "Rene Ruehr",          "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    (1354752474, "Open Singles Routines",        3,  "Jindrich Smola",      "0c1d57ac-f8a6-5c3e-a66f-eac82c8a5df4"),
    (1354752474, "Open Singles Routines",        4,  "Alexander Trenner",   "24ffb4bb-a58e-54d8-b0d9-06b989c794cd"),
    (1354752474, "Open Singles Routines",        5,  "Jakob Wagner",        "c90a00eb-95d7-5375-a769-b71cbce34d87"),
    (1354752474, "Open Singles Routines",        6,  "Andreas Nawrath",     "4beb2ce2-19e8-5443-9b5b-e221febea787"),
    (1354752474, "Open Singles Routines",        7,  "Nis Petersen",        "a81adde6-197e-5067-872a-b05092e75cf9"),
    (1354752474, "Open Singles Routines",        9,  "Pawel Nowak",         "9fccb3a3-3408-518a-aacb-43d9678fcce4"),
    (1354752474, "Women's Singles Routines",     1,  "Tina Aeberli",        "6c84f5b5-7da4-5b72-bdd7-453421a3c3d8"),
    (1354752474, "Women's Singles Routines",     2,  "Paloma Mayo",         "26d70b1f-bca9-579f-b9fa-0c95c3dde86a"),
    (1354752474, "Women's Singles Routines",     3,  "Dorota Wojtasiuk",    "7a8c8b83-9ae8-5b65-8f1f-6923058cc237"),

    # -----------------------------------------------------------------------
    # Event 1329132977 — Swiss Championships, 2012 (16 rows)
    # -----------------------------------------------------------------------
    (1329132977, "Intermediate Singles Routines",1,  "Pablo Wey",           "cfe791ea-8b10-5d08-8cb4-2ee8eb1b1ce5"),
    (1329132977, "Intermediate Singles Routines",2,  "Flurin Bischoff",     "d25f994e-0337-5fc5-923d-8f1c88d4a44d"),
    (1329132977, "Open Circle Contest",          6,  "Pawel Rozek",         "0b3ffb10-2257-510e-a69a-26ff2fc87508"),
    (1329132977, "Open Singles Routines",        1,  "Milan Benda",         "0b3204c8-7e99-586e-aeb2-4186d0901fe3"),
    (1329132977, "Open Singles Routines",        2,  "Rene Ruehr",          "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    (1329132977, "Open Singles Routines",        3,  "Jindrich Smola",      "0c1d57ac-f8a6-5c3e-a66f-eac82c8a5df4"),
    (1329132977, "Open Singles Routines",        4,  "Rafal Kaleta",        "82c36f56-236f-4496-826b-cf59a6fcc4ed"),
    (1329132977, "Open Singles Routines",        8,  "Jan Struz",           "28ed65ae-ea5e-5e04-bff5-97b8f71e6696"),
    (1329132977, "Women's Circle Contest",       1,  "Tina Aeberli",        "6c84f5b5-7da4-5b72-bdd7-453421a3c3d8"),
    (1329132977, "Women's Circle Contest",       2,  "Paloma Mayo",         "26d70b1f-bca9-579f-b9fa-0c95c3dde86a"),
    (1329132977, "Women's Circle Contest",       4,  "Katharina Probst",    "dd033603-4714-5162-8de9-39f56f77e9e6"),
    (1329132977, "Women's Circle Contest",       5,  "Tabea Fetz",          "313958c0-32b7-53ee-a0da-817ec8d36753"),
    (1329132977, "Women's Singles Routines",     1,  "Tina Aeberli",        "6c84f5b5-7da4-5b72-bdd7-453421a3c3d8"),
    (1329132977, "Women's Singles Routines",     3,  "Paloma Mayo",         "26d70b1f-bca9-579f-b9fa-0c95c3dde86a"),
    (1329132977, "Women's Singles Routines",     4,  "Katharina Probst",    "dd033603-4714-5162-8de9-39f56f77e9e6"),
    (1329132977, "Women's Singles Routines",     5,  "Tabea Fetz",          "313958c0-32b7-53ee-a0da-817ec8d36753"),
]

EXPECTED_PER_EVENT = {
    1231432184: 22,
    1262691385: 9,
    1321869570: 12,
    1354752474: 18,
    1329132977: 16,
}


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    # Verify no target events have changes outside our additions
    target_events = set(EXPECTED_PER_EVENT.keys())
    changes = 0
    per_event = {eid: 0 for eid in target_events}

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

    print(f"\nTotal changes: {changes} rows across {len(target_events)} events")
    if not all_ok:
        print("  WARNING: one or more event counts did not match expected values")

    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
