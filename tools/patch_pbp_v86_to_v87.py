"""
patch_pbp_v86_to_v87.py

Targeted patch for event 1293877677 (2011 32nd IFPA World Footbag Championships).

Fixes in this patch:
  A. 21 player rows: add missing person_id (person_canon already correct)
  B. 11 player rows: fix FFFD encoding artifacts + strip country suffix + add person_id
  C. Strip remaining U+FFFD from 'norm' column (cosmetic only)
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v86.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v87.csv"

EVENT_ID = 1293877677

# ---------------------------------------------------------------------------
# A. Person-id additions — clean name, just needs person_id linked
#    List of (division_canon, place, person_canon, person_id)
# ---------------------------------------------------------------------------
PERSON_ID_ADDITIONS = [
    # Intermediate divisions
    ("Intermediate Request Contest",   4,  "Robin Puchel",          "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    ("Intermediate Shred:30",          2,  "Robin Puchel",          "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    ("Intermediate Singles Routines",  3,  "Robin Puchel",          "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    # Open Circle Contest
    ("Open Circle Contest",            9,  "Rene Ruehr",            "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    ("Open Circle Contest",           16,  "Matthias Lino Schmidt", "f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    ("Open Circle Contest",           16,  "Mikko Lepisto",         "8e2c07e2-aeb8-5325-be31-ffcc452944ec"),
    # Open Request Contest
    ("Open Request Contest",           2,  "Vaclav Klouda",         "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    ("Open Request Contest",          20,  "Matthias Lino Schmidt", "f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    ("Open Request Contest",          20,  "Rene Ruehr",            "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    ("Open Request Contest",          30,  "Sebastien Duchesne",    "6dd904b7-11c6-5d62-af34-dad61040e67e"),
    # Open Shred:30
    ("Open Shred:30",                  5,  "Rene Ruehr",            "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    ("Open Shred:30",                 19,  "Mikko Lepisto",         "8e2c07e2-aeb8-5325-be31-ffcc452944ec"),
    # Open Singles Net
    ("Open Singles Net",               1,  "Tuomas Karki",          "e7937047-c7f1-5739-bb84-aceb23c17dd7"),
    ("Open Singles Net",               2,  "Francois Pelletier",    "9d9b7019-1c24-54b4-8dfb-75bc689657e2"),
    ("Open Singles Net",              46,  "Robin Puchel",          "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    ("Open Singles Net",              52,  "Roman Belozerov",       "37844c75-3ef3-5567-a289-9b6bd07fdfe4"),
    # Open Singles Routines
    ("Open Singles Routines",          5,  "Vaclav Klouda",         "98ce4e04-05ca-56ca-b8c6-83f51c164c89"),
    ("Open Singles Routines",          8,  "Mikko Lepisto",         "8e2c07e2-aeb8-5325-be31-ffcc452944ec"),
    ("Open Singles Routines",         13,  "Rene Ruehr",            "9585ffd8-cc96-528e-ba4a-4211da8cef00"),
    ("Open Singles Routines",         21,  "Matthias Lino Schmidt", "f52e0fe8-4eff-5532-9af4-14124f1e92ee"),
    # Women's Singles Net
    ("Women's Singles Net",            1,  "Geneviève Bousquet",    "fea99a91-ae13-5cb1-b87f-3c352783dc2e"),
]

# ---------------------------------------------------------------------------
# B. FFFD + country-suffix fixes
#    Key = original (corrupt) person_canon in PBP
#    Value = (clean person_canon matching PT, person_id)
#
#    These strings appear in multiple divisions; match event-wide.
# ---------------------------------------------------------------------------
PERSON_CANON_FIXES = {
    # "Toni Pääkkönen FIN" — 3 FFFD chars (ää, ö)
    "Toni P\uFFFD\uFFFDkk\uFFFDnen FIN":   ("Toni Pä\u00e4kkönen",         "98945320-03c6-5cb1-8dbd-886ce1019bb9"),
    # "Oskari Forstém FIN" — FFFD replacing é
    "Oskari Forst\uFFFDm FIN":             ("Oskari Forst\u00e9n",          "111079c7-b085-530e-b4da-68e9b975803e"),
    # "Florian Götze GER" — FFFD replacing ö
    "Florian G\uFFFDtze GER":              ("Florian Goetze",               "ab966624-6643-5dea-b165-67e66af402f9"),
    # "Sébastian Duchesne CAN" — FFFD replacing é
    "S\uFFFDbastian Duchesne CAN":         ("Sebastien Duchesne",           "6dd904b7-11c6-5d62-af34-dad61040e67e"),
    # "Sébastian Maillet FRA" — FFFD replacing é
    "S\uFFFDbastian Maillet FRA":          ("S\u00e9bastien Maillet",       "a9518594-35d9-53cd-8e8d-a7bc4de94236"),
    # "Carlos Márquez VEN" — FFFD replacing á
    "Carlos M\uFFFDrquez VEN":             ("Carlos Marquez",               "dfb408b6-d225-5b9f-b1c7-3f9a76c4319a"),
    # "Chris Löw GER" — FFFD replacing ö → PT canonical is "Chris Loew"
    "Chris L\uFFFDw GER":                  ("Chris Loew",                   "77077fa3-e070-5728-87d5-88c81b58466a"),
    # "Renato Zülli SUI" — FFFD replacing ü → PT canonical is "Renatto Zülli"
    "Renato Z\uFFFDlli SUI":               ("Renatto Z\u00fclli",           "73961edb-d45f-5638-9401-1a0de345da6f"),
    # "Piia Tantarimäki FIN" — FFFD replacing ä
    "Piia Tantarim\uFFFDki FIN":           ("Piia Tantarim\u00e4ki",        "1dc7bf3f-8a4d-57b0-912b-b6626fb29cfb"),
}


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    mask = df["event_id"] == EVENT_ID
    n_event = mask.sum()
    print(f"Event {EVENT_ID}: {n_event} rows")

    changes = 0

    # -----------------------------------------------------------------------
    # A. Person-id additions (clean names)
    # -----------------------------------------------------------------------
    for div, place, canon, pid in PERSON_ID_ADDITIONS:
        m = mask & (df["division_canon"] == div) & (df["place"] == place) & (df["person_canon"] == canon)
        n = m.sum()
        if n == 1:
            df.loc[m, "person_id"] = pid
            changes += 1
        elif n == 0:
            print(f"  A. WARNING: not found — div={div!r} p={place} canon={canon!r}")
        else:
            print(f"  A. WARNING: {n} rows matched — div={div!r} p={place} canon={canon!r} (expected 1)")

    print(f"  A. person_id additions: {changes} rows")

    # -----------------------------------------------------------------------
    # B. FFFD / country-suffix fixes
    # -----------------------------------------------------------------------
    b_changes = 0
    for orig_canon, (clean_canon, pid) in PERSON_CANON_FIXES.items():
        m = mask & (df["person_canon"] == orig_canon)
        n = m.sum()
        if n:
            df.loc[m, "person_canon"] = clean_canon
            df.loc[m, "person_id"]    = pid
            print(f"  B. {repr(orig_canon)} → {repr(clean_canon)}: {n} row(s)")
            b_changes += n
        else:
            print(f"  B. WARNING: not found — {repr(orig_canon)}")

    changes += b_changes
    print(f"  B. FFFD/suffix fixes: {b_changes} rows")

    # -----------------------------------------------------------------------
    # C. Strip remaining U+FFFD from 'norm' column (cosmetic only)
    # -----------------------------------------------------------------------
    norm_fffd = mask & df["norm"].astype(str).str.contains("\ufffd", na=False)
    n = norm_fffd.sum()
    if n:
        df.loc[norm_fffd, "norm"] = (
            df.loc[norm_fffd, "norm"].astype(str).str.replace("\ufffd", "", regex=False)
        )
        print(f"  C. norm FFFD stripped: {n} rows")
        changes += n

    # -----------------------------------------------------------------------
    # Verify: no remaining FFFD in player rows for this event
    # -----------------------------------------------------------------------
    player_rows = df[mask & (df["person_canon"] != "__NON_PERSON__")]
    still_fffd = player_rows.apply(
        lambda r: any("\ufffd" in str(v) for v in r), axis=1
    ).sum()
    if still_fffd:
        print(f"\n  WARNING: {still_fffd} player rows still contain U+FFFD in event {EVENT_ID}")
        problem = player_rows.apply(lambda r: any("\ufffd" in str(v) for v in r), axis=1)
        for _, row in player_rows[problem].iterrows():
            print(f"    div={row.division_canon} p={row.place} canon={repr(row.person_canon)}")
    else:
        print(f"\n  OK: no remaining U+FFFD in player rows for event {EVENT_ID}")

    print(f"\nTotal changes: {changes} fields across event {EVENT_ID}")
    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
