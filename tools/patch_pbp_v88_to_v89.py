"""
patch_pbp_v88_to_v89.py

Targeted patch for event 1568961264 (RNH Footbag 20th Anniversary, Paris, 2019).

Fix type: comma-inversion — Open Singles Net rows stored as "Last, First"
are rewritten to "First Last" canonical form and assigned their PT person_id.

All 19 rows have exact, unambiguous PT matches. No rows skipped.
p=14 (Belouin Ollivier) already has correct form and person_id — not touched.
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v88.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v89.csv"

EVENT_ID = 1568961264

# (place, raw_person_canon, clean_person_canon, person_id)
FIXES = [
    ( 1, "Maillet, Sébastien",   "Sébastien Maillet",   "a9518594-35d9-53cd-8e8d-a7bc4de94236"),
    ( 2, "Tellenbach, Grischa",  "Grischa Tellenbach",  "ec1d5118-cd4b-591f-a84a-04bbe20868bc"),
    ( 3, "Mlakar, Lena",         "Lena Mlakar",         "40624e68-19a3-53b1-a13a-e7dea012bb23"),
    ( 4, "Rusev, Radoslav",      "Radoslav Rusev",       "d2a69779-e080-590f-b59f-5af7ae6a2414"),
    ( 5, "Stanev, Ivan",         "Ivan Stanev",          "4d8b80e4-fb64-5532-84e8-d2f88decdc3a"),
    ( 6, "Kabbal, Mouss",        "Mouss Kabbal",         "897d9022-b9cd-5d56-b9c8-19d31f38b5a7"),
    ( 7, "Depoilly, Sven",       "Sven Depoilly",        "989e4bf9-b2ab-526c-bd1e-a09ebe6411aa"),
    ( 8, "Isackson, Quentin",    "Quentin Isackson",     "7d094f73-d2d9-5f99-9b73-6593696c90ef"),
    ( 9, "Rambaud, David",       "David Rambaud",        "9dd1729e-cd7f-5412-93ee-f08166e75fc7"),
    (10, "Arnaud, Anthony",      "Anthony Arnaud",       "07b2ad01-ccb8-5acf-ac37-d8a7261e5a73"),
    (11, "Janin, Nicolas",       "Nicolas Janin",        "4e6f43e2-e696-59ca-81f1-8f45729a2df4"),
    (12, "Tumelin, Audrey",      "Audrey Tumelin",       "ed4305db-f657-5e96-a434-aab2156c32b7"),
    (13, "Letort, Jean-Marie",   "Jean-Marie Letort",    "43ac12c4-b074-55ec-be51-6858775ac7c7"),
    (15, "Lancret, Xavier",      "Xavier Lancret",       "94f7127f-d01d-56c1-92bd-70e334487986"),
    (16, "Nguyen, Trung",        "Trung Nguyen",         "9e8e478b-e5e2-5f7a-95dc-5d14a949d174"),
    (17, "Martin, Baptiste",     "Baptiste Martin",      "0f854891-563d-53c2-9259-9a011004f8d4"),
    (18, "Bigey, Loic",          "Loic Bigey",           "f604fadc-5953-5d33-9bb2-628bff698444"),
    (19, "Galabrun, Jean",       "Jean Galabrun",        "c7bad462-0547-55a0-8b2b-cee382e67afe"),
    (20, "Jeliazkova, Tania",    "Tania Jeliazkova",     "f95453fc-b49b-55df-954e-7b9f22c63f3b"),
]


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    mask_event = df["event_id"] == EVENT_ID
    mask_div   = df["division_canon"] == "Open Singles Net"

    changes = 0
    for place, raw_canon, clean_canon, pid in FIXES:
        m = mask_event & mask_div & (df["place"] == place) & (df["person_canon"] == raw_canon)
        n = m.sum()
        if n == 1:
            df.loc[m, "person_canon"] = clean_canon
            df.loc[m, "person_id"]    = pid
            changes += 1
        elif n == 0:
            print(f"  WARNING: not found — p={place} canon={raw_canon!r}")
        else:
            print(f"  WARNING: {n} rows matched — p={place} canon={raw_canon!r}")

    print(f"  Rows patched: {changes} / {len(FIXES)}")

    # Verify: no remaining comma-form names in this event
    remaining = df[mask_event & df["person_canon"].str.contains(",", na=False)]
    if len(remaining):
        print(f"  WARNING: {len(remaining)} rows still have comma in person_canon")
        for _, r in remaining.iterrows():
            print(f"    div={r.division_canon!r} p={r.place} canon={repr(r.person_canon)}")
    else:
        print("  OK: no remaining comma-form names in event 1568961264")

    print(f"\nTotal changes: {changes} rows")
    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
