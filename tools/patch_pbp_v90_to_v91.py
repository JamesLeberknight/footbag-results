"""
patch_pbp_v90_to_v91.py

Recovery Batch 6 — RNH 2006 / US Open 2014 / East Coast 2014 / European 2004.

Events patched:
  1134914723 — RNH Contest 2006                        (16 rows)
  1386623061 — U.S. Open Footbag Championships 2014    (14 rows)
  1405875596 — 32nd Annual East Coast Footbag Champs   (10 rows)
  1079605499 — 6th Annual IFPA European Championships  (12 rows)

Fix type: A only — add missing person_id where person_canon already matches PT
exactly. No name changes, no heuristics.

SKIPPED rows (deferred):
  1134914723 / Mini Net          / p=1  / 'Franck Remy'          — no PT entry
  1134914723 / Open Singles Net  / p=10 / 'Mathieu'              — mononym
  1134914723 / Open Singles Net  / p=12 / 'Lisa Amengual'        — no PT exact match
  1134914723 / Singles Net       / p=10 / 'Mathieu'              — mononym
  1386623061 / Women's Singles Net / p=3 / 'Renee Sheets Johnson' — no PT entry
  1405875596 / Int. Circle Contest / p=6 / 'Daniel Carey'        — no PT entry
  1405875596 / Int. Circle Contest / p=6 / 'Vince Richardson'    — no PT entry
  1405875596 / Open Circle Contest / p=5 / 'Cassandra Taylor'    — no PT entry
  1405875596 / Open Circle Contest / p=5 / 'Nick Polini'         — no PT entry
  1079605499 / Open Golf         / p=7  / 'Ulisse'               — mononym
  1079605499 / Open Golf         / p=10 / 'Vinz'                 — mononym
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v90.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v91.csv"

# (event_id, division_canon, place, person_canon, person_id)
ADDITIONS = [
    # -----------------------------------------------------------------------
    # Event 1134914723 — RNH Contest 2006 (16 rows)
    # -----------------------------------------------------------------------
    (1134914723, "Open Shred 30",       1,  "Serge Kaldany",       "8983b558-9cb9-5a25-bb28-589435a8f34e"),
    (1134914723, "Open Shred 30",       2,  "Robinson Sustrac",    "71475c85-2292-5764-b481-1c24bd73bda6"),
    (1134914723, "Open Shred 30",       3,  "Fred Touzelet",       "414acac1-acb2-58f9-9c40-c0f8a8f2d70c"),
    (1134914723, "Open Shred 30",       4,  "Grischa Tellenbach",  "ec1d5118-cd4b-591f-a84a-04bbe20868bc"),
    (1134914723, "Open Shred 30",       5,  "Augustin Tiffaud",    "71501d92-2a94-5c78-9fe7-8cc7c58de04b"),
    (1134914723, "Open Shred 30",       6,  "Thomas Sustrac",      "a0bcdd4a-2d77-522f-8f5c-b74c30ade8d2"),
    (1134914723, "Open Singles Net",    4,  "Lino Landau",         "fbf52ca9-ba84-50bc-bce9-0244718c18e7"),
    (1134914723, "Open Singles Net",    5,  "Fabien Riffaud",      "9337337c-b3c1-53a6-9f38-ee3923ef6c13"),
    (1134914723, "Open Singles Net",    6,  "Eric Fonteneau",      "1645cd59-4ae0-5a7a-adaa-bead2c1d5a81"),
    (1134914723, "Open Singles Net",    7,  "Vincent Rousseau",    "b5c82731-ff37-5170-86a5-f744afe4d154"),
    (1134914723, "Open Singles Net",    8,  "Arnaud Saniez",       "698fd3e8-a756-5c8e-bfdc-127a68fb6a58"),
    (1134914723, "Open Singles Net",    9,  "Marco Brunet",        "c91a2084-4b3a-5614-8b34-a52171742e13"),
    (1134914723, "Open Singles Net",   11,  "Mouss Kabbal",        "897d9022-b9cd-5d56-b9c8-19d31f38b5a7"),
    (1134914723, "Open Singles Net",   13,  "Etienne Ruggeri",     "31fe03cd-4b0b-5eb3-b674-86bf4a5e876d"),
    (1134914723, "Open Singles Net",   14,  "Helena Schlichting",  "e023aa06-ace7-5617-8145-aaca829f4c54"),
    (1134914723, "Open Singles Net",   15,  "Thomas Sustrac",      "a0bcdd4a-2d77-522f-8f5c-b74c30ade8d2"),

    # -----------------------------------------------------------------------
    # Event 1386623061 — U.S. Open Footbag Championships 2014 (14 rows)
    # -----------------------------------------------------------------------
    (1386623061, "Intermediate Singles Net",  3,  "Drake Shults",        "85b19220-524f-553e-83f5-39286809df55"),
    (1386623061, "Intermediate Singles Net",  3,  "Gaylene Grossen",     "3975e864-a494-5e72-a6ca-0d0885e32e2b"),
    (1386623061, "Intermediate Singles Net",  5,  "Ben Johnson",         "251d6d5f-a1cf-5169-8602-637a9573958a"),
    (1386623061, "Open Sick 3",               1,  "Matt Kemmer",         "3e78df68-5425-5d48-a88a-3bf59fe2934f"),
    (1386623061, "Open Sick 3",               2,  "Chris Dean",          "43ddd1ca-e004-5d37-b4b5-38cd81bde3c2"),
    (1386623061, "Open Sick 3",               3,  "Brian Sherrill",      "11542335-67de-5fd2-9d43-5f2b55ea1fef"),
    (1386623061, "Open Sick 3",               4,  "Alex Dworetzky",      "337dc937-b022-51ef-8470-99fff306e2c9"),
    (1386623061, "Open Sick 3",               5,  "Derek Littlefield",   "720e2139-027a-5e14-ab81-e82e84c1c44b"),
    (1386623061, "Open Sick 3",               5,  "Dustin Rhodes",       "58bb3c4a-1a60-537d-ba4e-55de294abd8c"),
    (1386623061, "Open Singles Net",          4,  "Edwin Veltman",       "62bdd1f4-4414-5259-9996-91770a34d09f"),
    (1386623061, "Open Singles Net",          5,  "Jasper Shults",       "820cd94f-96f9-5bac-bc27-a2fa9f7332da"),
    (1386623061, "Open Singles Net",          6,  "Patrick Weinberg",    "d1afbdbf-5b4a-5ccf-bc8a-d5bea2c4ada2"),
    (1386623061, "Open Singles Net",          7,  "Dirk Janssens",       "a3036351-4d3f-504d-add4-4ae9fc629177"),
    (1386623061, "Women's Singles Net",       2,  "Leanne Makcrow",      "c9312d25-2097-5826-ba39-6de3a7ee7043"),

    # -----------------------------------------------------------------------
    # Event 1405875596 — 32nd Annual East Coast Footbag Championships (10 rows)
    # -----------------------------------------------------------------------
    (1405875596, "Intermediate Circle Contest", 5, "April Tou",          "97dc8652-bf81-51cc-8f79-d92647a989f7"),
    (1405875596, "Intermediate Circle Contest", 5, "Ryan Morris",        "d3721e3e-5868-5aa0-b0ac-ad62cec21842"),
    (1405875596, "Open Circle Contest",         5, "Anton Britting",     "5153f93f-1e75-5497-82c6-bf804faf6476"),
    (1405875596, "Open Circle Contest",         5, "Brian Sherrill",     "11542335-67de-5fd2-9d43-5f2b55ea1fef"),
    (1405875596, "Open Circle Contest",         5, "Derek Littlefield",  "720e2139-027a-5e14-ab81-e82e84c1c44b"),
    (1405875596, "Open Circle Contest",         5, "Kevin Hogan",        "018c9ed2-a70b-5f10-9b77-a36a479ee64f"),
    (1405875596, "Open Circle Contest",         5, "Mathieu Gauthier",   "ecd47dee-6803-5d73-b31c-dcb9860e767e"),
    (1405875596, "Open Circle Contest",         5, "Matt Kemmer",        "3e78df68-5425-5d48-a88a-3bf59fe2934f"),
    (1405875596, "Open Circle Contest",         5, "Mikey Etlinger",     "f7205d80-d739-51c3-906e-ec6d716c8fb7"),
    (1405875596, "Open Circle Contest",         5, "Ryan Thomas",        "8519ad8c-7103-5eee-97c4-a08a9895da57"),

    # -----------------------------------------------------------------------
    # Event 1079605499 — 6th Annual IFPA European Championships (12 rows)
    # -----------------------------------------------------------------------
    (1079605499, "Shred30",                              4,  "Linnanen Jere",   "83ba4a8c-d170-5c79-b698-e0fd6893a7c3"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 1, "Lenneis Verena",  "4ea3c32c-ba79-5e8b-8203-42efc78349ca"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 2, "Boehm Jule",      "968503ff-4e51-5a66-beb0-05f6ccff4587"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 3, "Mickiewicz Hanna","d9173a63-b38b-593a-b534-9b1a3291b6fe"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 4, "Thygesen Lise",   "e96d4d25-1446-570e-bfcc-277d318607e1"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 5, "Prikhosko Oxana", "beed13f8-2a6d-580b-beff-448a8a792a0e"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 6, "Busch Anne",      "6f089ad0-2735-5620-9e42-dfca71c1b652"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 7, "Locher Nora",     "bfa1c5a0-584c-5283-8c58-9003507522d7"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 8, "Mader Nina",      "17d8713a-1bac-5d57-a4d9-8f6bf0fd644c"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]", 9, "Nigisch Johanna", "7b325017-26ce-5082-86fb-b6aa0b0bafd7"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]",10, "Liukko Ninni",    "145d1998-d217-5dd8-a5b4-239c1c7c7c59"),
    (1079605499, "Womens Singles Freestyle [Very, Very Close (1-4)]",11, "Siewczyk Lila",   "876df740-2c9c-5b06-a3d7-d9ab06be3fc8"),
]

EXPECTED_PER_EVENT = {
    1134914723: 16,
    1386623061: 14,
    1405875596: 10,
    1079605499: 12,
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
            m_any = (df["event_id"] == eid) & (df["division_canon"] == div) & \
                    (df["place"] == place) & (df["person_canon"] == canon)
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
