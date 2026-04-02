"""
patch_pbp_v92_to_v93.py

Hotfix: remove OSR/Shred30 ghost entries from Women's Singles Net in event
1449259560 (2016 IFPA World Footbag Championships).

Root cause: 9 male players who competed in Open Singles Routines (or Shred30)
have duplicate rows in Women's Singles Net at the same place numbers as the
correct women's players. This produces participant_order > 1 in a singles
division — a structural contamination, not a legitimate tie.

Rows to REMOVE (division_canon="Women's Singles Net", event_id=1449259560):
  place 1  — Alexander Trenner    (OSR place 9)
  place 2  — Taishi Ishida        (OSR place 4)
  place 3  — Mariusz Wilk         (Shred30 place 3)
  place 4  — Evan Gatesman        (OSR place 1)
  place 5  — Jakub Mosciszewski   (OSR place 6)
  place 6  — Mikko Lepisto        (OSR place 9)
  place 7  — Patrik Cerny         (OSR place 3)
  place 8  — Krystof Maler        (OSR place 7)
  place 9  — Dominik Šimků        (OSR place 8)

After removal: 21 rows → 12 rows (places 1–12, one women's player each).
"""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v92.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v93.csv"

EVENT_ID = 1449259560
DIV      = "Women's Singles Net"

# (place, person_canon) — ghost rows to remove
GHOSTS = [
    (1,  "Alexander Trenner"),
    (2,  "Taishi Ishida"),
    (3,  "Mariusz Wilk"),
    (4,  "Evan Gatesman"),
    (5,  "Jakub Mosciszewski"),
    (6,  "Mikko Lepisto"),
    (7,  "Patrik Cerny"),
    (8,  "Krystof Maler"),
    (9,  "Dominik Šimků"),
]

EXPECTED_REMOVALS = len(GHOSTS)   # 9
EXPECTED_REMAINING = 12           # places 1–12, one women's player each


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    before = len(df)
    mask_event = df["event_id"] == EVENT_ID
    mask_div   = df["division_canon"] == DIV

    drop_indices = []
    not_found = []

    for place, canon in GHOSTS:
        m = mask_event & mask_div & (df["place"] == place) & (df["person_canon"] == canon)
        n = m.sum()
        if n == 1:
            drop_indices.extend(df[m].index.tolist())
            print(f"  REMOVE  place={place:<2}  {canon}")
        elif n == 0:
            print(f"  WARNING: not found — place={place} canon={canon!r}")
            not_found.append((place, canon))
        else:
            print(f"  WARNING: {n} rows matched — place={place} canon={canon!r}")

    df = df.drop(index=drop_indices).reset_index(drop=True)

    remaining_wsn = df[mask_event & mask_div]
    print()
    print(f"Rows removed:   {len(drop_indices)} / {EXPECTED_REMOVALS} expected")
    print(f"WSN rows after: {len(remaining_wsn)} (expected {EXPECTED_REMAINING})")
    print(f"Total rows:     {before} → {len(df)}")

    if not_found:
        print(f"\nWARNING: {len(not_found)} ghost rows not found — patch incomplete")

    if len(remaining_wsn) != EXPECTED_REMAINING:
        print(f"WARNING: remaining WSN row count {len(remaining_wsn)} != {EXPECTED_REMAINING}")

    print()
    print("Remaining Women's Singles Net rows:")
    print(remaining_wsn[["place", "person_canon", "person_id"]].sort_values("place").to_string())

    print(f"\nWriting {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
