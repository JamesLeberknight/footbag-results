#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from pathlib import Path


def load_alias_map(path: Path) -> dict[str, str]:
    """
    Returns dict[player_id] -> alias_group_id
    Read-only join. No guessing.
    """
    m: dict[str, str] = {}
    if not path.exists():
        return m

    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pid = (row.get("player_id") or "").strip()
            ag = (row.get("alias_group_id") or "").strip()
            if pid and ag:
                m[pid] = ag
    return m


def main() -> None:
    in_csv = Path("out/Placements_Flat.csv")
    alias_csv = Path("out/person_alias_map_bootstrap.csv")
    out_csv = Path("out/Placements_Flat_aliased.csv")

    if not in_csv.exists():
        raise SystemExit(f"Missing input: {in_csv}")
    if not alias_csv.exists():
        raise SystemExit(f"Missing alias map: {alias_csv}")

    alias_map = load_alias_map(alias_csv)

    with in_csv.open("r", encoding="utf-8", newline="") as f_in, \
         out_csv.open("w", encoding="utf-8", newline="") as f_out:

        r = csv.DictReader(f_in)
        fieldnames = list(r.fieldnames or [])

        # Add new columns (non-breaking; appended)
        new_cols = ["player1_alias_group_id", "player2_alias_group_id"]
        for c in new_cols:
            if c not in fieldnames:
                fieldnames.append(c)

        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()

        n = 0
        n1 = 0
        n2 = 0

        for row in r:
            p1 = (row.get("player1_id") or "").strip()
            p2 = (row.get("player2_id") or "").strip()

            a1 = alias_map.get(p1, "") if p1 else ""
            a2 = alias_map.get(p2, "") if p2 else ""

            row["player1_alias_group_id"] = a1
            row["player2_alias_group_id"] = a2

            if a1:
                n1 += 1
            if a2:
                n2 += 1

            w.writerow(row)
            n += 1

    print(f"Wrote: {out_csv} ({n} rows)")
    print(f"Rows with player1 alias_group_id: {n1}")
    print(f"Rows with player2 alias_group_id: {n2}")
    print(f"Alias map entries loaded: {len(alias_map)}")


if __name__ == "__main__":
    main()
