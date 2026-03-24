#!/usr/bin/env python3
"""
patch_pbp_v77_to_v78.py  —  Bucket A safe deterministic fixes from QC review

Removes 62 rows that are confirmed parsing artifacts in four events.
No player data is created or modified; only spurious rows are removed.
All removals are corroborated by a clean row at the same placement
(or are unambiguous non-person tokens).

Fixes applied:
  941418343  WORLD FOOTBAG CHAMPIONSHIPS (1999)
    • 21 spurious team rows in Open Golf / Womens Footbag Golf /
      Open Speed Consecutives (city-in-location parsed as doubles partner;
      Golf and Consecutives are singles disciplines — no team rows valid)
    • 4 spurious team rows in Womens Singles Net p5 and p7
      (same "(City / State -COUNTRY)" artifact; clean solo player rows remain)

  1001076315  5th Annual Finnish Footbag Championships
    • 26 spurious team rows in Intermediate Freestyle (p1-7, p9),
      Intermediate Singles Net, Pro Freestyle, Pro Singles Net
      (source format "Name / Finnish City" parsed as doubles;
      clean solo player row present at each placement)
    • p8 of Intermediate Freestyle excluded (Henry/Heikki Rautio
      name mismatch — quarantined for source review)

  955745735  4th Annual Kansas Footbag Open
    • 6 spurious team rows in Singles Footbag Net p1-3 and
      Novice Singles 5 Min. Timed Consecutives p1-3
      (source format "Name / City KS" in singles divisions;
      clean player row present at each placement)
    • 1 non-person player row: "Wichita Ks" at Novice Doubles p1
      (city artifact alongside valid team row; no alternate person reading)

  1235653935  11th Annual IFPA European Footbag Championships
    • 4 player rows with "?" as delimiter in Open Doubles Net p3, p4, p8, p9
      (encoding artifact: "/" rendered as "?"; correct piped team row
      already present at each placement)

Not touched:
  • Truncated player names in 941418343 Golf/Consecutives (no clean backup)
  • Intermediate Freestyle p8 in 1001076315 (name mismatch, quarantine)
  • Any 1035277529 rows (expected to clear on pipeline regeneration)
  • All Bucket B quarantine items
"""

import csv

INPUT  = "inputs/identity_lock/Placements_ByPerson_v77.csv"
OUTPUT = "inputs/identity_lock/Placements_ByPerson_v78.csv"


def build_removal_set(pbp):
    """Return set of row indices to remove."""
    to_remove = set()

    for i, r in enumerate(pbp):
        eid   = r["event_id"]
        ctype = r["competitor_type"]
        div   = r["division_canon"]
        place = r["place"]
        canon = r["person_canon"]

        # ── 941418343 ──────────────────────────────────────────────────────
        if eid == "941418343" and ctype == "team":
            # Golf / Consecutives: singles disciplines, all team rows are artifacts
            if div in ("Open Golf", "Womens Footbag Golf", "Open Speed Consecutives"):
                to_remove.add(i)
            # Womens Singles Net p5 and p7: city-format artifact team rows
            if div == "Womens Singles Net" and place in ("5", "7"):
                to_remove.add(i)

        # ── 1001076315 ──────────────────────────────────────────────────────
        if eid == "1001076315" and ctype == "team":
            FINN_SINGLES = {
                "Intermediate Freestyle",
                "Intermediate Singles Net",
                "Pro Freestyle",
                "Pro Singles Net",
            }
            if div in FINN_SINGLES:
                # Exclude Intermediate Freestyle p8 (name mismatch quarantine)
                if div == "Intermediate Freestyle" and place == "8":
                    continue
                to_remove.add(i)

        # ── 955745735 ──────────────────────────────────────────────────────
        if eid == "955745735":
            KANSAS_SINGLES = {
                "Singles Footbag Net",
                "Novice Singles 5 Min. Timed Consecutives",
            }
            # Spurious team rows in singles divisions
            if ctype == "team" and div in KANSAS_SINGLES:
                to_remove.add(i)
            # Non-person city artifact in Novice Doubles
            if ctype == "player" and canon == "Wichita Ks":
                to_remove.add(i)

        # ── 1235653935 ──────────────────────────────────────────────────────
        if eid == "1235653935":
            # Player rows with "?" delimiter in Open Doubles Net
            if ctype == "player" and div == "Open Doubles Net" and "?" in canon:
                to_remove.add(i)

    return to_remove


def main():
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        pbp = list(reader)

    print(f"Input rows: {len(pbp)}")

    to_remove = build_removal_set(pbp)

    # Sanity: log every removal
    by_event = {}
    for i in sorted(to_remove):
        r = pbp[i]
        eid = r["event_id"]
        by_event.setdefault(eid, []).append(r)

    for eid, rows in sorted(by_event.items()):
        print(f"\n  {eid} — {rows[0].get('event_name', '')} ({len(rows)} rows):")
        for r in rows:
            print(f"    [{r['competitor_type']}] {r['division_canon']} p{r['place']}"
                  f"  canon={r['person_canon']!r}"
                  f"  disp={r['team_display_name']!r}")

    output_rows = [r for i, r in enumerate(pbp) if i not in to_remove]

    print(f"\nRemoved: {len(to_remove)}")
    print(f"Output rows: {len(output_rows)}")
    print(f"  = {len(pbp)} - {len(to_remove)} = {len(pbp) - len(to_remove)} expected")
    assert len(output_rows) == len(pbp) - len(to_remove)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
