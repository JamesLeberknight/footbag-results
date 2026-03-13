#!/usr/bin/env python3
"""
Patch PBP v63 → v64: targeted data quality fixes for 4 events.

Changes:
  979816633  Strip leading ") " from person_canon/norm for 4 doubles rows
  979089216  Strip "between " prefix from Logan Dethman row; fix team_display_name caps
  1195677906 Restore single-name tokens for Circle Competition (Jakob/Matthias/André)
             replacing __NON_PERSON__ with person_unresolved=1 entries
  1369141018 Remove "()" partner rows; strip " / ()" from team_display_name
  1727756195 Open Golf group2 renumbered 1-7 → 4-10; division_raw set to "Open Golf"
             for all 10 Open Golf rows (group1 was 1-3, group2 was also 1-7)
"""

import csv
import re
import sys
from pathlib import Path

IN_FILE  = Path("inputs/identity_lock/Placements_ByPerson_v63.csv")
OUT_FILE = Path("inputs/identity_lock/Placements_ByPerson_v64.csv")

# Open Golf fix: within event 1727756195, the 10 rows starting at the block where
# Michal Klimczak is place=1 form two groups that both started at place 1.
# Group1 (places 1-3): Klimczak, Pietrzycki, Niczyporuk — keep places, set division
# Group2 (was places 1-7): Czech, Mosciszewski, Rog, Nowak, Ignaczak, Domin, Worek
#   → renumber to 4-10 and set division_raw
# Identification: consecutive positional block within the event, starting with
# Klimczak at place=1, next 9 rows are the interleaved group1/group2.
# Verified file_rows in v63 input: the Golf block is event rows 17-26 (0-indexed).
GOLF_EID = "1727756195"
GOLF_DIV = "Open Golf"
# Map: (person_canon_in_group2, current_place_str) → new_place_str
GOLF_GROUP2_REMAP = {
    ("Marcin Czech",        "1"): "4",
    ("Kuba Mosciszewski",   "2"): "5",
    ("Michal Rog",          "3"): "6",
    ("Pawel Nowak",         "4"): "7",
    ("Wojtek Ignaczak",     "5"): "8",
    ("Lukasz Domin",        "6"): "9",
    ("Jakub Worek",         "7"): "10",
}


def patch(rows):
    out = []
    removed = 0
    changed = 0

    for r in rows:
        eid = r["event_id"]

        # ── 979816633: strip leading ") " from doubles person_canon/norm ──────
        if eid == "979816633" and r["division_raw"] == "doubles":
            old_pc = r["person_canon"]
            new_pc = re.sub(r"^\)\s+", "", old_pc)
            if new_pc != old_pc:
                r = dict(r)
                r["person_canon"] = new_pc
                r["norm"] = re.sub(r"^\)\s+", "", r["norm"])
                changed += 1

        # ── 979089216: fix "between Logan Dethman" ────────────────────────────
        elif eid == "979089216" and r["person_canon"].startswith("between "):
            r = dict(r)
            r["person_canon"] = r["person_canon"][len("between "):]
            r["norm"]         = r["norm"].replace("between ", "", 1)
            # Fix team_display_name for this tpk (both rows share it)
            # We'll do a second pass below for the team_display_name fix
            changed += 1

        # ── 979089216: fix "Travis strickland" capitalisation in tpk row ──────
        elif eid == "979089216" and r["person_canon"] == "Travis strickland":
            r = dict(r)
            r["person_canon"] = "Travis Strickland"
            r["norm"]         = "travis strickland"  # norm stays lower
            changed += 1

        # ── 1195677906: restore single-name tokens for Circle Competition ─────
        elif eid == "1195677906" and r["division_raw"] == "Circle Competition":
            # Map place → name from stage2 (confirmed order: 1=Jakob, 2=Matthias, 3=André)
            name_by_place = {"1": "Jakob", "2": "Matthias", "3": "André"}
            place = r["place"]
            if place in name_by_place and r["person_canon"] == "__NON_PERSON__":
                name = name_by_place[place]
                r = dict(r)
                r["person_canon"]     = name
                r["norm"]             = name.lower()
                r["person_unresolved"] = "1"
                changed += 1

        # ── 1369141018: remove "()" partner rows ─────────────────────────────
        elif eid == "1369141018" and r["person_canon"] == "()":
            removed += 1
            continue  # drop this row

        out.append(r)

    # Second pass: fix team_display_name for 979089216 tpk=c1faea841b71
    TARGET_TPK = "c1faea841b71"
    for r in out:
        if r["event_id"] == "979089216" and r["team_person_key"] == TARGET_TPK:
            if "between " in r.get("team_display_name", ""):
                r["team_display_name"] = r["team_display_name"].replace(
                    "between Logan Dethman", "Logan Dethman"
                )
                # Also fix Travis capitalisation in the display name
                r["team_display_name"] = r["team_display_name"].replace(
                    "Travis strickland", "Travis Strickland"
                )

    # Second pass: fix team_display_name " / ()" for 1369141018
    for r in out:
        if r["event_id"] == "1369141018":
            td = r.get("team_display_name", "")
            if " / ()" in td:
                r["team_display_name"] = td.replace(" / ()", "")

    # ── 1727756195: fix Open Golf group2 places and set division_raw ────────────
    # Collect event rows in order; find the block starting with Klimczak at place=1
    golf_rows = [r for r in out if r["event_id"] == GOLF_EID]
    klimczak_block_start = None
    for idx, r in enumerate(golf_rows):
        if r["person_canon"] == "Michal Klimczak" and r["place"] == "1":
            klimczak_block_start = idx
            break

    if klimczak_block_start is not None:
        golf_block = golf_rows[klimczak_block_start : klimczak_block_start + 10]
        for r in golf_block:
            r["division_raw"] = GOLF_DIV
            key = (r["person_canon"], r["place"])
            if key in GOLF_GROUP2_REMAP:
                r["place"] = GOLF_GROUP2_REMAP[key]
                changed += 1
        # group1 rows (Klimczak, Pietrzycki, Niczyporuk): division_raw set above,
        # place unchanged — count only the rows we modified
        changed += 3  # 3 group1 rows had division_raw updated
        print(f"Open Golf block found at event_idx={klimczak_block_start}; "
              f"group2 renumbered, all 10 rows set to div='Open Golf'")
    else:
        print("WARNING: Open Golf block not found for event 1727756195!")

    print(f"Rows changed: {changed}")
    print(f"Rows removed: {removed}")
    print(f"Output rows:  {len(out)}")
    return out


def main():
    with open(IN_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = [dict(r) for r in reader]

    print(f"Input rows: {len(rows)}")
    out = patch(rows)

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out)

    print(f"Written: {OUT_FILE}")


if __name__ == "__main__":
    main()
