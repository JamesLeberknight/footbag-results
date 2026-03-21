#!/usr/bin/env python3
"""
patch_pbp_v76_to_v77.py

Convert all "old-format" team rows in PBP v76 to new piped-UUID format.

Old format: competitor_type="team", person_canon=<actual name>
  — one row per team member, team_person_key is a short hash

New format: competitor_type="team", person_canon="__NON_PERSON__"
  — one row per team, team_person_key is "uuid1|uuid2" (piped)

Process:
  1. Group old-format rows by (event_id, division_canon, place, team_display_name)
  2. Clean pairs (size == 2): merge into one new-format row
  3. Ambiguous groups (size != 2): leave unchanged, report to stdout
  4. All other rows: pass through unchanged

UUID strategy for team_person_key:
  - Resolved player: use existing person_id
  - Unresolved player (empty person_id): generate uuid5(PLAYERS_NAMESPACE, norm)
    matching stage2 convention in 02_canonicalize_results.py

Output: inputs/identity_lock/Placements_ByPerson_v77.csv
"""

import csv
import uuid
from collections import defaultdict

INPUT  = "inputs/identity_lock/Placements_ByPerson_v76.csv"
OUTPUT = "inputs/identity_lock/Placements_ByPerson_v77.csv"

# UUID5 namespace for unresolved players — matches stage2 PLAYERS_NAMESPACE
PLAYERS_NAMESPACE = uuid.UUID("11111111-2222-3333-4444-555555555555")


def get_uuid(row):
    """Return the UUID to use for this row's player in the team key."""
    if row["person_id"]:
        return row["person_id"]
    # Generate stable UUID5 from normalized name (matches stage2 convention)
    norm = row["person_canon"].lower().strip()
    return str(uuid.uuid5(PLAYERS_NAMESPACE, norm))


def is_old_format(row):
    return row["competitor_type"] == "team" and row["person_canon"] != "__NON_PERSON__"


def group_key(row):
    return (row["event_id"], row["division_canon"], row["place"], row["team_display_name"])


def merge_pair(r1, r2):
    """Create a new-format team row from two old-format partner rows."""
    uid1 = get_uuid(r1)
    uid2 = get_uuid(r2)
    team_key = f"{uid1}|{uid2}"

    merged = dict(r1)
    merged["person_id"]       = ""
    merged["person_canon"]    = "__NON_PERSON__"
    merged["person_unresolved"] = ""
    merged["norm"]            = ""
    merged["team_person_key"] = team_key
    # team_display_name: already "Player A / Player B" — keep as-is
    # coverage_flag: both rows of a pair share the same value — keep from r1
    return merged


def main():
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        all_rows = list(reader)

    print(f"Input rows: {len(all_rows)}")

    # --- Classify old-format rows ---
    groups = defaultdict(list)
    for row in all_rows:
        if is_old_format(row):
            groups[group_key(row)].append(row)

    clean_keys  = {k for k, v in groups.items() if len(v) == 2}
    ambig_keys  = {k for k, v in groups.items() if len(v) != 2}

    print(f"Old-format team rows: {sum(len(v) for v in groups.values())}")
    print(f"Clean pairs (will convert): {len(clean_keys)}")
    print(f"Ambiguous groups (left as-is): {len(ambig_keys)}")

    if ambig_keys:
        print()
        print("AMBIGUOUS GROUPS (not converted):")
        for k in sorted(ambig_keys):
            eid, div, place, disp = k
            members = groups[k]
            print(f"  event={eid} {div!r} p{place} size={len(members)} [{disp!r}]")
            for m in members:
                pid = m["person_id"][:8] if m["person_id"] else "unres"
                print(f"    {m['person_canon']!r} ({pid})")

    # --- Build merged rows (one per clean pair) ---
    merged_rows = {}
    for k in clean_keys:
        pair = groups[k]
        merged_rows[k] = merge_pair(pair[0], pair[1])

    # --- Build output preserving original row order ---
    output_rows = []
    seen_clean_keys = set()

    for row in all_rows:
        if is_old_format(row):
            k = group_key(row)
            if k in clean_keys:
                if k not in seen_clean_keys:
                    output_rows.append(merged_rows[k])
                    seen_clean_keys.add(k)
                # else: second row of pair — skip (already emitted merged)
            else:
                # Ambiguous: keep as-is
                output_rows.append(row)
        else:
            output_rows.append(row)

    print()
    print(f"Output rows: {len(output_rows)}")
    print(f"  = {len(all_rows)} input"
          f" - {len(clean_keys) * 2} old-format"
          f" + {len(clean_keys)} merged"
          f" = {len(all_rows) - len(clean_keys)} expected")

    # --- Write output ---
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\nWrote {OUTPUT}")

    # --- Per-event summary for notable events ---
    events_changed = defaultdict(lambda: {"removed": 0, "added": 0})
    for k in clean_keys:
        eid = k[0]
        events_changed[eid]["removed"] += 2
        events_changed[eid]["added"]   += 1

    top_events = sorted(events_changed.items(), key=lambda x: -x[1]["removed"])[:10]
    print()
    print("Top 10 events by rows changed:")
    for eid, delta in top_events:
        net = delta["added"] - delta["removed"]
        print(f"  {eid}: -{delta['removed']} +{delta['added']} (net {net:+d})")


if __name__ == "__main__":
    main()
