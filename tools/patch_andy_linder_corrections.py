"""
tools/patch_andy_linder_corrections.py
Apply Andy Linder historical corrections from overrides/andy_linder_corrections.yaml

Changes applied (S-01 through S-16, HC-01, HC-02):
  S-01  Remove 1983_worlds_wfa / Open Sgls Consecutive / p1 (only Andy) → cascade result
  S-02  Remove 1986_worlds_golden / Open Sgls Consecutive / p1 (only Andy) → cascade result
  S-03  Remove 8667de3590 (1987_worlds) / Open Sgls Consecutive / p1 → cascade result
  S-04  Remove Andy from 8667de3590 / Open Dbls Consecutive / p3 → keep Scott, add sentinel
  S-05  Remove 1989_worlds_golden / Open Sgls Consecutive / p1 → cascade result
  S-06  Remove Andy from 1990_worlds / Open Team Freestyle / p3 → keep Scott, add sentinel
  S-07  Remove 1990_worlds / Open Sgls Consecutive / p1 → cascade result
  S-08  Remove Andy from 1991_worlds / Open Team Freestyle / p2 → keep Scott, add sentinel
  S-09  Remove 1991_worlds / Open Sgls Consecutive / p1 → cascade result
  S-10  Shift 1991_worlds / Open Sgls Freestyle: p2=Dennis Ross→Andy, p3=Jim Fitzgerald→Dennis
  S-11  Remove Andy from 1992_worlds / Open Team Freestyle / p2 → keep Scott, add sentinel
  S-12  Remove 1992_worlds / Open Sgls Consecutive / p1 → cascade result
  S-13  Remove Andy from 1993_worlds / Open Team Freestyle / p2 → keep Scott, add sentinel
  S-14  Remove 1993_worlds / Open Sgls Consecutive / p1 → cascade result
  S-15  Remove Andy from 1994_worlds_palo_alto / Open Team Freestyle / p2 → keep Scott, sentinel
  S-16  Remove 1994_worlds_palo_alto / Open Sgls Consecutive / p1 → cascade result
  HC-01 Remove 1982_worlds / Open Sgls Consecutive discipline (all rows, cascade)
  HC-02 Update 1985_worlds event_type to WFA_WORLD_CHAMPIONSHIPS

Post-1997 changes are added to pipeline/05p5_remediate_canonical.py (S-17, S-18, S-19).
"""

import csv
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EARLY = ROOT / "early_data" / "final_pre1997"

ANDY_ID    = "64a7a989-aa2c-5a58-b141-e8378be4a962"
ANDY_CANON = "Andy Linder"
DENNIS_ID  = "84641a6f-36d4-5857-af3f-827b1f57102f"
DENNIS_CANON = "Dennis Ross"
UNKNOWN_SENTINEL = "[UNKNOWN PARTNER]"

def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)
    return rows, fieldnames

def save_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Saved {path.name}: {len(rows)} rows")

# ── Singles-only result_ids to REMOVE (Andy is only participant) ──────────────
# cascade = remove participant + result row
SINGLES_CASCADE_REMOVE = {
    "70305348fe",  # S-01: 1983_worlds_wfa / Open Sgls Consecutive / p1
    "9a6d30681d",  # S-02: 1986_worlds_golden / Open Sgls Consecutive / p1
    "f049610d9a",  # S-03: 8667de3590 / Open Sgls Consecutive / p1
    "7d648aa282",  # S-05: 1989_worlds_golden / Open Sgls Consecutive / p1
    "cb8e377361",  # S-07: 1990_worlds / Open Sgls Consecutive / p1
    "eed1531305",  # S-09: 1991_worlds / Open Sgls Consecutive / p1
    "7a870cf995",  # S-12: 1992_worlds / Open Sgls Consecutive / p1
    "77545950d9",  # S-14: 1993_worlds / Open Sgls Consecutive / p1
    "02b217f91f",  # S-16: 1994_worlds_palo_alto / Open Sgls Consecutive / p1
}

# HC-01: Remove ALL 1982_worlds / "Open Sgls Consecutive" rows (duplicate discipline)
HC01_REMOVE = {
    "590eadc6cb",  # 1982_worlds / Open Sgls Consecutive / p1 Kenny Shults
    "079c2f8370",  # 1982_worlds / Open Sgls Consecutive / p2 Andy Linder
    "5a70962dc8",  # 1982_worlds / Open Sgls Consecutive / p3 Gary Laut
}

ALL_RESULT_REMOVE = SINGLES_CASCADE_REMOVE | HC01_REMOVE

# ── Team result_ids where Andy should be REMOVED and replaced with sentinel ───
TEAM_SENTINEL = {
    "866e822855",  # S-06: 1990_worlds / Open Team Freestyle / p3
    "5a9872f948",  # S-08: 1991_worlds / Open Team Freestyle / p2
    "c41b5a7697",  # S-11: 1992_worlds / Open Team Freestyle / p2
    "393ea10b01",  # S-13: 1993_worlds / Open Team Freestyle / p2
    "abad56fa83",  # S-15: 1994_worlds_palo_alto / Open Team Freestyle / p2
    "28c3bb0f22",  # S-04: 8667de3590 / Open Dbls Consecutive / p3
}

# ── S-10: 1991_worlds Open Sgls Freestyle placement shift ────────────────────
# p2 49189e7843: Dennis Ross → Andy Linder
# p3 8b48ba4513: Jim Fitzgerald → Dennis Ross
S10_P2_RESULT_ID = "49189e7843"
S10_P3_RESULT_ID = "8b48ba4513"

JIM_FG_ID = "b54020bc-1a1a-5d23-89e1-34617b3514fa"


# =============================================================================
# 1. Patch event_result_participants_pre1997.csv
# =============================================================================
print("Patching event_result_participants_pre1997.csv...")

parts_path = EARLY / "event_result_participants_pre1997.csv"
parts, parts_fields = load_csv(parts_path)

new_parts = []
sentinel_inserts = 0
singles_removed  = 0
hc01_removed     = 0
s10_changed      = 0

for row in parts:
    rid = row["result_id"]

    # Remove singles / HC-01 rows entirely
    if rid in ALL_RESULT_REMOVE:
        if rid in SINGLES_CASCADE_REMOVE:
            singles_removed += 1
        else:
            hc01_removed += 1
        continue  # skip — cascaded away

    # Team sentinel: remove Andy, keep partner
    if rid in TEAM_SENTINEL:
        if row["person_id"] == ANDY_ID:
            # Replace with sentinel
            sentinel_row = dict(row)
            sentinel_row["player_name_raw"] = UNKNOWN_SENTINEL
            sentinel_row["person_id"]       = ""
            sentinel_row["person_canon"]    = UNKNOWN_SENTINEL
            sentinel_row["resolution_status"] = "SENTINEL"
            new_parts.append(sentinel_row)
            sentinel_inserts += 1
        else:
            new_parts.append(row)
        continue

    # S-10: p2 Dennis Ross → Andy Linder
    if rid == S10_P2_RESULT_ID:
        if row["person_id"] == DENNIS_ID:
            row["player_name_raw"] = ANDY_CANON
            row["person_id"]       = ANDY_ID
            row["person_canon"]    = ANDY_CANON
            s10_changed += 1
        new_parts.append(row)
        continue

    # S-10: p3 Jim Fitzgerald → Dennis Ross
    if rid == S10_P3_RESULT_ID:
        if row["person_id"] == JIM_FG_ID:
            row["player_name_raw"] = DENNIS_CANON
            row["person_id"]       = DENNIS_ID
            row["person_canon"]    = DENNIS_CANON
            s10_changed += 1
        new_parts.append(row)
        continue

    new_parts.append(row)

print(f"  Singles removed:    {singles_removed}")
print(f"  HC-01 removed:      {hc01_removed}")
print(f"  Sentinel inserts:   {sentinel_inserts}")
print(f"  S-10 changed:       {s10_changed}")
save_csv(parts_path, new_parts, parts_fields)


# =============================================================================
# 2. Patch event_results_pre1997.csv
# =============================================================================
print("\nPatching event_results_pre1997.csv...")

results_path = EARLY / "event_results_pre1997.csv"
results, results_fields = load_csv(results_path)

new_results = []
results_removed = 0
team_display_updated = 0
s10_results_changed  = 0

for row in results:
    rid = row["result_id"]

    # Remove cascade singles + HC-01
    if rid in ALL_RESULT_REMOVE:
        results_removed += 1
        continue

    # Team sentinel: update team_raw display
    if rid in TEAM_SENTINEL:
        old_team = row.get("team_raw", "")
        if "Andy Linder" in old_team:
            new_team = old_team.replace("Andy Linder", UNKNOWN_SENTINEL)
            row["team_raw"] = new_team
            team_display_updated += 1
        new_results.append(row)
        continue

    # S-10: p2 result display
    if rid == S10_P2_RESULT_ID:
        if "Dennis Ross" in row.get("player_raw", ""):
            row["player_raw"] = ANDY_CANON
            s10_results_changed += 1
        new_results.append(row)
        continue

    # S-10: p3 result display
    if rid == S10_P3_RESULT_ID:
        if "Jim Fitzgerald" in row.get("player_raw", ""):
            row["player_raw"] = DENNIS_CANON
            s10_results_changed += 1
        new_results.append(row)
        continue

    new_results.append(row)

print(f"  Results removed:          {results_removed}")
print(f"  Team display updated:     {team_display_updated}")
print(f"  S-10 result display:      {s10_results_changed}")
save_csv(results_path, new_results, results_fields)


# =============================================================================
# 3. Patch event_disciplines_pre1997.csv (HC-01)
# =============================================================================
print("\nPatching event_disciplines_pre1997.csv (HC-01)...")

disc_path = EARLY / "event_disciplines_pre1997.csv"
discs, disc_fields = load_csv(disc_path)

new_discs = [
    row for row in discs
    if not (row["canonical_event_id"] == "1982_worlds" and row["division_raw"] == "Open Sgls Consecutive")
]
removed_disc = len(discs) - len(new_discs)
print(f"  HC-01 disciplines removed: {removed_disc}")
save_csv(disc_path, new_discs, disc_fields)


# =============================================================================
# 4. Patch events_pre1997.csv (HC-02)
# =============================================================================
print("\nPatching events_pre1997.csv (HC-02)...")

events_path = EARLY / "events_pre1997.csv"
evts, evt_fields = load_csv(events_path)

hc02_changed = 0
for row in evts:
    if row["canonical_event_id"] == "1985_worlds":
        if row["normalized_event_type"] != "WFA_WORLD_CHAMPIONSHIPS":
            print(f"  HC-02: 1985_worlds event_type: {row['normalized_event_type']} → WFA_WORLD_CHAMPIONSHIPS")
            row["normalized_event_type"] = "WFA_WORLD_CHAMPIONSHIPS"
            hc02_changed += 1

print(f"  HC-02 events updated: {hc02_changed}")
save_csv(events_path, evts, evt_fields)


print("\nDone. Run pipeline: 05 → 05p5 → script 12 → build_appsafe_merged → QC")
