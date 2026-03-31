"""
tools/patch_1982_1983_structural.py

Apply SAFE CHANGES (S-A1 through S-A5) and HIGH-CONFIDENCE FIXES (H-1 through H-4)
from the Authoritative Results 1980–1985 Analysis Report.

SAFE CHANGES:
  S-A1  1982_worlds: remove duplicate placement rows (FBW Mixed/Women's Dbls/Women's Sgls)
  S-A2  1982_worlds: remove duplicate disciplines (FBW Open Golf, Open Dbls Net, Open Sgls Net)
  S-A3  1982_worlds: remove blank-disc OLD_RESULTS rows + orphan FBW singles/team freestyle
  S-A4  1983_worlds_wfa: remove duplicate placement rows (FBW Women's Dbls/Sgls Net)
  S-A5  1983: remove duplicate OLD_RESULTS disciplines (Singles = Singles Freestyle,
                Team = Team Freestyle) in both NHSA and WFA;
              remove FBW Open Dbls Net duplicates from both events

HIGH-CONFIDENCE FIXES:
  H-1  1983_worlds_nhsa: remove FBW Mixed Dbls Net (WFA contamination in team_raw rows)
  H-2  1983_worlds_wfa: remove FBW Mixed Dbls Net (NHSA contamination in team_raw rows)
  H-3  1983_worlds_nhsa: remove FBW Open Golf, Open Sgls Consecutive, Open Sgls Freestyle
       (WFA data at wrong event; correct OLD_RESULTS data kept)
  H-4  1983_worlds_wfa: remove FBW Open Golf, Open Sgls Freestyle, Open Team Freestyle
       (wrong/contaminated data; correct OLD_RESULTS data kept)

Not applied: R-1, R-2, R-3, R-4, R-5 (review required items).
"""

import csv
from pathlib import Path
from collections import defaultdict

ROOT  = Path(__file__).resolve().parents[1]
EARLY = ROOT / "early_data" / "final_pre1997"

RESULTS_PATH = EARLY / "event_results_pre1997.csv"
PARTS_PATH   = EARLY / "event_result_participants_pre1997.csv"
DISCS_PATH   = EARLY / "event_disciplines_pre1997.csv"


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


# =============================================================================
# Define removal criteria
# Each entry: (canonical_event_id, division_raw, source_type_or_None)
# source_type_or_None = None means remove regardless of source_type
# =============================================================================

# FBW rows to remove — all contaminated or duplicate
FBW_REMOVE = [
    # --- 1982_worlds FBW rows (S-A1, S-A2, S-A3) ---
    ("1982_worlds", "Mixed Dbls Net",       "FBW"),   # S-A1: dup of Mixed Doubles Net
    ("1982_worlds", "Open Dbls Net",         "FBW"),   # S-A2: dup of Doubles Net
    ("1982_worlds", "Open Golf",             "FBW"),   # S-A2: dup of Golf
    ("1982_worlds", "Open Sgls Net",         "FBW"),   # S-A2: dup of Singles Net
    ("1982_worlds", "Open Sgls Freestyle",   "FBW"),   # S-A3: orphan (Gary Laut truncation)
    ("1982_worlds", "Open Team Freestyle",   "FBW"),   # S-A3: orphan (single placement, reversed)
    ("1982_worlds", "Women's Dbls Net",      "FBW"),   # S-A1: dup of Women's Doubles Net
    ("1982_worlds", "Women's Sgls Net",      "FBW"),   # S-A1: dup of Women's Singles Net

    # --- 1983_worlds_nhsa FBW rows (H-1, H-3, S-A5) ---
    ("1983_worlds_nhsa", "Mixed Dbls Net",      "FBW"),  # H-1: WFA contamination (Lori Jean Tarr/Bruce Guettich)
    ("1983_worlds_nhsa", "Open Dbls Net",        "FBW"),  # S-A5: dup of Doubles Net
    ("1983_worlds_nhsa", "Open Golf",            "FBW"),  # H-3: WFA Golf at wrong event (wrong p2: Mackey vs Prater)
    ("1983_worlds_nhsa", "Open Sgls Consecutive","FBW"),  # H-3: WFA consecutive (Jack Schoolcraft) at wrong event
    ("1983_worlds_nhsa", "Open Sgls Freestyle",  "FBW"),  # H-3: WFA Singles Freestyle at NHSA event
    ("1983_worlds_nhsa", "Open Sgls Net",        "FBW"),  # S-A5: dup of Singles Net
    ("1983_worlds_nhsa", "Open Team Freestyle",  "FBW"),  # S-A5: dup of Team Freestyle (team_raw format)

    # --- 1983_worlds_wfa FBW rows (H-2, H-4, S-A4, S-A5) ---
    ("1983_worlds_wfa", "Mixed Dbls Net",      "FBW"),   # H-2: NHSA contamination (Nancy Reynolds/Constance Constable)
    ("1983_worlds_wfa", "Open Dbls Net",        "FBW"),   # S-A5: dup of Doubles Net
    ("1983_worlds_wfa", "Open Golf",            "FBW"),   # H-4: wrong data (Kenny Shults p1, correct is Mike Harding)
    ("1983_worlds_wfa", "Open Sgls Freestyle",  "FBW"),   # H-4: wrong data (Kenny Shults p1, correct is Jack Schoolcraft)
    ("1983_worlds_wfa", "Open Sgls Net",        "FBW"),   # S-A5: dup of Singles Net
    ("1983_worlds_wfa", "Open Team Freestyle",  "FBW"),   # H-4: NHSA contamination (Mag Hughes/Greg Cortopassi)
    ("1983_worlds_wfa", "Women's Dbls Net",     "FBW"),   # S-A4: dup of Women's Doubles Net
    ("1983_worlds_wfa", "Women's Sgls Net",     "FBW"),   # S-A4: dup of Women's Singles Net
]

# OLD_RESULTS rows to remove — within-source duplicates
OLD_RESULTS_REMOVE = [
    # --- 1982_worlds (S-A3): blank disc = duplicate of Freestyle ---
    ("1982_worlds", "",        "OLD_RESULTS"),

    # --- 1983_worlds_nhsa (S-A5): Singles = Singles Freestyle, Team = Team Freestyle ---
    ("1983_worlds_nhsa", "Singles", "OLD_RESULTS"),
    ("1983_worlds_nhsa", "Team",    "OLD_RESULTS"),

    # --- 1983_worlds_wfa (S-A5): same duplicate disciplines ---
    ("1983_worlds_wfa", "Singles", "OLD_RESULTS"),
    ("1983_worlds_wfa", "Team",    "OLD_RESULTS"),
]

ALL_REMOVE = FBW_REMOVE + OLD_RESULTS_REMOVE

# Build lookup set: (event_id, division_raw, source_type)
REMOVE_SET = set(ALL_REMOVE)


# =============================================================================
# 1. Patch event_results_pre1997.csv
# =============================================================================
print("Patching event_results_pre1997.csv ...")
results, results_fields = load_csv(RESULTS_PATH)

new_results = []
removed_by_cat = defaultdict(int)

for row in results:
    key = (row["canonical_event_id"], row["division_raw"], row["source_type"])
    if key in REMOVE_SET:
        cat = f"{row['canonical_event_id']} / {row['division_raw']!r} ({row['source_type']})"
        removed_by_cat[cat] += 1
        continue
    new_results.append(row)

print(f"  Removed {len(results) - len(new_results)} result rows:")
for cat, cnt in sorted(removed_by_cat.items()):
    print(f"    {cnt}  {cat}")

save_csv(RESULTS_PATH, new_results, results_fields)

# Collect result_ids that were removed (for participant cascade)
kept_result_ids = {r["result_id"] for r in new_results}
removed_result_ids = {r["result_id"] for r in results} - kept_result_ids


# =============================================================================
# 2. Patch event_result_participants_pre1997.csv
# =============================================================================
print("\nPatching event_result_participants_pre1997.csv ...")
parts, parts_fields = load_csv(PARTS_PATH)

new_parts = []
parts_removed = 0

for row in parts:
    if row["result_id"] in removed_result_ids:
        parts_removed += 1
        continue
    new_parts.append(row)

print(f"  Removed {parts_removed} participant rows (cascade from result removals)")
save_csv(PARTS_PATH, new_parts, parts_fields)


# =============================================================================
# 3. Patch event_disciplines_pre1997.csv
# =============================================================================
print("\nPatching event_disciplines_pre1997.csv ...")
discs, discs_fields = load_csv(DISCS_PATH)

# Build set of (event_id, division_raw) combos to remove from disciplines
DISCS_REMOVE = {(eid, div) for (eid, div, _src) in ALL_REMOVE}

# Additionally remove any discipline entry whose division_raw has NO remaining result rows
remaining_div_keys = {(r["canonical_event_id"], r["division_raw"]) for r in new_results}

new_discs = []
discs_removed = 0
for row in discs:
    key = (row["canonical_event_id"], row["division_raw"])
    if key in DISCS_REMOVE:
        discs_removed += 1
        continue
    # Also purge any orphan discipline with no results left
    affected_events = {"1982_worlds", "1983_worlds_nhsa", "1983_worlds_wfa"}
    if row["canonical_event_id"] in affected_events and key not in remaining_div_keys:
        print(f"  Orphan discipline removed: {row['canonical_event_id']} / {row['division_raw']!r}")
        discs_removed += 1
        continue
    new_discs.append(row)

print(f"  Removed {discs_removed} discipline rows")
save_csv(DISCS_PATH, new_discs, discs_fields)


# =============================================================================
# Summary
# =============================================================================
print("\n=== PATCH SUMMARY ===")
print(f"  Results removed:      {len(results) - len(new_results)}")
print(f"  Participants removed: {parts_removed}")
print(f"  Disciplines removed:  {discs_removed}")
print("\nRun pipeline: scripts 10 → 12 → 05 → 05p5, then QC")
