"""
tools/57_patch_pbp_v66_to_v67.py
Patch Placements_ByPerson v66 → v67

Fixes event 1386623061 (U.S. Open Footbag Championships 2014):
  - Intermediate Singles Net: places 1 (Steve Femmel) and 2 (Joey Vu) missing
  - Open Singles Net: places 1 (Kenny Shults), 2 (Genevieve Bousquet), 3 (Chris Young) missing

Both gaps confirmed against stage2_canonical_events.csv results_raw.
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "inputs" / "identity_lock"
SRC  = LOCK / "Placements_ByPerson_v66.csv"
DST  = LOCK / "Placements_ByPerson_v67.csv"

csv.field_size_limit(10 * 1024 * 1024)

NEW_ROWS = [
    # Intermediate Singles Net — place 1, 2
    {
        "event_id":        "1386623061",
        "year":            "2014",
        "division_canon":  "Intermediate Singles Net",
        "division_category": "net",
        "place":           "1",
        "competitor_type": "player",
        "person_id":       "fb605451-fa41-5e6d-ad21-1b541b3fc82b",
        "team_person_key": "",
        "person_canon":    "Steve Femmel",
        "team_display_name": "",
        "coverage_flag":   "complete",
        "person_unresolved": "",
        "norm":            "steve femmel",
        "division_raw":    "",
    },
    {
        "event_id":        "1386623061",
        "year":            "2014",
        "division_canon":  "Intermediate Singles Net",
        "division_category": "net",
        "place":           "2",
        "competitor_type": "player",
        "person_id":       "320a02aa-961a-57f9-a2e5-81fe6fec03e1",
        "team_person_key": "",
        "person_canon":    "Joey Vu",
        "team_display_name": "",
        "coverage_flag":   "complete",
        "person_unresolved": "",
        "norm":            "joey vu",
        "division_raw":    "",
    },
    # Open Singles Net — places 1, 2, 3
    {
        "event_id":        "1386623061",
        "year":            "2014",
        "division_canon":  "Open Singles Net",
        "division_category": "net",
        "place":           "1",
        "competitor_type": "player",
        "person_id":       "2a6a7c9e-1d8a-4f9a-a8f5-6f3a3c1e9b0f",
        "team_person_key": "",
        "person_canon":    "Kenneth Shults",
        "team_display_name": "",
        "coverage_flag":   "complete",
        "person_unresolved": "",
        "norm":            "kenneth shults",
        "division_raw":    "",
    },
    {
        "event_id":        "1386623061",
        "year":            "2014",
        "division_canon":  "Open Singles Net",
        "division_category": "net",
        "place":           "2",
        "competitor_type": "player",
        "person_id":       "fea99a91-ae13-5cb1-b87f-3c352783dc2e",
        "team_person_key": "",
        "person_canon":    "Genevieve Bousquet",
        "team_display_name": "",
        "coverage_flag":   "complete",
        "person_unresolved": "",
        "norm":            "genevieve bousquet",
        "division_raw":    "",
    },
    {
        "event_id":        "1386623061",
        "year":            "2014",
        "division_canon":  "Open Singles Net",
        "division_category": "net",
        "place":           "3",
        "competitor_type": "player",
        "person_id":       "646452e8-7ede-58d9-aa7d-f82c092800ad",
        "team_person_key": "",
        "person_canon":    "Chris Young",
        "team_display_name": "",
        "coverage_flag":   "complete",
        "person_unresolved": "",
        "norm":            "chris young",
        "division_raw":    "",
    },
]

# Build a set of existing (event_id, division_canon, place, person_id) to detect duplicates
with open(SRC, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

existing_keys = {
    (r["event_id"], r["division_canon"], r["place"], r["person_id"])
    for r in rows
}

added = 0
for nr in NEW_ROWS:
    key = (nr["event_id"], nr["division_canon"], nr["place"], nr["person_id"])
    if key in existing_keys:
        print(f"  SKIP (already exists): {nr['division_canon']} p{nr['place']} {nr['person_canon']}")
    else:
        rows.append(nr)
        existing_keys.add(key)
        added += 1
        print(f"  ADD: {nr['division_canon']} p{nr['place']} {nr['person_canon']}")

# Sort: event_id, division_canon, place (int), person_canon
rows.sort(key=lambda r: (
    r["event_id"],
    r["division_canon"],
    int(r["place"]) if r["place"].isdigit() else 0,
    r["person_canon"],
))

with open(DST, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)

print(f"\nv66 ({len(rows) - added:,} rows) → v67 ({len(rows):,} rows, +{added} added)")
print(f"Written: {DST}")
