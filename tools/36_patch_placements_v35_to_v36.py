#!/usr/bin/env python3
"""
tools/36_patch_placements_v35_to_v36.py

Targeted patch: Placements_ByPerson_v35 → v36

Fix: "Last, First" European name format was incorrectly parsed as doubles teams
     in singles divisions. After parser fix in 02_canonicalize_results.py, these
     entries now correctly resolve to known persons. This patch adds them to PBP.

Changes:
  - For 8 events using "Last, First" format, add newly resolvable single-player
    placements that were missing from v35 (128 placements, ~97 now resolvable).
  - No deletions or identity changes. Existing v35 rows are preserved unchanged.
"""
import csv
import json
import re
import sys
from pathlib import Path
from unicodedata import normalize

ROOT = Path(__file__).resolve().parents[1]
IN_PBP = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v35.csv"
OUT_PBP = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v36.csv"
PT_CSV = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v32.csv"
STAGE2_CSV = ROOT / "out" / "stage2_canonical_events.csv"

# Events that used "Last, First" format — identified by parser fix analysis
AFFECTED_EVENTS = {
    '1284182261',  # 1era COPA VENEZUELA
    '1286381599',  # 4to Campeonato Suramericano de Footbag
    '1293877917',  # 13th Annual IFPA European Footbag Championships
    '1299019355',  # 10 years Pieds à Gilles - 7th Swiss Footbag Open
    '1353223688',  # Finnish Footbag Open
    '1366240051',  # 4ta. Copa Ciencias
    '1435784719',  # The Perpetual Flame Footbag Net Challenge
    '1466891959',  # Swiss Footbag Open 2016
    '1568961264',  # RNH Footbag 20th Anniversary
}

LAST_FIRST_RE = re.compile(
    r'^[A-Za-zÀ-ÿ][\w\-]*(?: [\w\-]+)?,\s*[A-Za-zÀ-ÿ][\w\-]*$'
)


def norm_key(name: str) -> str:
    """Unicode-normalized lowercase for matching."""
    n = normalize("NFKD", name).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r'\s+', ' ', n).strip()


csv.field_size_limit(10_000_000)

# ── Load PT v32 ──────────────────────────────────────────────────────────────
print("Loading Persons_Truth_Final_v32.csv...")
player_to_person: dict[str, tuple[str, str]] = {}  # player_id → (person_id, person_canon)
with open(PT_CSV, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        pid = row['effective_person_id']
        canon = row['person_canon']
        for uid in row['player_ids_seen'].split(' | '):
            uid = uid.strip()
            if uid:
                player_to_person[uid] = (pid, canon)
print(f"  Loaded {len(player_to_person):,} player_id → person mappings")

# ── Load existing PBP v35 ────────────────────────────────────────────────────
print("Loading Placements_ByPerson_v35.csv...")
existing_rows: list[dict] = []
existing_keys: set[tuple] = set()  # (event_id, division_canon, place)
with open(IN_PBP, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        existing_rows.append(row)
        existing_keys.add((row['event_id'], row['division_canon'], row['place']))
print(f"  Loaded {len(existing_rows):,} rows")

# ── Load stage2 for affected events ──────────────────────────────────────────
print("Loading stage2_canonical_events.csv for affected events...")
new_rows: list[dict] = []
added = 0
skipped_already_present = 0
skipped_unresolved = 0

with open(STAGE2_CSV, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        eid = row['event_id']
        if eid not in AFFECTED_EVENTS:
            continue
        year = row.get('year', '')
        pj = json.loads(row.get('placements_json', '[]'))
        for p in pj:
            entry_raw = p.get('entry_raw', '')
            # Only process Last,First entries (the bug we're fixing)
            if not LAST_FIRST_RE.match(entry_raw):
                continue
            div_canon = p.get('division_canon', '')
            div_cat = p.get('division_category', '')
            place = str(p.get('place', ''))
            ctype = p.get('competitor_type', 'player')
            player1_id = p.get('player1_id')

            key = (eid, div_canon, place)
            if key in existing_keys:
                skipped_already_present += 1
                continue

            if not player1_id or player1_id not in player_to_person:
                skipped_unresolved += 1
                continue

            person_id, person_canon = player_to_person[player1_id]
            new_rows.append({
                'event_id': eid,
                'year': year,
                'division_canon': div_canon,
                'division_category': div_cat,
                'place': place,
                'competitor_type': 'player',
                'person_id': person_id,
                'team_person_key': '',
                'person_canon': person_canon,
                'team_display_name': '',
                'coverage_flag': 'complete',
                'person_unresolved': '',
                'norm': norm_key(person_canon),
            })
            existing_keys.add(key)
            added += 1

print(f"  New resolvable placements: {added}")
print(f"  Already present (skipped): {skipped_already_present}")
print(f"  Unresolved (skipped): {skipped_unresolved}")

# ── Merge and write output ────────────────────────────────────────────────────
# Sort: existing rows first (preserve order), new rows appended by event/div/place
all_rows = existing_rows + sorted(
    new_rows,
    key=lambda r: (r['event_id'], r['division_canon'], int(r['place']))
)

print(f"\nWriting {len(all_rows):,} rows to {OUT_PBP.name}...")
with open(OUT_PBP, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_rows)

print(f"Done. v35: {len(existing_rows):,} rows → v36: {len(all_rows):,} rows (+{added})")
