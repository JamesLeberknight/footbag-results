#!/usr/bin/env python3
"""
tools/37_patch_pt_v32_to_v33_and_pbp_v36_to_v37.py

Combined patch:
  Persons_Truth_Final_v32.csv → v33  (+2 new persons, 1 updated)
  Placements_ByPerson_v36.csv → v37  (+35 placements from Last,First events)

Changes to PT:
  1. Markus Kapszak (9c8376c9): add UUID 6f4e302e ("Kaspczak, Markus" token)
     + "Markus Kaspczak" to player_names_seen and aliases_presentable
  2. NEW: Vlad Eskanasy (bd039e6a) — place 23, Swiss Open 2011
  3. NEW: Wilder González (6a2b2558) — place 18, Copa Venezuela

Changes to PBP:
  35 placements from 9 Last,First events unresolved after v35→v36 patch.
  Resolved via "Last, First" → "First Last" name lookup against PT v33.
"""
import csv
import json
import re
import uuid
from pathlib import Path
from unicodedata import normalize

ROOT = Path(__file__).resolve().parents[1]

IN_PT   = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v32.csv"
OUT_PT  = ROOT / "inputs" / "identity_lock" / "Persons_Truth_Final_v33.csv"
IN_PBP  = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v36.csv"
OUT_PBP = ROOT / "inputs" / "identity_lock" / "Placements_ByPerson_v37.csv"
STAGE2  = ROOT / "out" / "stage2_canonical_events.csv"

csv.field_size_limit(10_000_000)

_UUID_NS_PERSON = uuid.UUID("3b5d5c7e-7c4b-4d21-8b44-3c39d1a0f4d6")

AFFECTED_EVENTS = {
    '1284182261', '1286381599', '1293877917', '1299019355',
    '1353223688', '1366240051', '1435784719', '1466891959', '1568961264',
}

LAST_FIRST_RE = re.compile(
    r'^[A-Za-zÀ-ÿ][\w\-]*(?: [\w\-]+)?,\s*[A-Za-zÀ-ÿ][\w\-]*$'
)

# Manual overrides: entry_raw → (person_id, person_canon)
# For cases where name reconstruction fails:
#   "Kaspczak, Markus" → "Markus Kaspczak" doesn't match "Markus Kapszak" (z vs sz)
#   "Quimel, Gonzales" → source has last/first reversed (first=Quimel, last=Gonzales)
#   "Orace, Jhon" → PT has "Jhon Orace Valera" (full name)
#   "Pacheco, Wladiuska" → PT has "Wladiuska Pacheco Castro" (full name)
MANUAL_OVERRIDES: dict[str, tuple[str, str]] = {
    'Kaspczak, Markus': ('9c8376c9-68ea-572f-abe2-822871cf0e7b', 'Markus Kapszak'),
    'Quimel, Gonzales': ('db589403-4ad7-51a1-b839-201eaffc1fca', 'Quimel Gonzales'),
    'Orace, Jhon':      ('267a2b93-5e5f-5298-9712-a30b4177a6ac', 'Jhon Orace Valera'),
    'Pacheco, Wladiuska': ('719e635e-65c0-5f2b-bbae-a483b0a2a6c2', 'Wladiuska Pacheco Castro'),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def norm(s: str) -> str:
    n = normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r'\s+', ' ', n).strip()

def last_first_to_first_last(entry_raw: str) -> str:
    """'Daouk, Karim' → 'Karim Daouk';  'Karki,Tuomas' → 'Tuomas Karki'"""
    parts = [p.strip() for p in entry_raw.split(',', 1)]
    if len(parts) == 2:
        return parts[1] + ' ' + parts[0]
    return entry_raw

# ── Step 1: Build PT v33 ──────────────────────────────────────────────────────
print("=== PT v32 → v33 ===")

KAPSZAK_PID    = '9c8376c9-68ea-572f-abe2-822871cf0e7b'
KAPSZAK_NEW_UID = '6f4e302e-f7aa-5037-9069-af4c5fd8534d'   # token "Kaspczak, Markus"

ESKANASY_PID  = 'bd039e6a-6fd1-52e5-bc37-149a787ff03e'
ESKANASY_UID  = 'bdb179bb-7053-547c-b1b9-8ebbebff8584'    # token "Eskanasy, Vlad"

GONZALEZ_W_PID = '6a2b2558-a3b2-54e1-8588-db62b8fcad77'
GONZALEZ_W_UID = '97f24ace-4a0b-5f37-9567-5024cb9fc97f'   # token "González, Wilder"

with open(IN_PT, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    pt_fieldnames = reader.fieldnames
    pt_rows = list(reader)

updated_rows = []
for row in pt_rows:
    if row['effective_person_id'] == KAPSZAK_PID:
        # Add new UUID to player_ids_seen
        existing_ids = row['player_ids_seen'].split(' | ')
        if KAPSZAK_NEW_UID not in existing_ids:
            existing_ids.append(KAPSZAK_NEW_UID)
            row['player_ids_seen'] = ' | '.join(existing_ids)
        # Add "Markus Kaspczak" to player_names_seen
        existing_names = [n.strip() for n in row['player_names_seen'].split(' | ')]
        if 'Markus Kaspczak' not in existing_names:
            existing_names.append('Markus Kaspczak')
            row['player_names_seen'] = ' | '.join(existing_names)
        # Add to aliases_presentable
        pres = [n.strip() for n in row['aliases_presentable'].split(' | ')] if row['aliases_presentable'] else []
        if 'Markus Kaspczak' not in pres:
            pres.append('Markus Kaspczak')
            row['aliases_presentable'] = ' | '.join(pres)
        row['notes'] = (row.get('notes','') + ' | 2026-03-07: added Kaspczak token from European Champs 2011').strip(' | ')
        print(f"  Updated: Markus Kapszak — added UUID {KAPSZAK_NEW_UID[:8]}")
    updated_rows.append(row)

# New person rows
new_persons = [
    {
        'effective_person_id': ESKANASY_PID,
        'person_canon':        'Vlad Eskanasy',
        'player_ids_seen':     ESKANASY_UID,
        'player_names_seen':   'Eskanasy, Vlad',
        'aliases':             '',
        'alias_statuses':      '',
        'notes':               '2026-03-07: new person, place 23 Swiss Open 2011 (Last,First fix)',
        'source':              'data_only',
        'person_canon_clean':  'Vlad Eskanasy',
        'person_canon_clean_reason': '',
        'aliases_presentable': '',
        'exclusion_reason':    '',
        'last_token':          'eskanasy',
        'norm_key':            '',
        'legacyid':            '',
    },
    {
        'effective_person_id': GONZALEZ_W_PID,
        'person_canon':        'Wilder González',
        'player_ids_seen':     GONZALEZ_W_UID,
        'player_names_seen':   'González, Wilder',
        'aliases':             '',
        'alias_statuses':      '',
        'notes':               '2026-03-07: new person, place 18 Copa Venezuela (Last,First fix)',
        'source':              'data_only',
        'person_canon_clean':  'Wilder González',
        'person_canon_clean_reason': '',
        'aliases_presentable': '',
        'exclusion_reason':    '',
        'last_token':          'gonzalez',
        'norm_key':            '',
        'legacyid':            '',
    },
]

for np in new_persons:
    updated_rows.append(np)
    print(f"  Added new person: {np['person_canon']}  ({np['effective_person_id'][:8]})")

with open(OUT_PT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=pt_fieldnames)
    writer.writeheader()
    writer.writerows(updated_rows)

print(f"  PT v32: {len(pt_rows)} rows → v33: {len(updated_rows)} rows (+2)")

# ── Step 2: Build lookup from PT v33 ─────────────────────────────────────────
pt_by_norm: dict[str, tuple[str, str]] = {}   # norm_name → (person_id, person_canon)
for row in updated_rows:
    n = norm(row['person_canon'])
    pt_by_norm[n] = (row['effective_person_id'], row['person_canon'])

# ── Step 3: Load PBP v36 ──────────────────────────────────────────────────────
print("\n=== PBP v36 → v37 ===")

with open(IN_PBP, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    pbp_fieldnames = reader.fieldnames
    pbp_rows = list(reader)

existing_keys = {(r['event_id'], r['division_canon'], r['place']) for r in pbp_rows}
print(f"  Loaded {len(pbp_rows):,} rows from v36")

# ── Step 4: Load stage2, find unresolved Last,First placements ────────────────
new_pbp: list[dict] = []
added = 0
skipped_already = 0
skipped_unresolved = 0

with open(STAGE2, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        eid = row['event_id']
        if eid not in AFFECTED_EVENTS:
            continue
        year = row.get('year', '')
        pj = json.loads(row.get('placements_json', '[]'))
        for p in pj:
            entry_raw = p.get('entry_raw', '')
            if not LAST_FIRST_RE.match(entry_raw):
                continue

            div_canon = p.get('division_canon', '')
            div_cat   = p.get('division_category', '')
            place     = str(p.get('place', ''))
            key       = (eid, div_canon, place)

            if key in existing_keys:
                skipped_already += 1
                continue

            # Check manual overrides first
            match = None
            if entry_raw in MANUAL_OVERRIDES:
                match = MANUAL_OVERRIDES[entry_raw]
            else:
                # Reconstruct "First Last" and look up in PT v33
                first_last = last_first_to_first_last(entry_raw)
                match = pt_by_norm.get(norm(first_last))

            if not match:
                print(f"  UNRESOLVED: {entry_raw!r} — no PT match")
                skipped_unresolved += 1
                continue

            person_id, person_canon = match
            new_pbp.append({
                'event_id':         eid,
                'year':             year,
                'division_canon':   div_canon,
                'division_category': div_cat,
                'place':            place,
                'competitor_type':  'player',
                'person_id':        person_id,
                'team_person_key':  '',
                'person_canon':     person_canon,
                'team_display_name': '',
                'coverage_flag':    'complete',
                'person_unresolved': '',
                'norm':             norm(person_canon),
            })
            existing_keys.add(key)
            added += 1

print(f"  Added: {added}  |  Already present: {skipped_already}  |  Unresolved: {skipped_unresolved}")

all_pbp = pbp_rows + sorted(
    new_pbp,
    key=lambda r: (r['event_id'], r['division_canon'], int(r['place']))
)

with open(OUT_PBP, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=pbp_fieldnames)
    writer.writeheader()
    writer.writerows(all_pbp)

print(f"  PBP v36: {len(pbp_rows):,} rows → v37: {len(all_pbp):,} rows (+{added})")
print("\nDone.")
