#!/usr/bin/env python3
"""
60_migrate_pbp_v59_to_v60.py

Add missing stage2 divisions to PBP for 12 "stale" events where the parser
has been improved since PBP was last generated.

Produces: inputs/identity_lock/Placements_ByPerson_v60.csv

Rules:
- Do NOT alter existing PBP rows
- New rows use person_unresolved='1', person_id=''
- person_canon = cleaned player name (NOT "__IDENTITY_PENDING__")
- Skip clearly noisy divisions and placements
- Rule 5: Consolidate same-name rows within (event_id, division_canon)
"""

import csv
import json
import re
import sys
from pathlib import Path

csv.field_size_limit(10**7)

ROOT = Path(__file__).parent.parent
STAGE2_CSV = ROOT / 'out' / 'stage2_canonical_events.csv'
PBP_V59 = ROOT / 'inputs' / 'identity_lock' / 'Placements_ByPerson_v59.csv'
PBP_V60 = ROOT / 'inputs' / 'identity_lock' / 'Placements_ByPerson_v60.csv'

STALE_EVENTS = {
    '1005667143', '1025084282', '1158263300', '1179679872', '1216058526',
    '1678957450', '859923755', '876591529', '886044392', '892446131',
    '910551956', '981560223',
}

# ---------------------------------------------------------------------------
# Skip criteria — division level
# ---------------------------------------------------------------------------

_RE_NUMERIC_DIV = re.compile(r'^\d+\s+(competitors|players|entries)\b', re.IGNORECASE)


def _norm_div(name: str) -> str:
    """Normalize division name for dedup comparison."""
    s = name.lower()
    s = re.sub(r'[^a-z0-9\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def should_skip_division(div_canon: str, existing_divs_norm: set, event_id: str) -> tuple[bool, str]:
    """Return (skip, reason) for a division."""
    if div_canon.startswith('**') or div_canon.startswith('*'):
        return True, 'round header (starts with *)'
    if div_canon == 'Unknown':
        return True, 'Unknown division'
    if _RE_NUMERIC_DIV.match(div_canon):
        return True, f'numeric competitors header ({div_canon!r})'
    norm = _norm_div(div_canon)
    if norm in existing_divs_norm:
        return True, f'already in PBP (normalized: {norm!r})'
    # Event-specific overrides
    if event_id == '910551956':
        return True, 'event 910551956 is all noise — skipping entire event'
    if event_id == '981560223':
        noisy = {
            _norm_div('I) Womens Open Frestyle'),
            _norm_div('Ii) Mens Open Freestyle'),
            _norm_div('* Advanced To Finals'),
            _norm_div('**Advanced To Swiss Finals'),
        }
        if norm in noisy:
            return True, f'981560223 known duplicate/noise division ({div_canon!r})'
    return False, ''


# ---------------------------------------------------------------------------
# Skip criteria — placement level
# ---------------------------------------------------------------------------

_RE_NOISE_NAME = re.compile(
    r'^\s*(results|sponsors|annual|tournament|championship|organiz|timed|minute|dew|open|footbag)\b',
    re.IGNORECASE,
)
_RE_SEED = re.compile(r'seed\s+partner', re.IGNORECASE)


def should_skip_placement(player1_name: str) -> tuple[bool, str]:
    """Return (skip, reason) for a placement."""
    name = player1_name.strip()
    if len(name) < 3:
        return True, f'name too short ({name!r})'
    if _RE_NOISE_NAME.match(name):
        return True, f'noise keyword at start ({name!r})'
    # starts with digit and has no letters after the digit
    if re.match(r'^\d', name) and not re.search(r'[A-Za-z]', name[1:]):
        return True, f'digit-only string ({name!r})'
    # ALL CAPS, >20 chars, no space — likely a header
    if name == name.upper() and len(name) > 20 and ' ' not in name:
        return True, f'ALL CAPS header ({name!r})'
    if _RE_SEED.search(name):
        return True, f'seed partner line ({name!r})'
    return False, ''


# ---------------------------------------------------------------------------
# Player name cleaning
# ---------------------------------------------------------------------------

_RE_SCORE_SUFFIX = re.compile(r'\s*\(?\d+\s*(pkt|pts|points?)\)?\s*$', re.IGNORECASE)
_RE_COUNTRY_PAREN = re.compile(r'\s*\([A-Z]{2,3}\)\s*$')
_RE_PAREN_MISC = re.compile(r'\s*\(\w+,?\s*\w*\)\s*$')


def _looks_like_nickname_paren(text: str) -> bool:
    """Return True if parenthetical content looks like a nickname, not a country/location."""
    # If it has spaces inside (like "the Footbagger") it's a nickname
    inner = text.strip('()')
    return ' ' in inner or len(inner) > 6


def clean_player_name(name: str) -> str:
    """Apply cleaning steps to a raw player name."""
    name = name.strip()

    # Step 1: Strip trailing score patterns
    name = _RE_SCORE_SUFFIX.sub('', name).strip()

    # Step 2 & 3: Strip trailing ", City" patterns
    # Only strip if the last comma-separated part is a single capitalized word
    if ',' in name:
        parts = name.rsplit(',', 1)
        last_part = parts[1].strip()
        # Single capitalized word (1-20 chars, no spaces in it that make it a full clause)
        # Don't strip if last part itself contains a comma (already stripped above)
        if re.match(r'^[A-Z][A-Za-zÀ-ÖØ-öø-ÿ.\-]{0,19}$', last_part):
            # Check there's no other clue it's a name part (like "Jr", "III")
            if last_part not in ('Jr', 'Sr', 'III', 'II', 'IV'):
                name = parts[0].strip()

    # Step 4: Strip trailing country code in parens (2-3 uppercase letters)
    # But be careful not to strip nickname parens
    m = _RE_COUNTRY_PAREN.search(name)
    if m:
        name = name[:m.start()].strip()
    else:
        # Try generic paren at end, but only if it doesn't look like a nickname
        m2 = _RE_PAREN_MISC.search(name)
        if m2:
            candidate = name[:m2.start()].strip()
            inner = name[m2.start():].strip()
            if not _looks_like_nickname_paren(inner):
                name = candidate

    # Step 5: Strip trailing whitespace
    name = name.strip()
    return name


# ---------------------------------------------------------------------------
# Division category inference
# ---------------------------------------------------------------------------

_FREESTYLE_KW = re.compile(
    r'\b(freestyle|routine|shred|sick|circle|contest|battle|ironman|combo|request)\b',
    re.IGNORECASE
)
_NET_KW = re.compile(r'\b(net|singles|doubles)\b', re.IGNORECASE)
_GOLF_KW = re.compile(r'\bgolf\b', re.IGNORECASE)


def infer_division_category(div_name: str, event_category_map: dict) -> str:
    """Infer division category from name or event context."""
    cat = event_category_map.get(_norm_div(div_name))
    if cat:
        return cat
    if _FREESTYLE_KW.search(div_name):
        return 'freestyle'
    if _GOLF_KW.search(div_name):
        return 'golf'
    if _NET_KW.search(div_name):
        return 'net'
    return 'freestyle'


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def load_pbp_v59() -> tuple[list[dict], dict[str, dict], dict]:
    """Load PBP v59.

    Returns:
        rows: all rows as list of dicts
        existing_by_event: {event_id -> {div_canon: list of rows}}
        fieldnames: ordered column names
    """
    rows = []
    existing_by_event: dict[str, dict] = {}
    fieldnames = None

    with open(PBP_V59, encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)
            eid = row['event_id']
            if eid in STALE_EVENTS:
                if eid not in existing_by_event:
                    existing_by_event[eid] = {}
                div = row['division_canon']
                existing_by_event[eid].setdefault(div, []).append(row)

    return rows, existing_by_event, fieldnames


def load_stage2() -> dict[str, dict]:
    """Load stage2 canonical events for the stale event IDs."""
    result = {}
    with open(STAGE2_CSV, encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['event_id'] in STALE_EVENTS:
                pj = json.loads(row['placements_json']) if row['placements_json'] else []
                result[row['event_id']] = {
                    'year': row['year'],
                    'event_name': row['event_name'],
                    'placements': pj,
                }
    return result


def build_event_category_map(existing_rows: list[dict]) -> dict:
    """Build {norm_div_name -> division_category} from existing PBP rows for an event."""
    cat_map = {}
    for row in existing_rows:
        norm = _norm_div(row['division_canon'])
        cat_map[norm] = row['division_category']
    return cat_map


def make_unresolved_row(
    event_id: str,
    year: str,
    div_canon: str,
    div_category: str,
    place: int,
    competitor_type: str,
    player1_name: str,
    player2_name: str,
) -> dict:
    """Construct a new unresolved PBP row."""
    if competitor_type == 'team':
        team_display = f"{player1_name} / {player2_name}" if player2_name else player1_name
        person_canon = '__NON_PERSON__'
        norm = team_display.lower()
        return {
            'event_id': event_id,
            'year': year,
            'division_canon': div_canon,
            'division_category': div_category,
            'place': str(place),
            'competitor_type': 'team',
            'person_id': '',
            'team_person_key': '',
            'person_canon': person_canon,
            'team_display_name': team_display,
            'coverage_flag': 'partial',
            'person_unresolved': '',
            'norm': norm,
        }
    else:
        cleaned = clean_player_name(player1_name)
        norm = cleaned.lower().strip()
        return {
            'event_id': event_id,
            'year': year,
            'division_canon': div_canon,
            'division_category': div_category,
            'place': str(place),
            'competitor_type': 'player',
            'person_id': '',
            'team_person_key': '',
            'person_canon': cleaned,
            'team_display_name': '',
            'coverage_flag': 'partial',
            'person_unresolved': '1',
            'norm': norm,
        }


def dedup_new_rows(rows: list[dict]) -> list[dict]:
    """Rule 5: Within (event_id, division_canon), deduplicate by normalized name + place."""
    seen: set[tuple] = set()
    result = []
    for row in rows:
        key = (
            row['event_id'],
            row['division_canon'],
            row['norm'],
            row['place'],
        )
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


def process_event(
    event_id: str,
    year: str,
    placements: list[dict],
    existing_by_event: dict,
) -> tuple[list[dict], list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Process one stale event.

    Returns:
        new_rows: rows to add to PBP
        skipped_divs: list of (div_canon, reason)
        added_divs: list of (div_canon, count)
    """
    existing_rows_for_event = []
    for div_rows in existing_by_event.get(event_id, {}).values():
        existing_rows_for_event.extend(div_rows)

    existing_divs_norm = {
        _norm_div(div) for div in existing_by_event.get(event_id, {})
    }

    cat_map = build_event_category_map(existing_rows_for_event)

    # Group placements by division_canon
    by_div: dict[str, list[dict]] = {}
    for p in placements:
        div = p.get('division_canon', 'Unknown')
        by_div.setdefault(div, []).append(p)

    new_rows: list[dict] = []
    skipped_divs: list[tuple[str, str]] = []
    added_divs: list[tuple[str, str]] = []

    for div_canon, div_placements in sorted(by_div.items()):
        skip_div, skip_reason = should_skip_division(div_canon, existing_divs_norm, event_id)
        if skip_div:
            skipped_divs.append((div_canon, skip_reason))
            continue

        div_category = infer_division_category(div_canon, cat_map)
        div_new_rows: list[dict] = []
        skip_count = 0

        for p in div_placements:
            player1 = p.get('player1_name', '').strip()
            player2 = p.get('player2_name', '').strip()
            place = p.get('place', 0)
            comp_type = p.get('competitor_type', 'player')

            skip_p, skip_p_reason = should_skip_placement(player1)
            if skip_p:
                skip_count += 1
                continue

            row = make_unresolved_row(
                event_id=event_id,
                year=year,
                div_canon=div_canon,
                div_category=div_category,
                place=int(place),
                competitor_type=comp_type,
                player1_name=player1,
                player2_name=player2,
            )
            div_new_rows.append(row)

        if div_new_rows:
            added_divs.append((div_canon, len(div_new_rows)))
            new_rows.extend(div_new_rows)
        else:
            skipped_divs.append((div_canon, f'all {skip_count} placements were noise'))

    return new_rows, skipped_divs, added_divs


def main() -> None:
    print('Loading PBP v59 ...')
    v59_rows, existing_by_event, fieldnames = load_pbp_v59()
    print(f'  {len(v59_rows):,} existing rows')

    print('Loading stage2 canonical events ...')
    stage2 = load_stage2()
    print(f'  {len(stage2)} stale events found in stage2')

    all_new_rows: list[dict] = []
    report: dict[str, dict] = {}

    for event_id in sorted(STALE_EVENTS):
        if event_id not in stage2:
            print(f'  WARNING: {event_id} not found in stage2 — skipping')
            continue

        ev = stage2[event_id]
        year = ev['year']
        event_name = ev['event_name']
        placements = ev['placements']

        new_rows, skipped_divs, added_divs = process_event(
            event_id, year, placements, existing_by_event
        )

        report[event_id] = {
            'event_name': event_name,
            'year': year,
            'new_rows': len(new_rows),
            'added_divs': added_divs,
            'skipped_divs': skipped_divs,
        }
        all_new_rows.extend(new_rows)

    # Rule 5 dedup across all new rows
    before_dedup = len(all_new_rows)
    all_new_rows = dedup_new_rows(all_new_rows)
    after_dedup = len(all_new_rows)
    if before_dedup != after_dedup:
        print(f'  Rule 5 dedup: {before_dedup} -> {after_dedup} rows ({before_dedup - after_dedup} removed)')

    # Sort new rows by event_id, division_canon, place
    all_new_rows.sort(key=lambda r: (r['event_id'], r['division_canon'], int(r['place'])))

    # Write v60
    print(f'\nWriting PBP v60 to {PBP_V60} ...')
    with open(PBP_V60, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(v59_rows)
        writer.writerows(all_new_rows)

    total_v60 = len(v59_rows) + len(all_new_rows)

    # ---------------------------------------------------------------------------
    # Report
    # ---------------------------------------------------------------------------
    print('\n' + '=' * 70)
    print('PER-EVENT REPORT')
    print('=' * 70)

    events_with_no_rows = []

    for event_id in sorted(STALE_EVENTS):
        if event_id not in report:
            events_with_no_rows.append(f'{event_id} (not in stage2)')
            continue

        r = report[event_id]
        event_label = f"{event_id}  {r['event_name']} ({r['year']})"
        print(f'\n{event_label}')
        print(f'  New rows added: {r["new_rows"]}')

        if r['added_divs']:
            print('  Divisions added:')
            for div, cnt in r['added_divs']:
                print(f'    + {div!r}: {cnt} rows')

        if r['skipped_divs']:
            print('  Divisions skipped:')
            for div, reason in r['skipped_divs']:
                print(f'    - {div!r}: {reason}')

        if r['new_rows'] == 0:
            events_with_no_rows.append(event_id)

    print('\n' + '=' * 70)
    print('SUMMARY')
    print('=' * 70)
    print(f'  v59 existing rows : {len(v59_rows):,}')
    print(f'  New rows added    : {len(all_new_rows):,}')
    print(f'  v60 total rows    : {total_v60:,}')

    if events_with_no_rows:
        print('\nEvents where NO rows were added (all divisions were noise):')
        for eid in events_with_no_rows:
            print(f'  {eid}')

    print(f'\nOutput: {PBP_V60}')


if __name__ == '__main__':
    main()
