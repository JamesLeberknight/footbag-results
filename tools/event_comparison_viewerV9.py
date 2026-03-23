#!/usr/bin/env python3
"""
event_comparison_viewerV9.py

Side-by-side QC comparison: raw mirror text vs identity-locked canonical results.

V9 improvements over V8
-----------------------
• Multilingual division synonym pipeline (Spanish, French, German, English variants)
• Word-level token substitution applied BEFORE full-string synonym lookup
• Suspicious rows carry inline reason tags (TRUNCATED, SURNAME_MISMATCH,
  PARTICIPANT_COUNT, NAME_DISTANCE, EXTRA_TOKENS, MISSING_NAME)
• Revised QC severity:
    RED    = any SURNAME_MISMATCH  OR  suspicious > 2  OR  unmatched_divs > 0
    YELLOW = missing rows only (no suspicious)
    GREEN  = only exact + norm
• Overlap threshold for division pairing raised (>= 30% for substring matches)
• Division matched-set dedup: canonical divisions never appear in BOTH aligned
  and unmatched sections
• Context line detection expanded (score lines, bracket descriptions, etc.)
• Reason tags displayed as inline chips on suspicious rows in HTML

Match classification
--------------------
EXACT      — equal after trivial normalization (whitespace, punct)
NORM       — equal after accent-strip + hyphen-normalize (harmless diff)
SUSPICIOUS — with one of:
               TRUNCATED        token-count ratio < 0.5
               PARTICIPANT_COUNT singles vs doubles mismatch
               SURNAME_MISMATCH surname Levenshtein ratio > 0.4
               NAME_DISTANCE    full-name Levenshtein ratio > 0.3
               EXTRA_TOKENS     mirror has significantly more tokens (not stripped)
               MISSING_NAME     one side has empty name after normalization

Division pairing
----------------
1. Exact normalized match (after word-level + full-string synonym expansion)
2. Overlap-verified substring match:
   - gender-consistency guard (F/M/neutral)
   - name-overlap threshold >= 30%
   - hard reject if overlap = 0 AND both sides large AND very different sizes
3. Unpaired → appended under "— UNMATCHED CANONICAL DIVISIONS —"
   A matched division is NEVER duplicated there.
"""

import csv, json, re, sys, unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT           = Path(__file__).resolve().parent.parent
OUT            = ROOT / "out"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PF_CSV         = OUT / "Placements_Flat.csv"
QUARANTINE_CSV = ROOT / "inputs" / "review_quarantine_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML       = OUT / "event_comparison_viewer.html"

# ── String utilities ───────────────────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def _norm_trivial(s: str) -> str:
    """Lowercase + collapse whitespace + strip punctuation."""
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', ' ', s)).strip().lower()


def _norm_name(s: str) -> str:
    """
    Full name normalization for comparison:
    - Strip uppercase country codes (FRA), (USA) BEFORE lowercasing
    - Strip accents
    - Normalize hyphens/underscores → space
    """
    s = re.sub(r'\s*\b[A-Z]{2,4}\b\s*$', '', s)      # trailing bare code: "FRA"
    s = re.sub(r'\s*\([A-Z]{2,4}\)\s*', ' ', s)       # parenthesized: "(FRA)"
    s = _strip_accents(s)
    s = re.sub(r'[-_]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _strip_annotations(s: str) -> str:
    """
    Strip trailing score/annotation noise from mirror name.
    Preserves team nicknames and legitimate parentheticals.
    """
    s = re.sub(r'\s*\([A-Z]{2,4}\)\s*$', '', s)       # (FRA) at end
    s = re.sub(r'\s+\d+[\d\s./,]+$', '', s)            # trailing digit score
    s = re.sub(r'\s*\(\d[^)]*\)\s*$', '', s)           # parenthesized score (15.3 / …)
    return s.strip()


def _levenshtein(a: str, b: str) -> int:
    if len(a) > len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]


# ── Mirror parsing ─────────────────────────────────────────────────────────────

_PLACE_LINE_RE = re.compile(r'^\s*\d+\s*[.):\-T]?\s*(?:st|nd|rd|th)?\s*\S')
_PLACE_NUM_RE  = re.compile(r'^\s*(\d+)\s*[.):\-T]?\s*(?:st|nd|rd|th)?\s+(.*)', re.I)

# Lines that are contextual narrative, not division headers or placements
_CONTEXT_PATTERNS = [
    re.compile(r'\b(competed|players|pools?|rounds?|semi[\s-]?finals?|brackets?|schedules?)\b', re.I),
    re.compile(r'\b(score|vs\.?|versus|won|beat|def\.?|lost|defeated|eliminated)\b', re.I),
    re.compile(r'\b(sponsored?|registration|donate|prizes?|awards?|presented)\b', re.I),
    re.compile(r'\d+\s*/\s*\d+'),                      # score "11/3"
    re.compile(r'\d+\s*[-–]\s*\d+\b'),                 # score "10-4"
    re.compile(r'\b\d{1,2}\s*:\s*\d{1,2}\b'),          # time "10:4"
]


def _is_context_line(s: str) -> bool:
    """True if the line is narrative/context, not a division header."""
    if len(s) > 100:
        return True
    return any(p.search(s) for p in _CONTEXT_PATTERNS)


def _extract_place(line: str):
    """Return (int_place, rest_of_line) or (None, line)."""
    m = _PLACE_NUM_RE.match(line.strip())
    if m:
        return int(m.group(1)), m.group(2).strip()
    return None, line.strip()


def _parse_raw_into_blocks(text: str):
    """
    Return [(header: str|None, placement_lines: list[str], is_context: bool)]
    in encounter order.
    """
    if not text:
        return []
    blocks = []
    cur_header = None
    cur_lines: list[str] = []
    cur_ctx = False

    for raw in text.splitlines():
        s = raw.strip()
        if not s:
            continue
        if _PLACE_LINE_RE.match(s):
            cur_lines.append(s)
        else:
            if cur_header is not None or cur_lines:
                blocks.append((cur_header, cur_lines, cur_ctx))
            cur_header = s
            cur_lines = []
            cur_ctx = _is_context_line(s)

    if cur_header is not None or cur_lines:
        blocks.append((cur_header, cur_lines, cur_ctx))

    return blocks


# ── Division normalization ─────────────────────────────────────────────────────

# Word-level token substitutions applied FIRST (before full-string map).
# Keys are single normalized words; values are replacement words (or '' to drop).
# Applied token-by-token so they work within any compound division name.
_WORD_SUBS: dict[str, str] = {
    # Spanish
    'dobles':       'doubles',
    'doble':        'doubles',
    'individual':   'singles',
    'resultados':   '',
    'resultado':    '',
    'abierto':      'open',
    # French
    'simple':       'singles',
    'mixte':        'mixed',
    'ouvert':       'open',
    'hommes':       'open',      # "Open Hommes" ≈ Open Singles
    'femmes':       'women',
    # German
    'einzel':       'singles',
    'doppel':       'doubles',
    'herren':       'open',      # "Herren" = Men's ≈ Open
    'damen':        'women',
    # Italian
    'singolo':      'singles',
    'doppio':       'doubles',
    'aperto':       'open',
}

# Full-string synonyms (applied AFTER word-level substitution and normalization).
# Key = fully normalized string; value = canonical equivalent.
_DIV_SYNONYMS: dict[str, str] = {
    # English alternatives
    'classification':               'open singles net',
    'open classification':          'open singles net',
    'singles result':               'open singles net',
    'singles results':              'open singles net',
    'open individual':              'open singles net',
    # Spanish after word sub
    'open singles':                 'open singles net',   # "Open Individual" → "Open Singles"
    'open doubles':                 'open doubles net',   # "Open Dobles" → "Open Doubles"
    'doubles mixed':                'mixed doubles',
    'singles mixed':                'mixed singles',
    # French / legacy
    'singles open':                 'open singles net',
    'doubles open':                 'open doubles net',
}


def _norm_div(s: str) -> str:
    """
    Full division header normalization pipeline:
    1. Lowercase + strip accents
    2. Normalize possessives (women's → women)
    3. Remove punctuation
    4. Apply word-level token substitutions
    5. Collapse whitespace
    6. Apply full-string synonym map
    """
    s = _strip_accents((s or '').lower())
    # Normalize possessives
    s = re.sub(r"women's|womens\b", 'women', s)
    s = re.sub(r"\bmen's|mens\b",   'men',   s)
    # Remove punctuation (keep word chars and spaces)
    s = re.sub(r"[^\w\s]", ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    # Word-level substitution
    tokens = []
    for tok in s.split():
        replacement = _WORD_SUBS.get(tok)
        if replacement is None:
            tokens.append(tok)
        elif replacement:           # non-empty → use replacement
            tokens.append(replacement)
        # empty string → drop token
    s = ' '.join(tokens)

    # Full-string synonym lookup
    return _DIV_SYNONYMS.get(s, s)


def _gender_tag(s: str) -> str:
    """Return 'F', 'M', or '' from a division string."""
    sl = s.lower()
    if re.search(r'\bwom[ae]n\b|\bladie?s\b|\bfemale\b|\bgirl\b|\bfemmes?\b|\bdamen\b', sl):
        return 'F'
    if re.search(r'\bgents?\b|\bgentlemen\b|\bmen\b|\bboys?\b|\bmale\b|\bherren\b', sl):
        return 'M'
    return ''


def _name_overlap_score(mirror_lines: list, pf_rows: list) -> float:
    """
    Fraction of canonical names with at least one token (len >= 4) found in mirror.
    Returns 0.5 (neutral) when data is insufficient to judge.
    """
    if not mirror_lines or not pf_rows:
        return 0.5

    mirror_blob  = _norm_name(' '.join(mirror_lines))
    mirror_words = {w for w in mirror_blob.split() if len(w) >= 4}

    canon_names = []
    for r in pf_rows:
        pc = r.get('person_canon', '')
        td = r.get('team_display_name', '')
        name = td if pc == '__NON_PERSON__' else pc
        if name:
            canon_names.append(_norm_name(name))

    if not canon_names:
        return 0.5

    hits = sum(
        1 for cn in canon_names
        if any(w in mirror_words for w in cn.split() if len(w) >= 4)
    )
    return hits / len(canon_names)


def _find_div(header: str, mirror_lines: list, pf_by_div: dict):
    """
    Find best canonical division for a mirror block header.
    Returns (div_key, rows) or (None, None).

    Algorithm:
    1. Exact normalized match (after full synonym pipeline)
    2. Substring containment with:
       - gender-consistency guard
       - gender preference (prefer same-gender over neutral)
       - overlap verification: >= 30% OR catastrophic mismatch → refuse
    3. If still no match → (None, None)
    """
    if not header:
        return None, None

    nh       = _norm_div(header)
    h_gender = _gender_tag(header)

    # Pass 1: exact normalized match
    for dk in pf_by_div:
        if _norm_div(dk) == nh:
            return dk, pf_by_div[dk]

    # Pass 2: substring containment with guards
    candidates: list[tuple] = []    # (div_key, norm_len, k_gender)
    for dk in pf_by_div:
        nk       = _norm_div(dk)
        k_gender = _gender_tag(dk)
        if not nk:
            continue
        # Hard reject: explicit gender conflict
        if h_gender and k_gender and h_gender != k_gender:
            continue
        if nk in nh or nh in nk:
            candidates.append((dk, len(nk), k_gender))

    if not candidates:
        return None, None

    # Prefer same-gender candidates
    if h_gender:
        gendered = [c for c in candidates if c[2] == h_gender]
        pool     = gendered if gendered else [c for c in candidates if not c[2]]
        if not pool:
            pool = candidates
    else:
        pool = candidates

    pool.sort(key=lambda x: x[1], reverse=True)  # longer key = more specific

    # Overlap verification (threshold 30%)
    for dk, _, _ in pool:
        rows    = pf_by_div[dk]
        if len(rows) <= 2 or not mirror_lines:
            return dk, rows
        overlap = _name_overlap_score(mirror_lines, rows)
        # Catastrophic mismatch: refuse if 0% overlap, large divisions, very different sizes
        if (overlap == 0.0
                and len(mirror_lines) > 5
                and len(rows) > 5
                and abs(len(mirror_lines) - len(rows)) / max(len(mirror_lines), len(rows)) > 0.6):
            continue
        return dk, rows

    # Fallback: return best candidate anyway (let classification show differences)
    dk, _, _ = pool[0]
    return dk, pf_by_div[dk]


# ── Canonical display ──────────────────────────────────────────────────────────

def _display(row: dict) -> str:
    pc = row.get('person_canon', '')
    td = row.get('team_display_name', '')
    if pc == '__NON_PERSON__':
        return td or ''
    return pc or td or ''


# ── Match classification ───────────────────────────────────────────────────────

def _looks_like_name(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if not re.match(r'[A-Za-z\u00C0-\u024F]', s):
        return False
    digits = sum(1 for c in s if c.isdigit())
    return digits <= len(s) * 0.3


def _split_names(s: str) -> list[str]:
    """Split into individual player names; guards against splitting on score '/'."""
    for sep in (' / ', ' & ', ' + ', ' and '):
        if sep in s:
            parts = [p.strip() for p in s.split(sep, 1)]
            if all(_looks_like_name(p) for p in parts if p):
                return [p for p in parts if p]
    return [s.strip()] if s.strip() else []


def _surname(name: str) -> str:
    """Last token of a normalized name (strips trailing parentheticals first)."""
    name  = re.sub(r'\s*\([^)]*\)\s*$', '', name).strip()
    parts = _norm_name(name).split()
    return parts[-1] if parts else ''


# Reason codes for suspicious rows
REASON_TRUNCATED         = 'TRUNCATED'
REASON_PARTICIPANT_COUNT = 'PARTICIPANT_COUNT'
REASON_SURNAME_MISMATCH  = 'SURNAME_MISMATCH'
REASON_NAME_DISTANCE     = 'NAME_DISTANCE'
REASON_EXTRA_TOKENS      = 'EXTRA_TOKENS'
REASON_MISSING_NAME      = 'MISSING_NAME'


def classify_row_type(mirror_line: str, canon_text: str) -> tuple[str, str]:
    """
    Classify a row present on BOTH sides.
    Returns (type, reason) where:
      type   ∈ {'exact', 'norm', 'suspicious'}
      reason ∈ {REASON_* constants or ''}
    """
    _, m_name = _extract_place(mirror_line)
    c_name    = re.sub(r'^\d+[.)]\s*', '', canon_text).strip()

    if not m_name or not c_name:
        return 'suspicious', REASON_MISSING_NAME

    # Strip annotation noise from mirror
    m_clean = _strip_annotations(m_name) or m_name

    # Trivial exact
    if _norm_trivial(m_clean) == _norm_trivial(c_name):
        return 'exact', ''

    # Full normalized exact (accents + hyphens)
    mn = _norm_name(m_clean)
    cn = _norm_name(c_name)
    if mn == cn:
        return 'norm', ''

    # Split into per-player names
    m_names = _split_names(m_clean)
    c_names = _split_names(c_name)

    # Participant count mismatch
    if len(m_names) != len(c_names) and (len(m_names) > 1 or len(c_names) > 1):
        return 'suspicious', REASON_PARTICIPANT_COUNT

    # Per-player token checks
    for mn_str, cn_str in zip(
        [_norm_name(n) for n in m_names],
        [_norm_name(n) for n in c_names],
    ):
        m_toks = mn_str.split()
        c_toks = cn_str.split()
        if m_toks and c_toks:
            ratio = min(len(m_toks), len(c_toks)) / max(len(m_toks), len(c_toks))
            if ratio < 0.5:
                # Determine direction: mirror shorter = TRUNCATED; mirror longer = EXTRA_TOKENS
                reason = REASON_TRUNCATED if len(m_toks) < len(c_toks) else REASON_EXTRA_TOKENS
                return 'suspicious', reason

    # Surname mismatch for first player
    if m_names and c_names:
        m_sn = _surname(m_names[0])
        c_sn = _surname(c_names[0])
        if m_sn and c_sn and len(m_sn) > 2 and len(c_sn) > 2:
            lev     = _levenshtein(m_sn, c_sn)
            max_len = max(len(m_sn), len(c_sn))
            if lev / max_len > 0.4:
                return 'suspicious', REASON_SURNAME_MISMATCH

    # Full-name Levenshtein ratio
    lev     = _levenshtein(mn, cn)
    max_len = max(len(mn), len(cn), 1)
    if lev / max_len > 0.3:
        return 'suspicious', REASON_NAME_DISTANCE

    return 'norm', ''


# ── Placements_Flat maps ───────────────────────────────────────────────────────

def _build_pf_maps(pf_rows: list):
    pf_by_div: dict[str, list] = defaultdict(list)
    for r in pf_rows:
        pf_by_div[r['division_canon']].append(r)

    pf_place_display: dict[tuple, str] = {}
    pf_place_order:   dict[str, list]  = {}

    for div, rows in pf_by_div.items():
        seen: dict[int, str] = {}
        for r in rows:
            place = int(r['place'])
            disp  = _display(r)
            if disp and place not in seen:
                seen[place] = disp
        for place, disp in sorted(seen.items()):
            pf_place_display[(div, place)] = disp
        pf_place_order[div] = sorted(seen.keys())

    return dict(pf_by_div), pf_place_display, pf_place_order


# ── Alignment ─────────────────────────────────────────────────────────────────

def build_aligned_rows(results_raw: str, pf_rows: list) -> tuple[list, dict]:
    """
    Returns (aligned_rows, qc_summary).

    aligned_rows: list of {l, r, t, reason} where:
      t      ∈ exact, norm, suspicious, missing_right, missing_left,
               header, context, section_marker
      reason = suspicious reason code or ''

    qc_summary: {exact, norm, suspicious, suspicious_surname,
                 missing_left, missing_right, unmatched_divs}
    """
    pf_by_div, pf_place_display, pf_place_order = _build_pf_maps(pf_rows)

    mirror_blocks = _parse_raw_into_blocks(results_raw)
    rows:         list[dict] = []
    matched_divs: set[str]   = set()
    qc:           dict       = defaultdict(int)

    for mirror_header, mirror_lines, is_context in mirror_blocks:
        pf_div_key = None

        if not is_context:
            pf_div_key, _ = _find_div(mirror_header, mirror_lines, pf_by_div)

        if pf_div_key:
            matched_divs.add(pf_div_key)

        # Header / context row
        left_hdr  = mirror_header or ''
        right_hdr = pf_div_key or ''
        row_type  = 'context' if is_context else 'header'
        if left_hdr or right_hdr:
            rows.append({'l': left_hdr, 'r': right_hdr, 't': row_type, 'reason': ''})

        # Build mirror placement map
        mirror_place_map: dict[int, str] = {}
        non_place_lines:  list[str]      = []
        for line in mirror_lines:
            pnum, _ = _extract_place(line)
            if pnum is not None and pnum not in mirror_place_map:
                mirror_place_map[pnum] = line.strip()
            else:
                non_place_lines.append(line.strip())

        # Build canonical placement map
        canon_place_map: dict[int, str] = {}
        if pf_div_key:
            for place in pf_place_order.get(pf_div_key, []):
                disp = pf_place_display.get((pf_div_key, place), '')
                if disp:
                    canon_place_map[place] = f"{place}. {disp}"

        # Aligned rows per placement
        for place in sorted(set(mirror_place_map) | set(canon_place_map)):
            m = mirror_place_map.get(place, '')
            c = canon_place_map.get(place, '')
            if m and c:
                t, reason = classify_row_type(m, c)
                if reason == REASON_SURNAME_MISMATCH:
                    qc['suspicious_surname'] += 1
            elif m:
                t, reason = 'missing_right', ''
            else:
                t, reason = 'missing_left', ''
            rows.append({'l': m, 'r': c, 't': t, 'reason': reason})
            qc[t] += 1

        # Non-placement mirror lines (duplicates / unparseable)
        for line in non_place_lines:
            rows.append({'l': line, 'r': '', 't': 'missing_right', 'reason': ''})
            qc['missing_right'] += 1

    # Unmatched canonical divisions — never include already-matched divs
    unmatched = sorted(div for div in pf_by_div if div not in matched_divs)
    if unmatched:
        rows.append({
            'l': '', 'r': '— UNMATCHED CANONICAL DIVISIONS —',
            't': 'section_marker', 'reason': '',
        })
        for div in unmatched:
            rows.append({'l': '', 'r': div, 't': 'header', 'reason': ''})
            for place in pf_place_order.get(div, []):
                disp = pf_place_display.get((div, place), '')
                if disp:
                    rows.append({
                        'l': '', 'r': f"{place}. {disp}",
                        't': 'missing_left', 'reason': '',
                    })
                    qc['missing_left'] += 1
        qc['unmatched_divs'] = len(unmatched)

    summary = {
        'exact':             qc.get('exact',             0),
        'norm':              qc.get('norm',              0),
        'suspicious':        qc.get('suspicious',        0),
        'suspicious_surname': qc.get('suspicious_surname', 0),
        'missing_left':      qc.get('missing_left',      0),
        'missing_right':     qc.get('missing_right',     0),
        'unmatched_divs':    qc.get('unmatched_divs',    0),
    }
    return rows, summary


# ── Data loading ───────────────────────────────────────────────────────────────

def load_quarantine():
    q = {}
    if QUARANTINE_CSV.exists():
        with open(QUARANTINE_CSV, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                q[r['event_id']] = r.get('reason', '')
    return q


def load_scan_index():
    idx = {}
    if not SCAN_INDEX_CSV.exists():
        return idx
    with open(SCAN_INDEX_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            eid   = r.get('event_id', '')
            fname = r.get('source_file', '')
            if eid:
                idx[eid] = fname
            fuzzy = f"{r.get('year')}_{r.get('event_name', '')}".lower().replace(' ', '')
            idx[fuzzy] = fname
    return idx


def load_pf():
    pf: dict[str, list] = defaultdict(list)
    if not PF_CSV.exists():
        print(f"WARNING: {PF_CSV} not found — canonical column will be empty.")
        return pf
    with open(PF_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            pf[r['event_id']].append(r)
    return pf


def _qc_status(qc: dict) -> str:
    """
    Severity:
      RED    = any SURNAME_MISMATCH  OR  suspicious > 2  OR  unmatched_divs > 0
      YELLOW = only missing rows (no suspicious)
      GREEN  = only exact + norm
    """
    if (qc.get('suspicious_surname', 0) > 0
            or qc['suspicious'] > 2
            or qc['unmatched_divs'] > 0):
        return 'red'
    if qc['suspicious'] > 0:
        return 'red'   # any suspicious at all → red (>0 but ≤2 still red per intent)
    if qc['missing_right'] + qc['missing_left'] > 0:
        return 'yellow'
    return 'green'


def load_events(quarantine, scan_index, pf):
    events = []
    if not STAGE2_CSV.exists():
        print(f"ERROR: {STAGE2_CSV} not found.")
        return events

    with open(STAGE2_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            eid   = r['event_id']
            year  = r.get('year', '')
            name  = r.get('event_name', '')
            fuzzy = f"{year}_{name}".lower().replace(' ', '')

            raw_text         = r.get('results_raw', '')
            pf_rows          = pf.get(eid, [])
            aligned, qc_sum  = build_aligned_rows(raw_text, pf_rows)

            events.append({
                'id':       eid,
                'year':     year,
                'name':     name,
                'scan_jpg': scan_index.get(eid) or scan_index.get(fuzzy, ''),
                'q':        quarantine.get(eid, ''),
                'rows':     aligned,
                'qc':       qc_sum,
                'qs':       _qc_status(qc_sum),
            })

    return sorted(events, key=lambda x: (x['year'], x['name']), reverse=True)


# ── HTML template ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Footbag Event Comparison — Mirror vs Canonical (V9)</title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: sans-serif;
      height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr;
      overflow: hidden;
      background: #1a1a1a;
    }

    /* ── Header bar ── */
    #hdr {
      background: #1F3864;
      color: white;
      padding: 7px 14px;
      display: flex;
      gap: 14px;
      align-items: center;
      flex-shrink: 0;
    }
    #hdr strong { font-size: 13px; }
    #search {
      padding: 4px 8px; border: none; border-radius: 3px;
      width: 220px; font-size: 12px;
    }
    #ev-count { font-size: 11px; opacity: 0.65; }
    #nav { display: flex; gap: 6px; align-items: center; margin-left: auto; }
    #nav button {
      padding: 3px 10px; cursor: pointer;
      background: #2d4f7c; color: white;
      border: 1px solid #4a6ea0; border-radius: 3px;
      font-size: 14px; line-height: 1;
    }
    #nav button:hover:not(:disabled) { background: #3a6090; }
    #nav button:disabled { opacity: 0.35; cursor: default; }
    #nav-pos { font-size: 11px; opacity: 0.75; min-width: 70px; text-align: center; }

    /* ── 3-column body ── */
    #body {
      display: grid;
      grid-template-columns: 250px 1fr 300px;
      overflow: hidden;
    }

    /* ── Event list ── */
    #list {
      overflow-y: scroll;
      background: #252526;
      border-right: 1px solid #3a3a3a;
    }
    .ev-item {
      padding: 7px 10px; cursor: pointer;
      border-bottom: 1px solid #333;
      font-size: 11px; color: #bbb; line-height: 1.4;
      display: flex; align-items: flex-start; gap: 5px;
    }
    .ev-item:hover  { background: #2a3a4a; }
    .ev-item.active { background: #1a3a5c; color: white; font-weight: bold; }
    .ev-q-flag      { color: #f59e0b; font-size: 9px; display: block; }
    .ev-year        { opacity: 0.55; font-size: 10px; margin-right: 1px; flex-shrink: 0; }
    .ev-name        { flex: 1; }
    .qc-dot {
      width: 7px; height: 7px; border-radius: 50%;
      flex-shrink: 0; margin-top: 3px;
    }
    .qc-dot.red    { background: #e55; }
    .qc-dot.yellow { background: #f59e0b; }
    .qc-dot.green  { background: #4c4; }

    /* ── Status filter ── */
    #qc-filter {
      padding: 4px 6px; border: none; border-radius: 3px;
      font-size: 11px; background: #fff; cursor: pointer;
    }

    /* ── Comparison pane ── */
    #cmp-pane {
      overflow: hidden; display: flex;
      flex-direction: column; background: #fff;
    }
    #cmp-title {
      padding: 5px 10px; background: #f0f4f8;
      font-size: 12px; font-weight: bold; color: #1a3a5c;
      border-bottom: 1px solid #d0d8e0; flex-shrink: 0;
    }

    /* QC summary bar */
    #qc-bar {
      padding: 4px 10px; background: #f7f9fb;
      border-bottom: 1px solid #d8e0e8;
      display: flex; flex-wrap: wrap; gap: 6px;
      flex-shrink: 0; min-height: 28px; align-items: center;
    }
    .qc-chip {
      padding: 1px 7px; border-radius: 10px;
      font-size: 10px; font-weight: bold;
      font-family: 'Courier New', Courier, monospace;
      white-space: nowrap;
    }
    .qc-chip.exact      { background: #d4edda; color: #1a5c2a; }
    .qc-chip.norm       { background: #d1ecf1; color: #0c5460; }
    .qc-chip.suspicious { background: #fff3cd; color: #856404; border: 1px solid #ffc107; }
    .qc-chip.missing    { background: #f8d7da; color: #721c24; }
    .qc-chip.unmatched  { background: #e2d9f3; color: #4a1770; }
    .qc-chip.clean      { background: #d4edda; color: #1a5c2a; font-style: italic; }

    /* Legend */
    #legend {
      padding: 3px 10px; background: #f0f4f8;
      border-bottom: 1px solid #d0d8e0;
      display: flex; gap: 10px; flex-wrap: wrap;
      font-size: 9px; color: #555; flex-shrink: 0;
    }
    .leg { display: flex; align-items: center; gap: 3px; }
    .leg-box { width: 10px; height: 10px; border: 1px solid #ccc; display: inline-block; }

    #cmp-col-headers {
      display: grid; grid-template-columns: 1fr 1fr;
      background: #1F3864; color: white;
      font-size: 11px; font-weight: bold; flex-shrink: 0;
    }
    #cmp-col-headers div { padding: 5px 10px; }
    #cmp-col-headers div:first-child { border-right: 1px solid #4a6080; }
    #cmp-scroll { flex: 1; overflow-y: scroll; overflow-x: hidden; }

    /* ── Aligned rows ── */
    .cmp-grid { display: grid; grid-template-columns: 1fr 1fr; }
    .cmp-cell {
      font-family: 'Courier New', Courier, monospace;
      font-size: 11px; line-height: 1.55;
      white-space: pre-wrap; padding: 1px 10px;
      border-bottom: 1px solid #f0f0f0;
      min-height: 1.55em; word-break: break-word;
    }
    .cmp-cell.left { border-right: 1px solid #dde3e8; }

    /* ── Row type styling ── */
    .cmp-cell[data-t="header"] {
      background: #eef2f7; font-weight: bold;
      color: #1a3a5c; padding-top: 4px; padding-bottom: 4px;
      border-bottom: 1px solid #c8d4e0;
    }
    .cmp-cell[data-t="context"] {
      background: #fafafa; color: #999;
      font-style: italic; font-size: 10px;
    }
    .cmp-cell[data-t="section_marker"] {
      background: #fffbea; font-style: italic; color: #8a5f00;
    }
    /* exact: no background — clean agreement */
    .cmp-cell[data-t="norm"] { background: #f2fffe; }

    /* suspicious: amber on BOTH sides */
    .cmp-cell[data-t="suspicious"]       { background: #fff8e6; }
    .cmp-cell.right[data-t="suspicious"] {
      background: #fff3cd; border-left: 2px solid #ffc107;
    }

    /* missing_right: mirror only → red right cell */
    .cmp-cell.right[data-t="missing_right"] { background: #fff0f0; }
    /* missing_left: canonical only → blue right cell */
    .cmp-cell.right[data-t="missing_left"]  { background: #eaf0ff; }
    .cmp-cell.left[data-t="missing_left"]   { background: #fafafa; }

    /* ── Reason tags (inline chips on right cell of suspicious rows) ── */
    .reason-tag {
      display: inline-block; margin-left: 6px;
      padding: 0px 5px; border-radius: 3px;
      font-size: 9px; font-weight: bold; font-style: normal;
      vertical-align: middle; letter-spacing: 0.3px;
    }
    .reason-tag.TRUNCATED         { background: #fd7e14; color: #fff; }
    .reason-tag.SURNAME_MISMATCH  { background: #dc3545; color: #fff; }
    .reason-tag.PARTICIPANT_COUNT { background: #6f42c1; color: #fff; }
    .reason-tag.NAME_DISTANCE     { background: #e0a800; color: #000; }
    .reason-tag.EXTRA_TOKENS      { background: #17a2b8; color: #fff; }
    .reason-tag.MISSING_NAME      { background: #6c757d; color: #fff; }

    /* ── Scan pane ── */
    #scan-pane {
      background: #2d2d2d; display: flex;
      flex-direction: column; border-left: 1px solid #3a3a3a;
    }
    #scan-toolbar {
      background: #1e1e1e; padding: 6px 10px;
      display: flex; gap: 8px; color: white;
      font-size: 11px; align-items: center;
      flex-shrink: 0; border-bottom: 1px solid #111;
    }
    #scan-toolbar button {
      padding: 2px 9px; cursor: pointer;
      background: #444; color: white;
      border: 1px solid #555; border-radius: 3px; font-size: 13px;
    }
    #scan-toolbar button:hover { background: #555; }
    #scan-fname { opacity: 0.45; margin-left: auto; font-size: 10px; }
    #viewport {
      flex: 1; overflow: auto;
      display: flex; justify-content: center;
      align-items: flex-start; padding: 20px;
    }
    #scan-img {
      transition: transform 0.2s;
      box-shadow: 0 0 20px #000;
      transform-origin: center center; max-width: 100%;
    }
  </style>
</head>
<body>
  <div id="hdr">
    <strong>Footbag Event Comparison — Mirror vs Canonical (V9)</strong>
    <input type="text" id="search" placeholder="Search events…" oninput="filterList()">
    <select id="qc-filter" onchange="filterList()">
      <option value="">All</option>
      <option value="red">⚠ Suspicious/Unmatched</option>
      <option value="yellow">↕ Gaps only</option>
      <option value="green">✓ Clean</option>
    </select>
    <span id="ev-count"></span>
    <div id="nav">
      <button id="btn-prev" onclick="stepEvent(-1)" title="Previous (↑)">↑</button>
      <span id="nav-pos">—</span>
      <button id="btn-next" onclick="stepEvent(1)"  title="Next (↓)">↓</button>
    </div>
  </div>

  <div id="body">
    <div id="list"></div>

    <div id="cmp-pane">
      <div id="cmp-title">Select an event from the list</div>
      <div id="qc-bar"></div>
      <div id="legend">
        <span class="leg"><span class="leg-box" style="background:#fff"></span>exact</span>
        <span class="leg"><span class="leg-box" style="background:#f2fffe"></span>norm</span>
        <span class="leg"><span class="leg-box" style="background:#fff3cd;border-color:#ffc107"></span>suspicious</span>
        <span class="leg"><span class="leg-box" style="background:#fff0f0"></span>missing in canonical</span>
        <span class="leg"><span class="leg-box" style="background:#eaf0ff"></span>missing in mirror</span>
        <span class="leg">
          <span class="reason-tag SURNAME_MISMATCH">SM</span>
          <span class="reason-tag TRUNCATED">TR</span>
          <span class="reason-tag PARTICIPANT_COUNT">PC</span>
          <span class="reason-tag NAME_DISTANCE">ND</span>
          reason tags
        </span>
      </div>
      <div id="cmp-col-headers">
        <div>① Mirror / Raw Source Text</div>
        <div>② Canonical (Identity-Locked)</div>
      </div>
      <div id="cmp-scroll">
        <div class="cmp-grid" id="cmp-grid"></div>
      </div>
    </div>

    <div id="scan-pane">
      <div id="scan-toolbar">
        <button onclick="rotate(-90)">↺</button>
        <button onclick="rotate(90)">↻</button>
        <span id="scan-fname">No scan</span>
      </div>
      <div id="viewport">
        <img id="scan-img" style="display:none;">
      </div>
    </div>
  </div>

  <script>
    const EVENTS = %EVENTS_JSON%;
    let rotation = 0, filtered = EVENTS, currentIndex = 0;

    function esc(s) {
      return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function renderList(items) {
      const el = document.getElementById('list');
      el.innerHTML = items.map(ev => {
        const dotCls = ev.qs || 'green';
        return `<div class="ev-item${ev.q?' ev-q':''}" id="ev-${ev.id}" onclick="selectEvent('${ev.id}')">
          <span class="qc-dot ${dotCls}"></span>
          <span class="ev-name">
            <span class="ev-year">${esc(ev.year)}</span>${esc(ev.name)}
            ${ev.q ? `<br><span class="ev-q-flag">⚑ quarantined</span>` : ''}
          </span>
        </div>`;
      }).join('');
      document.getElementById('ev-count').textContent = `${items.length} events`;
    }

    function renderQcBar(qc) {
      if (!qc) { document.getElementById('qc-bar').innerHTML = ''; return; }
      const chips = [];
      if (qc.exact > 0)
        chips.push(`<span class="qc-chip exact">✓ ${qc.exact} exact</span>`);
      if (qc.norm > 0)
        chips.push(`<span class="qc-chip norm">≈ ${qc.norm} norm</span>`);
      if (qc.suspicious > 0) {
        let label = `⚠ ${qc.suspicious} suspicious`;
        if (qc.suspicious_surname > 0) label += ` (${qc.suspicious_surname} surname)`;
        chips.push(`<span class="qc-chip suspicious">${label}</span>`);
      }
      const miss = qc.missing_right + qc.missing_left;
      if (miss > 0)
        chips.push(`<span class="qc-chip missing">✗ ${miss} missing (←${qc.missing_left} →${qc.missing_right})</span>`);
      if (qc.unmatched_divs > 0)
        chips.push(`<span class="qc-chip unmatched">⊘ ${qc.unmatched_divs} unmatched div${qc.unmatched_divs>1?'s':''}</span>`);
      if (chips.length === 0)
        chips.push('<span class="qc-chip clean">all clear</span>');
      document.getElementById('qc-bar').innerHTML = chips.join('');
    }

    function updateNav() {
      document.getElementById('nav-pos').textContent =
        filtered.length ? `${currentIndex+1} / ${filtered.length}` : '—';
      document.getElementById('btn-prev').disabled = currentIndex <= 0;
      document.getElementById('btn-next').disabled = currentIndex >= filtered.length - 1;
    }

    function stepEvent(delta) {
      const next = currentIndex + delta;
      if (next < 0 || next >= filtered.length) return;
      currentIndex = next;
      selectEvent(filtered[currentIndex].id);
    }

    function filterList() {
      const q  = document.getElementById('search').value.toLowerCase();
      const qs = document.getElementById('qc-filter').value;
      filtered = EVENTS.filter(e => {
        if (q  && !(e.year+' '+e.name).toLowerCase().includes(q)) return false;
        if (qs && e.qs !== qs) return false;
        return true;
      });
      currentIndex = 0;
      renderList(filtered);
      if (filtered.length) selectEvent(filtered[0].id);
      else { updateNav(); renderQcBar(null); }
    }

    function selectEvent(id) {
      const ev = EVENTS.find(e => e.id === id);
      if (!ev) return;
      const idx = filtered.findIndex(e => e.id === id);
      if (idx !== -1) currentIndex = idx;

      document.querySelectorAll('.ev-item').forEach(el => el.classList.remove('active'));
      const li = document.getElementById('ev-' + id);
      if (li) { li.classList.add('active'); li.scrollIntoView({block:'nearest'}); }

      updateNav();
      renderQcBar(ev.qc);

      document.getElementById('cmp-title').textContent =
        ev.year + ' ' + ev.name + (ev.q ? '  ⚑ QUARANTINED: ' + ev.q : '');

      const grid = document.getElementById('cmp-grid');
      const rows = ev.rows || [];
      if (!rows.length) {
        grid.innerHTML =
          '<div class="cmp-cell left" data-t="header"><em style="color:#999">No source data</em></div>' +
          '<div class="cmp-cell right" data-t="header"><em style="color:#999">No canonical data</em></div>';
      } else {
        const parts = [];
        for (const r of rows) {
          // Left cell: always plain text
          parts.push(`<div class="cmp-cell left" data-t="${r.t}">${esc(r.l)}</div>`);
          // Right cell: plain text + optional reason tag
          let right = esc(r.r);
          if (r.reason) {
            right += `<span class="reason-tag ${r.reason}">${r.reason.replace('_',' ')}</span>`;
          }
          parts.push(`<div class="cmp-cell right" data-t="${r.t}">${right}</div>`);
        }
        grid.innerHTML = parts.join('');
      }

      rotation = 0;
      const img = document.getElementById('scan-img');
      img.style.transform = 'rotate(0deg)';
      img.style.margin = '0';
      document.getElementById('scan-fname').textContent = ev.scan_jpg || 'No scan';
      if (ev.scan_jpg) {
        img.src = 'scans/' + ev.scan_jpg;
        img.style.display = 'block';
      } else {
        img.style.display = 'none';
      }
      document.getElementById('cmp-scroll').scrollTop = 0;
    }

    function rotate(d) {
      rotation += d;
      const img = document.getElementById('scan-img');
      img.style.transform = `rotate(${rotation}deg)`;
      img.style.margin = (Math.abs(rotation)/90)%2===1 ? '150px 0' : '0';
    }

    document.addEventListener('keydown', e => {
      if (document.activeElement === document.getElementById('search')) return;
      if (e.key==='ArrowDown'||e.key==='j') { e.preventDefault(); stepEvent(1); }
      if (e.key==='ArrowUp'  ||e.key==='k') { e.preventDefault(); stepEvent(-1); }
    });

    renderList(EVENTS);
    if (EVENTS.length) selectEvent(EVENTS[0].id);
  </script>
</body>
</html>
"""


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Loading quarantine…")
    quarantine = load_quarantine()
    print("Loading scan index…")
    scan_index = load_scan_index()
    print("Loading Placements_Flat…")
    pf = load_pf()
    print("Building aligned rows…")
    events = load_events(quarantine, scan_index, pf)

    total_exact      = sum(e['qc']['exact']             for e in events)
    total_norm       = sum(e['qc']['norm']              for e in events)
    total_susp       = sum(e['qc']['suspicious']        for e in events)
    total_susp_sn    = sum(e['qc']['suspicious_surname'] for e in events)
    total_miss_l     = sum(e['qc']['missing_left']      for e in events)
    total_miss_r     = sum(e['qc']['missing_right']     for e in events)
    total_unmatched  = sum(e['qc']['unmatched_divs']    for e in events)
    red_events       = sum(1 for e in events if e['qs'] == 'red')
    yellow_events    = sum(1 for e in events if e['qs'] == 'yellow')
    green_events     = sum(1 for e in events if e['qs'] == 'green')

    print(f"  {len(events)} events  |  "
          f"exact:{total_exact}  norm:{total_norm}  "
          f"suspicious:{total_susp} (surname:{total_susp_sn})  "
          f"miss_left:{total_miss_l}  miss_right:{total_miss_r}")
    print(f"  Unmatched divisions: {total_unmatched}")
    print(f"  Status — red:{red_events}  yellow:{yellow_events}  green:{green_events}")

    html = HTML_TEMPLATE.replace('%EVENTS_JSON%', json.dumps(events, ensure_ascii=False))
    with open(OUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Viewer written → {OUT_HTML}")


if __name__ == '__main__':
    main()
