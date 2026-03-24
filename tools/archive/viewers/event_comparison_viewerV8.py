#!/usr/bin/env python3
"""
event_comparison_viewerV8.py

Side-by-side comparison viewer: raw mirror text (column 1) vs identity-locked
canonical results (column 2), with explicit line-for-line alignment.

V8 improvements over V7
-----------------------
• EXACT / NORMALIZED / SUSPICIOUS match classification (replaces generic "match")
• Division matching: gender-consistency guard, synonym expansion, overlap verification
• Context line detection: narrative/score text rendered muted, not as division headers
• Event-level QC summary banner per event
• QC status indicator in event list (red = has suspicious/unmatched, yellow = gaps)
• Alignment safety check: catastrophically low-overlap division pairs flagged
• Row types: exact, norm, suspicious, missing_right, missing_left, header,
             context, section_marker

Match classification rules
---------------------------
EXACT      — strings equal after trivial normalization (whitespace, punctuation)
NORM       — equal after also stripping accents, normalizing hyphens/slashes
SUSPICIOUS — any of:
             • token count ratio < 0.5 (truncation)
             • participant count mismatch (singles vs doubles)
             • surname Levenshtein ratio > 0.4
             • full name Levenshtein ratio > 0.3

Division pairing rules
-----------------------
1. Exact normalized match (+ synonym table)
2. Substring containment, with:
   - gender-consistency guard (women vs men vs neutral)
   - preference for same-gender canonical divisions
   - participant-overlap verification (>= 15% token hit-rate)
3. If no match → division left unmatched (appended at end)
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
    """Full name normalization: strip accents, country codes, normalize hyphens."""
    # Strip country codes BEFORE lowercasing (they're uppercase in mirror)
    s = re.sub(r'\s*\b[A-Z]{2,4}\b\s*$', '', s)        # trailing "FRA", "USA" etc
    s = re.sub(r'\s*\([A-Z]{2,4}\)\s*', ' ', s)         # parenthesized "(FRA)"
    s = _strip_accents(s)
    s = re.sub(r'[-_]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def _strip_annotations(s: str) -> str:
    """
    Strip trailing score/annotation noise from a mirror name string.
    Preserves legitimate name content (team nicknames, etc.).
    """
    # Parenthesized country codes: (FRA), (CAN) — uppercase 2-4 letters
    s = re.sub(r'\s*\([A-Z]{2,4}\)\s*$', '', s)
    # Score-like trailing content: digits / digits (after a space)
    s = re.sub(r'\s+\d+[\d\s./,]+$', '', s)
    # Parenthesized score annotations: (15.3 / 14.2 ...) — starts with digit
    s = re.sub(r'\s*\(\d[^)]*\)\s*$', '', s)
    return s.strip()


def _levenshtein(a: str, b: str) -> int:
    """Standard Levenshtein edit distance."""
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

# Context-line patterns: lines that are narrative/scores, not division names
_CONTEXT_PATTERNS = [
    re.compile(r'\b(competed|players|pools?|rounds?|semi[\s-]?finals?|brackets?|schedules?)\b', re.I),
    re.compile(r'\b(score|vs\.?|versus|won|beat|def\.?|lost|defeated|eliminated)\b', re.I),
    re.compile(r'\b(sponsored?|registration|donate|prizes?|awards?|presented)\b', re.I),
    re.compile(r'\d+\s*/\s*\d+'),      # score like "11/3"
]


def _is_context_line(s: str) -> bool:
    """Return True if the line is narrative/contextual, not a division header."""
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
    Return list of (header: str|None, placement_lines: list[str], is_context: bool)
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


# ── Division matching ──────────────────────────────────────────────────────────

# Mirror header synonyms → normalized canonical form.
# Keys are _norm_div() outputs; values are what we substitute before matching.
_DIV_SYNONYMS: dict[str, str] = {
    'classification':              'open singles net',
    'open classification':         'open singles net',
    'singles result':              'open singles net',
    'singles results':             'open singles net',
    'resultados open individual':  'open singles net',
    'resultado open individual':   'open singles net',
    'open individual':             'open singles net',
    'simple ouvert':               'open singles net',
    'dobles abierto':              'open doubles net',
    'doble abierto':               'open doubles net',
    'doubles ouvert':              'open doubles net',
    'double ouvert':               'open doubles net',
}


def _norm_div(s: str) -> str:
    """
    Normalize a division string for matching.
    - Strip accents
    - Normalize possessives / plural gender forms
    - Remove punctuation
    - Consult synonym map
    """
    s = _strip_accents((s or '').lower())
    s = re.sub(r"women's|womens\b", 'women', s)
    s = re.sub(r"\bmen's|mens\b",   'men',   s)
    s = re.sub(r"[^\w\s]", ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return _DIV_SYNONYMS.get(s, s)


def _gender_tag(s: str) -> str:
    """Return 'F', 'M', or '' for the gender specificity of a division name."""
    sl = s.lower()
    if re.search(r'\bwom[ae]n\b|\bladie?s\b|\bfemale\b|\bgirl\b', sl):
        return 'F'
    if re.search(r'\bgents?\b|\bgentlemen\b|\bmen\b|\bboys?\b|\bmale\b', sl):
        return 'M'
    return ''


def _name_overlap_score(mirror_lines: list, pf_rows: list) -> float:
    """
    Fraction of canonical names with at least one token (len>=4) match in mirror.
    Returns 0.5 when insufficient data to judge.
    """
    if not mirror_lines or not pf_rows:
        return 0.5

    mirror_blob = _norm_name(' '.join(mirror_lines))
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
    Find the best canonical division for a mirror block header.
    Returns (div_key, rows) or (None, None).

    Algorithm:
    1. Exact normalized match (after synonym expansion)
    2. Substring containment:
       a. Respect gender: prefer same-gender canonical; reject explicit gender conflict
       b. Verify via name overlap (safety net for catastrophic mispairing)
    """
    if not header:
        return None, None

    nh = _norm_div(header)
    h_gender = _gender_tag(header)

    # Pass 1: exact
    for dk in pf_by_div:
        if _norm_div(dk) == nh:
            return dk, pf_by_div[dk]

    # Pass 2: substring with gender guard
    candidates: list[tuple] = []   # (div_key, norm_len, k_gender)
    for dk in pf_by_div:
        nk = _norm_div(dk)
        if not nk:
            continue
        k_gender = _gender_tag(dk)

        # Hard reject: explicit gender conflict
        if h_gender and k_gender and h_gender != k_gender:
            continue

        if nk in nh or nh in nk:
            candidates.append((dk, len(nk), k_gender))

    if not candidates:
        return None, None

    # Prefer same-gender candidates when header is gendered
    if h_gender:
        gendered = [c for c in candidates if c[2] == h_gender]
        pool = gendered if gendered else [c for c in candidates if not c[2]]
        if not pool:
            pool = candidates
    else:
        pool = candidates

    # Sort by specificity (longer normalized key = more specific)
    pool.sort(key=lambda x: x[1], reverse=True)

    # Overlap verification: skip only for extreme mismatches
    for dk, _, _ in pool:
        rows = pf_by_div[dk]
        if len(rows) <= 2 or not mirror_lines:
            return dk, rows
        overlap = _name_overlap_score(mirror_lines, rows)
        # Refuse pairing only if overlap is zero AND both sides large AND very different sizes
        if (overlap == 0.0
                and len(mirror_lines) > 5
                and len(rows) > 5
                and abs(len(mirror_lines) - len(rows)) / max(len(mirror_lines), len(rows)) > 0.6):
            continue   # catastrophic mismatch — try next candidate
        return dk, rows

    # Fallback: best candidate regardless of overlap
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
    """Return True if string plausibly represents a person name, not a score."""
    s = s.strip()
    if not s:
        return False
    if not re.match(r'[A-Za-z\u00C0-\u024F]', s):
        return False
    # Reject if more than 30% digits (score fragments)
    digits = sum(1 for c in s if c.isdigit())
    return digits <= len(s) * 0.3


def _split_names(s: str) -> list[str]:
    """
    Split display string into individual player names.
    Guards against splitting on '/' inside score annotations.
    """
    for sep in (' / ', ' & ', ' + ', ' and '):
        if sep in s:
            parts = [p.strip() for p in s.split(sep, 1)]
            # Validate both parts look like names, not score fragments
            if all(_looks_like_name(p) for p in parts if p):
                return [p for p in parts if p]
    return [s.strip()] if s.strip() else []


def _surname(name: str) -> str:
    """Last token of a normalized name, after stripping trailing parentheticals."""
    name = re.sub(r'\s*\([^)]*\)\s*$', '', name).strip()
    parts = _norm_name(name).split()
    return parts[-1] if parts else ''


def classify_row_type(mirror_line: str, canon_text: str) -> str:
    """
    Classify a row that exists on BOTH sides.
    Returns: 'exact', 'norm', or 'suspicious'

    Rules:
    - EXACT     : strings equal after trivial normalization
    - NORM      : equal after full normalization (accents, hyphens)
    - SUSPICIOUS: token-count truncation, participant-count mismatch,
                  surname divergence, or high Levenshtein ratio
    """
    _, m_name = _extract_place(mirror_line)
    c_name = re.sub(r'^\d+[.)]\s*', '', canon_text).strip()

    if not m_name or not c_name:
        return 'suspicious'

    # Strip trailing annotation noise from mirror side (country codes, scores)
    m_name_clean = _strip_annotations(m_name)
    if not m_name_clean:
        m_name_clean = m_name   # don't strip everything

    # Trivial exact (use cleaned mirror name)
    if _norm_trivial(m_name_clean) == _norm_trivial(c_name):
        return 'exact'

    # Full normalized exact
    mn = _norm_name(m_name_clean)
    cn = _norm_name(c_name)
    if mn == cn:
        return 'norm'

    # Split into per-player names (use cleaned mirror name)
    m_names = _split_names(m_name_clean)
    c_names = _split_names(c_name)

    # Participant count mismatch (singles vs doubles)
    if len(m_names) != len(c_names) and (len(m_names) > 1 or len(c_names) > 1):
        return 'suspicious'

    # Per-player token-count ratio
    for mn_str, cn_str in zip(
        [_norm_name(n) for n in m_names],
        [_norm_name(n) for n in c_names],
    ):
        m_toks = mn_str.split()
        c_toks = cn_str.split()
        if m_toks and c_toks:
            ratio = min(len(m_toks), len(c_toks)) / max(len(m_toks), len(c_toks))
            if ratio < 0.5:
                return 'suspicious'   # one side clearly truncated

    # Surname check for first player
    if m_names and c_names:
        m_sn = _surname(m_names[0])
        c_sn = _surname(c_names[0])
        if m_sn and c_sn and len(m_sn) > 2 and len(c_sn) > 2:
            lev = _levenshtein(m_sn, c_sn)
            max_len = max(len(m_sn), len(c_sn))
            if lev / max_len > 0.4:
                return 'suspicious'

    # Full-name Levenshtein ratio
    lev = _levenshtein(mn, cn)
    max_len = max(len(mn), len(cn), 1)
    if lev / max_len > 0.3:
        return 'suspicious'

    return 'norm'


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
            disp = _display(r)
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

    aligned_rows: list of {l, r, t} where t ∈
        exact, norm, suspicious, missing_right, missing_left,
        header, context, section_marker

    qc_summary: {exact, norm, suspicious, missing_left, missing_right, unmatched_divs}
    """
    pf_by_div, pf_place_display, pf_place_order = _build_pf_maps(pf_rows)

    mirror_blocks = _parse_raw_into_blocks(results_raw)
    rows: list[dict] = []
    matched_divs: set[str] = set()
    qc: dict[str, int] = defaultdict(int)

    for mirror_header, mirror_lines, is_context in mirror_blocks:
        pf_div_key = None

        if not is_context:
            pf_div_key, _ = _find_div(mirror_header, mirror_lines, pf_by_div)

        if pf_div_key:
            matched_divs.add(pf_div_key)

        # Header row (context lines render muted)
        left_hdr  = mirror_header or ''
        right_hdr = pf_div_key or ''
        row_type  = 'context' if is_context else 'header'
        if left_hdr or right_hdr:
            rows.append({'l': left_hdr, 'r': right_hdr, 't': row_type})

        # Build place maps
        mirror_place_map: dict[int, str] = {}
        non_place_lines: list[str] = []
        for line in mirror_lines:
            pnum, _ = _extract_place(line)
            if pnum is not None and pnum not in mirror_place_map:
                mirror_place_map[pnum] = line.strip()
            else:
                non_place_lines.append(line.strip())

        canon_place_map: dict[int, str] = {}
        if pf_div_key:
            for place in pf_place_order.get(pf_div_key, []):
                disp = pf_place_display.get((pf_div_key, place), '')
                if disp:
                    canon_place_map[place] = f"{place}. {disp}"

        # Aligned placement rows
        all_places = sorted(set(mirror_place_map) | set(canon_place_map))
        for place in all_places:
            m = mirror_place_map.get(place, '')
            c = canon_place_map.get(place, '')
            if m and c:
                t = classify_row_type(m, c)
            elif m:
                t = 'missing_right'
            else:
                t = 'missing_left'
            rows.append({'l': m, 'r': c, 't': t})
            qc[t] += 1

        # Non-placement mirror lines (duplicates / unparseable)
        for line in non_place_lines:
            rows.append({'l': line, 'r': '', 't': 'missing_right'})
            qc['missing_right'] += 1

    # Unmatched canonical divisions
    unmatched = sorted(div for div in pf_by_div if div not in matched_divs)
    if unmatched:
        rows.append({'l': '', 'r': '— UNMATCHED CANONICAL DIVISIONS —', 't': 'section_marker'})
        for div in unmatched:
            rows.append({'l': '', 'r': div, 't': 'header'})
            for place in pf_place_order.get(div, []):
                disp = pf_place_display.get((div, place), '')
                if disp:
                    rows.append({'l': '', 'r': f"{place}. {disp}", 't': 'missing_left'})
                    qc['missing_left'] += 1
        qc['unmatched_divs'] = len(unmatched)

    summary = {
        'exact':          qc.get('exact',          0),
        'norm':           qc.get('norm',            0),
        'suspicious':     qc.get('suspicious',      0),
        'missing_left':   qc.get('missing_left',    0),
        'missing_right':  qc.get('missing_right',   0),
        'unmatched_divs': qc.get('unmatched_divs',  0),
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
    """Return 'red', 'yellow', or 'green' for the event list indicator."""
    if qc['suspicious'] > 0 or qc['unmatched_divs'] > 0:
        return 'red'
    if qc['missing_right'] + qc['missing_left'] > 5:
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

            raw_text = r.get('results_raw', '')
            pf_rows  = pf.get(eid, [])
            aligned, qc_summary = build_aligned_rows(raw_text, pf_rows)

            events.append({
                'id':       eid,
                'year':     year,
                'name':     name,
                'scan_jpg': scan_index.get(eid) or scan_index.get(fuzzy, ''),
                'q':        quarantine.get(eid, ''),
                'rows':     aligned,
                'qc':       qc_summary,
                'qs':       _qc_status(qc_summary),
            })

    return sorted(events, key=lambda x: (x['year'], x['name']), reverse=True)


# ── HTML template ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Footbag Event Comparison — Mirror vs Canonical</title>
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
      padding: 4px 8px;
      border: none;
      border-radius: 3px;
      width: 220px;
      font-size: 12px;
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
      padding: 7px 10px;
      cursor: pointer;
      border-bottom: 1px solid #333;
      font-size: 11px;
      color: #bbb;
      line-height: 1.4;
      display: flex;
      align-items: flex-start;
      gap: 5px;
    }
    .ev-item:hover  { background: #2a3a4a; }
    .ev-item.active { background: #1a3a5c; color: white; font-weight: bold; }
    .ev-q-flag      { color: #f59e0b; font-size: 9px; display: block; }
    .ev-year        { opacity: 0.55; font-size: 10px; margin-right: 1px; flex-shrink: 0; }
    .ev-name        { flex: 1; }
    /* QC status dot */
    .qc-dot {
      width: 7px; height: 7px; border-radius: 50%;
      flex-shrink: 0; margin-top: 3px;
    }
    .qc-dot.red    { background: #e55; }
    .qc-dot.yellow { background: #f59e0b; }
    .qc-dot.green  { background: #4c4; }

    /* ── Comparison pane ── */
    #cmp-pane {
      overflow: hidden;
      display: flex;
      flex-direction: column;
      background: #fff;
    }
    #cmp-title {
      padding: 5px 10px;
      background: #f0f4f8;
      font-size: 12px;
      font-weight: bold;
      color: #1a3a5c;
      border-bottom: 1px solid #d0d8e0;
      flex-shrink: 0;
    }

    /* QC summary bar */
    #qc-bar {
      padding: 4px 10px;
      background: #f7f9fb;
      border-bottom: 1px solid #d8e0e8;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      flex-shrink: 0;
      min-height: 28px;
      align-items: center;
    }
    .qc-chip {
      padding: 1px 7px;
      border-radius: 10px;
      font-size: 10px;
      font-weight: bold;
      font-family: 'Courier New', Courier, monospace;
      white-space: nowrap;
    }
    .qc-chip.exact       { background: #d4edda; color: #1a5c2a; }
    .qc-chip.norm        { background: #d1ecf1; color: #0c5460; }
    .qc-chip.suspicious  { background: #fff3cd; color: #856404; border: 1px solid #ffc107; }
    .qc-chip.missing     { background: #f8d7da; color: #721c24; }
    .qc-chip.unmatched   { background: #e2d9f3; color: #4a1770; }
    .qc-chip.clean       { background: #d4edda; color: #1a5c2a; font-style: italic; }

    #cmp-col-headers {
      display: grid;
      grid-template-columns: 1fr 1fr;
      background: #1F3864;
      color: white;
      font-size: 11px;
      font-weight: bold;
      flex-shrink: 0;
    }
    #cmp-col-headers div { padding: 5px 10px; }
    #cmp-col-headers div:first-child { border-right: 1px solid #4a6080; }
    #cmp-scroll {
      flex: 1;
      overflow-y: scroll;
      overflow-x: hidden;
    }

    /* ── Aligned rows grid ── */
    .cmp-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
    }

    /* Both cells share identical typography */
    .cmp-cell {
      font-family: 'Courier New', Courier, monospace;
      font-size: 11px;
      line-height: 1.55;
      white-space: pre-wrap;
      padding: 1px 10px;
      border-bottom: 1px solid #f0f0f0;
      min-height: 1.55em;
      word-break: break-word;
    }
    .cmp-cell.left { border-right: 1px solid #dde3e8; }

    /* ── Row type styles ── */

    /* header: blue-grey band */
    .cmp-cell[data-t="header"] {
      background: #eef2f7;
      font-weight: bold;
      color: #1a3a5c;
      padding-top: 4px;
      padding-bottom: 4px;
      border-bottom: 1px solid #c8d4e0;
    }

    /* context: narrative text — muted, italic */
    .cmp-cell[data-t="context"] {
      background: #fafafa;
      color: #999;
      font-style: italic;
      font-size: 10px;
    }

    /* section marker */
    .cmp-cell[data-t="section_marker"] {
      background: #fffbea;
      font-style: italic;
      color: #8a5f00;
    }

    /* exact: no highlight — clean agreement */
    /* (intentionally no background override) */

    /* norm: very light teal — acceptable normalization difference */
    .cmp-cell[data-t="norm"] {
      background: #f2fffe;
    }

    /* suspicious: amber on BOTH cells — real difference, needs review */
    .cmp-cell[data-t="suspicious"] {
      background: #fff8e6;
    }
    .cmp-cell.right[data-t="suspicious"] {
      background: #fff3cd;
      border-left: 2px solid #ffc107;
    }

    /* missing_right: mirror has it, canonical doesn't — red on right */
    .cmp-cell.right[data-t="missing_right"] { background: #fff0f0; }

    /* missing_left: canonical has it, mirror doesn't — blue on right */
    .cmp-cell.right[data-t="missing_left"]  { background: #eaf0ff; }
    .cmp-cell.left[data-t="missing_left"]   { background: #fafafa; }

    /* ── Scan pane ── */
    #scan-pane {
      background: #2d2d2d;
      display: flex;
      flex-direction: column;
      border-left: 1px solid #3a3a3a;
    }
    #scan-toolbar {
      background: #1e1e1e;
      padding: 6px 10px;
      display: flex;
      gap: 8px;
      color: white;
      font-size: 11px;
      align-items: center;
      flex-shrink: 0;
      border-bottom: 1px solid #111;
    }
    #scan-toolbar button {
      padding: 2px 9px;
      cursor: pointer;
      background: #444;
      color: white;
      border: 1px solid #555;
      border-radius: 3px;
      font-size: 13px;
    }
    #scan-toolbar button:hover { background: #555; }
    #scan-fname { opacity: 0.45; margin-left: auto; font-size: 10px; }
    #viewport {
      flex: 1;
      overflow: auto;
      display: flex;
      justify-content: center;
      align-items: flex-start;
      padding: 20px;
    }
    #scan-img {
      transition: transform 0.2s;
      box-shadow: 0 0 20px #000;
      transform-origin: center center;
      max-width: 100%;
    }

    /* ── Legend ── */
    #legend {
      padding: 3px 10px;
      background: #f0f4f8;
      border-bottom: 1px solid #d0d8e0;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      font-size: 9px;
      color: #555;
      flex-shrink: 0;
    }
    .leg { display: flex; align-items: center; gap: 3px; }
    .leg-box {
      width: 10px; height: 10px; border: 1px solid #ccc;
      display: inline-block; flex-shrink: 0;
    }
  </style>
</head>
<body>
  <div id="hdr">
    <strong>Footbag Event Comparison — Mirror vs Canonical (V8)</strong>
    <input type="text" id="search" placeholder="Search events…" oninput="filterList()">
    <select id="qc-filter" onchange="filterList()" style="padding:4px 6px;border:none;border-radius:3px;font-size:11px;background:#fff;cursor:pointer;">
      <option value="">All</option>
      <option value="red">⚠ Suspicious/Unmatched</option>
      <option value="yellow">↕ Gaps only</option>
      <option value="green">✓ Clean</option>
    </select>
    <span id="ev-count"></span>
    <div id="nav">
      <button id="btn-prev" onclick="stepEvent(-1)" title="Previous event (↑)">↑</button>
      <span id="nav-pos">—</span>
      <button id="btn-next" onclick="stepEvent(1)"  title="Next event (↓)">↓</button>
    </div>
  </div>

  <div id="body">
    <div id="list"></div>

    <div id="cmp-pane">
      <div id="cmp-title">Select an event from the list</div>
      <div id="qc-bar"></div>
      <div id="legend">
        <span class="leg"><span class="leg-box" style="background:#fff"></span>exact match</span>
        <span class="leg"><span class="leg-box" style="background:#f2fffe"></span>norm match</span>
        <span class="leg"><span class="leg-box" style="background:#fff3cd;border-color:#ffc107"></span>suspicious</span>
        <span class="leg"><span class="leg-box" style="background:#fff0f0"></span>missing in canonical</span>
        <span class="leg"><span class="leg-box" style="background:#eaf0ff"></span>missing in mirror</span>
        <span class="leg"><span class="leg-box" style="background:#fafafa;font-style:italic"></span>context</span>
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
        <button onclick="rotate(-90)" title="Rotate left">↺</button>
        <button onclick="rotate(90)"  title="Rotate right">↻</button>
        <span id="scan-fname">No scan</span>
      </div>
      <div id="viewport">
        <img id="scan-img" style="display:none;">
      </div>
    </div>
  </div>

  <script>
    const EVENTS = %EVENTS_JSON%;
    let rotation = 0;
    let filtered = EVENTS;
    let currentIndex = 0;

    function esc(s) {
      return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function renderList(items) {
      const el = document.getElementById('list');
      el.innerHTML = items.map(ev => {
        const dotCls = ev.qs || 'green';
        return `<div class="ev-item${ev.q ? ' ev-q' : ''}" id="ev-${ev.id}" onclick="selectEvent('${ev.id}')">
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
      const total = qc.exact + qc.norm + qc.suspicious;
      const chips = [];
      if (total === 0 && qc.missing_left === 0 && qc.missing_right === 0 && qc.unmatched_divs === 0) {
        chips.push('<span class="qc-chip clean">no data</span>');
      } else {
        if (qc.exact > 0)
          chips.push(`<span class="qc-chip exact">✓ ${qc.exact} exact</span>`);
        if (qc.norm > 0)
          chips.push(`<span class="qc-chip norm">≈ ${qc.norm} norm</span>`);
        if (qc.suspicious > 0)
          chips.push(`<span class="qc-chip suspicious">⚠ ${qc.suspicious} suspicious</span>`);
        const miss = qc.missing_right + qc.missing_left;
        if (miss > 0)
          chips.push(`<span class="qc-chip missing">✗ ${miss} missing (←${qc.missing_left} →${qc.missing_right})</span>`);
        if (qc.unmatched_divs > 0)
          chips.push(`<span class="qc-chip unmatched">⊘ ${qc.unmatched_divs} unmatched div${qc.unmatched_divs>1?'s':''}</span>`);
        if (qc.suspicious === 0 && qc.unmatched_divs === 0 && miss === 0)
          chips.push('<span class="qc-chip clean">all clear</span>');
      }
      document.getElementById('qc-bar').innerHTML = chips.join('');
    }

    function updateNav() {
      document.getElementById('nav-pos').textContent =
        filtered.length ? `${currentIndex + 1} / ${filtered.length}` : '—';
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
        if (q  && !(e.year + ' ' + e.name).toLowerCase().includes(q)) return false;
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

      const title = ev.year + ' ' + ev.name + (ev.q ? '  ⚑ QUARANTINED: ' + ev.q : '');
      document.getElementById('cmp-title').textContent = title;

      const grid = document.getElementById('cmp-grid');
      const rows = ev.rows || [];
      if (!rows.length) {
        grid.innerHTML =
          '<div class="cmp-cell left"  data-t="header"><em style="color:#999">No source data</em></div>' +
          '<div class="cmp-cell right" data-t="header"><em style="color:#999">No canonical data</em></div>';
      } else {
        const parts = [];
        for (const r of rows) {
          parts.push(`<div class="cmp-cell left"  data-t="${r.t}">${esc(r.l)}</div>`);
          parts.push(`<div class="cmp-cell right" data-t="${r.t}">${esc(r.r)}</div>`);
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
      img.style.margin = (Math.abs(rotation) / 90) % 2 === 1 ? '150px 0' : '0';
    }

    document.addEventListener('keydown', e => {
      if (document.activeElement === document.getElementById('search')) return;
      if (e.key === 'ArrowDown' || e.key === 'j') { e.preventDefault(); stepEvent(1); }
      if (e.key === 'ArrowUp'   || e.key === 'k') { e.preventDefault(); stepEvent(-1); }
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
    print("Building aligned rows for each event…")
    events = load_events(quarantine, scan_index, pf)

    # Aggregate stats
    total_exact      = sum(e['qc']['exact']          for e in events)
    total_norm       = sum(e['qc']['norm']           for e in events)
    total_suspicious = sum(e['qc']['suspicious']     for e in events)
    total_miss_l     = sum(e['qc']['missing_left']   for e in events)
    total_miss_r     = sum(e['qc']['missing_right']  for e in events)
    total_unmatched  = sum(e['qc']['unmatched_divs'] for e in events)
    red_events       = sum(1 for e in events if e['qs'] == 'red')

    print(f"  {len(events)} events processed.")
    print(f"  Rows — exact:{total_exact}  norm:{total_norm}  "
          f"suspicious:{total_suspicious}  "
          f"missing_left:{total_miss_l}  missing_right:{total_miss_r}")
    print(f"  Unmatched canonical divs: {total_unmatched}")
    print(f"  Events with suspicious/unmatched (red): {red_events}")

    html = HTML_TEMPLATE.replace('%EVENTS_JSON%', json.dumps(events, ensure_ascii=False))
    with open(OUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Viewer written → {OUT_HTML}")


if __name__ == '__main__':
    main()
