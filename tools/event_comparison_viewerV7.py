#!/usr/bin/env python3
"""
event_comparison_viewerV7.py

Side-by-side comparison viewer: raw mirror text (column 1) vs identity-locked
canonical results (column 2), with explicit line-for-line alignment.

Alignment strategy
------------------
For each event:
  1. Parse results_raw into division blocks in encounter order.
     A "block" is (header_line, [placement_lines]).  A placement line is one
     that starts with a digit (possibly decorated with . ) : - st nd rd th).
     Everything else is treated as a header/narrative line that opens a new block.
  2. From Placements_Flat, build a per-division map keyed on division_canon.
     Each entry is {place → display_name}.  For __NON_PERSON__ rows the
     team_display_name is used; plain person rows use person_canon.
  3. For each mirror block, fuzzy-match the header against canonical division
     names (exact normalised → then substring containment).
  4. Within each matched division, align on placement number:
       - present on both sides → "match"
       - mirror only           → "missing_right"
       - canonical only        → "missing_left"
  5. Canonical divisions with no mirror counterpart are appended at the end
     under a visible section marker.

The resulting aligned row list is pre-computed in Python and embedded in the
HTML as compact JSON {l, r, t} (left, right, type).  Both columns in the
browser are rendered from the same list, guaranteeing identical row counts
and identical line spacing.
"""

import csv, json, re, sys
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

# ── Mirror parsing ────────────────────────────────────────────────────────────

# A placement line starts with a leading digit (optional ordinal/punctuation).
# Examples:  "1. Name"  "2nd Name"  "3) Name"  " 4 Name"  "1T Name"
_PLACE_LINE_RE  = re.compile(r'^\s*\d+\s*[.):\-T]?\s*(?:st|nd|rd|th)?\s*\S')
_PLACE_NUM_RE   = re.compile(r'^\s*(\d+)\s*[.):\-T]?\s*(?:st|nd|rd|th)?\s+(.*)', re.I)


def _parse_raw_into_blocks(text: str):
    """
    Return list of (header: str|None, placement_lines: list[str]) in encounter
    order.  Non-placement lines open new blocks.
    """
    if not text:
        return []
    blocks: list = []
    cur_header = None
    cur_lines: list[str] = []

    for raw in text.splitlines():
        s = raw.strip()
        if not s:
            continue
        if _PLACE_LINE_RE.match(s):
            cur_lines.append(s)
        else:
            if cur_header is not None or cur_lines:
                blocks.append((cur_header, cur_lines))
            cur_header = s
            cur_lines = []

    if cur_header is not None or cur_lines:
        blocks.append((cur_header, cur_lines))

    return blocks


def _extract_place(line: str):
    """Return (int_place, rest_of_line) or (None, line)."""
    m = _PLACE_NUM_RE.match(line)
    if m:
        return int(m.group(1)), m.group(2).strip()
    return None, line.strip()


# ── Division matching ─────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r'\W+', ' ', s or '').strip().lower()


def _find_div(header: str, pf_by_div: dict):
    """Return (div_key, rows) for the best canonical match, or (None, None)."""
    if not header:
        return None, None
    nh = _norm(header)
    # 1. Exact normalised match
    for dk in pf_by_div:
        if _norm(dk) == nh:
            return dk, pf_by_div[dk]
    # 2. Substring containment (longer key contains shorter query or vice-versa)
    best_len = 0
    best_key = None
    for dk in pf_by_div:
        nk = _norm(dk)
        if not nk:
            continue
        if nk in nh or nh in nk:
            if len(nk) > best_len:
                best_len = len(nk)
                best_key = dk
    if best_key:
        return best_key, pf_by_div[best_key]
    return None, None


# ── Canonical display ─────────────────────────────────────────────────────────

def _display(row: dict) -> str:
    """Canonical display name for a PF row; never returns __NON_PERSON__."""
    pc = row.get('person_canon', '')
    td = row.get('team_display_name', '')
    if pc == '__NON_PERSON__':
        return td or ''
    return pc or td or ''


# ── Alignment builder ─────────────────────────────────────────────────────────

def _build_pf_maps(pf_rows: list):
    """
    Build:
      pf_by_div:        div_canon → [raw PF rows]
      pf_place_display: (div_canon, place) → display_str
      pf_place_order:   div_canon → [sorted unique places]
    """
    pf_by_div: dict[str, list] = defaultdict(list)
    for r in pf_rows:
        pf_by_div[r['division_canon']].append(r)

    pf_place_display: dict[tuple, str] = {}
    pf_place_order: dict[str, list] = {}

    for div, rows in pf_by_div.items():
        seen: dict[int, str] = {}
        for r in rows:
            place = int(r['place'])
            disp = _display(r)
            if not disp:
                continue
            if place not in seen:
                seen[place] = disp
            # If team_display_name already covers both players, don't concat.
        for place, disp in sorted(seen.items()):
            pf_place_display[(div, place)] = disp
        pf_place_order[div] = sorted(seen.keys())

    return pf_by_div, pf_place_display, pf_place_order


def build_aligned_rows(results_raw: str, pf_rows: list) -> list:
    """
    Return list of compact row dicts {l, r, t} where t ∈
    {header, match, missing_right, missing_left, section_marker}.
    """
    pf_by_div, pf_place_display, pf_place_order = _build_pf_maps(pf_rows)

    mirror_blocks = _parse_raw_into_blocks(results_raw)
    rows: list[dict] = []
    matched_divs: set[str] = set()

    for mirror_header, mirror_lines in mirror_blocks:
        pf_div_key, _ = _find_div(mirror_header, pf_by_div)
        if pf_div_key:
            matched_divs.add(pf_div_key)

        # Header row
        left_hdr  = mirror_header or ''
        right_hdr = pf_div_key if pf_div_key else ''
        if left_hdr or right_hdr:
            rows.append({'l': left_hdr, 'r': right_hdr, 't': 'header'})

        # Build place maps for this block
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
            t = 'match' if (m and c) else ('missing_right' if m else 'missing_left')
            rows.append({'l': m, 'r': c, 't': t})

        # Non-placement mirror lines (narrative, unparseable)
        for line in non_place_lines:
            rows.append({'l': line, 'r': '', 't': 'missing_right'})

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

    return rows


# ── Data loading ──────────────────────────────────────────────────────────────

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
            fname = r.get('source_file', '')
            eid   = r.get('event_id', '')
            if eid:
                idx[eid] = fname
            fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(' ', '')
            idx[fuzzy] = fname
    return idx


def load_pf():
    """Return {event_id: [rows]} from Placements_Flat."""
    pf: dict[str, list] = defaultdict(list)
    if not PF_CSV.exists():
        print(f"WARNING: {PF_CSV} not found — canonical column will be empty.")
        return pf
    with open(PF_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            pf[r['event_id']].append(r)
    return pf


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

            raw_text  = r.get('results_raw', '')
            pf_rows   = pf.get(eid, [])
            aligned   = build_aligned_rows(raw_text, pf_rows)

            events.append({
                'id':       eid,
                'year':     year,
                'name':     name,
                'scan_jpg': scan_index.get(eid) or scan_index.get(fuzzy, ''),
                'q':        quarantine.get(eid, ''),
                'rows':     aligned,
            })

    return sorted(events, key=lambda x: (x['year'], x['name']), reverse=True)


# ── HTML template ─────────────────────────────────────────────────────────────

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
    }
    .ev-item:hover  { background: #2a3a4a; }
    .ev-item.active { background: #1a3a5c; color: white; font-weight: bold; }
    .ev-q-flag      { color: #f59e0b; font-size: 9px; }
    .ev-year        { opacity: 0.55; font-size: 10px; margin-right: 3px; }

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
    #cmp-col-headers {
      display: grid;
      grid-template-columns: 1fr 1fr;
      background: #1F3864;
      color: white;
      font-size: 11px;
      font-weight: bold;
      flex-shrink: 0;
    }
    #cmp-col-headers div {
      padding: 5px 10px;
    }
    #cmp-col-headers div:first-child {
      border-right: 1px solid #4a6080;
    }
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

    /* Both cells share identical typography — the core alignment contract */
    .cmp-cell {
      font-family: 'Courier New', Courier, monospace;
      font-size: 11px;
      line-height: 1.55;
      white-space: pre-wrap;
      padding: 1px 10px;
      border-bottom: 1px solid #f0f0f0;
      min-height: 1.55em;   /* empty cells match the height of their neighbour */
      word-break: break-word;
    }
    .cmp-cell.left {
      border-right: 1px solid #dde3e8;
    }

    /* Row-type styles ─────────────────────────── */
    /* header: light blue-grey band across both cells */
    .cmp-cell[data-t="header"] {
      background: #eef2f7;
      font-weight: bold;
      color: #1a3a5c;
      padding-top: 4px;
      padding-bottom: 4px;
      border-bottom: 1px solid #c8d4e0;
    }

    /* section marker (unmatched canonical) */
    .cmp-cell[data-t="section_marker"] {
      background: #fffbea;
      font-style: italic;
      color: #8a5f00;
    }

    /* missing_right: in mirror but absent from canonical — highlight RIGHT cell red */
    .cmp-cell.right[data-t="missing_right"] {
      background: #fff0f0;
    }

    /* missing_left: in canonical but absent from mirror — highlight RIGHT cell blue */
    .cmp-cell.right[data-t="missing_left"] {
      background: #eaf0ff;
    }
    /* left cell for missing_left is empty — give it a very faint stripe so the
       absence is visible without being distracting */
    .cmp-cell.left[data-t="missing_left"] {
      background: #fafafa;
    }

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
  </style>
</head>
<body>
  <div id="hdr">
    <strong>Footbag Event Comparison — Mirror vs Canonical</strong>
    <input type="text" id="search" placeholder="Search events…" oninput="filterList()">
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
    let currentIndex = 0;   // index into `filtered`

    function esc(s) {
      return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function renderList(items) {
      const el = document.getElementById('list');
      el.innerHTML = items.map(ev =>
        `<div class="ev-item${ev.q ? ' ev-q' : ''}" id="ev-${ev.id}" onclick="selectEvent('${ev.id}')">
          <span class="ev-year">${esc(ev.year)}</span>${esc(ev.name)}
          ${ev.q ? `<br><span class="ev-q-flag">⚑ quarantined</span>` : ''}
        </div>`
      ).join('');
      document.getElementById('ev-count').textContent = `${items.length} events`;
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
      const q = document.getElementById('search').value.toLowerCase();
      filtered = q ? EVENTS.filter(e => (e.year + ' ' + e.name).toLowerCase().includes(q)) : EVENTS;
      currentIndex = 0;
      renderList(filtered);
      if (filtered.length) selectEvent(filtered[0].id);
      else updateNav();
    }

    function selectEvent(id) {
      const ev = EVENTS.find(e => e.id === id);
      if (!ev) return;

      // Sync currentIndex to match the selected id in filtered list
      const idx = filtered.findIndex(e => e.id === id);
      if (idx !== -1) currentIndex = idx;

      document.querySelectorAll('.ev-item').forEach(el => el.classList.remove('active'));
      const li = document.getElementById('ev-' + id);
      if (li) { li.classList.add('active'); li.scrollIntoView({block:'nearest'}); }

      updateNav();

      const title = ev.year + ' ' + ev.name + (ev.q ? '  ⚑ QUARANTINED: ' + ev.q : '');
      document.getElementById('cmp-title').textContent = title;

      // Render aligned rows
      const grid = document.getElementById('cmp-grid');
      const rows = ev.rows || [];
      if (!rows.length) {
        grid.innerHTML =
          '<div class="cmp-cell left"  data-t="match"><em style="color:#999;">No source data</em></div>' +
          '<div class="cmp-cell right" data-t="match"><em style="color:#999;">No canonical data</em></div>';
      } else {
        const parts = [];
        for (const r of rows) {
          parts.push(`<div class="cmp-cell left"  data-t="${r.t}">${esc(r.l)}</div>`);
          parts.push(`<div class="cmp-cell right" data-t="${r.t}">${esc(r.r)}</div>`);
        }
        grid.innerHTML = parts.join('');
      }

      // Scan image
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

    // Keyboard navigation — ↑/↓ when not typing in the search box
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading quarantine…")
    quarantine  = load_quarantine()
    print("Loading scan index…")
    scan_index  = load_scan_index()
    print("Loading Placements_Flat…")
    pf          = load_pf()
    print("Building aligned rows for each event…")
    events      = load_events(quarantine, scan_index, pf)

    print(f"  {len(events)} events processed.")
    html = HTML_TEMPLATE.replace('%EVENTS_JSON%', json.dumps(events, ensure_ascii=False))

    with open(OUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Viewer written → {OUT_HTML}")


if __name__ == '__main__':
    main()
