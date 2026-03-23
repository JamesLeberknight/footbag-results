#!/usr/bin/env python3
"""
tools/event_comparison_viewer.py
─────────────────────────────────
Generates out/event_comparison_viewer.html — a self-contained browser for
comparing raw mirror text vs canonical identity-locked placements for all
774 footbag events.

Usage:
    .venv/bin/python tools/event_comparison_viewer.py

Output:
    out/event_comparison_viewer.html   (~6-7 MB, open in any browser)
"""

from __future__ import annotations
import csv, json, sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT           = Path(__file__).resolve().parent.parent
OUT            = ROOT / "out"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PF_CSV         = OUT / "Placements_Flat.csv"
QUARANTINE_CSV = ROOT / "inputs" / "review_quarantine_events.csv"
EVENTS_CSV     = OUT / "canonical" / "events.csv"
OUT_HTML       = OUT / "event_comparison_viewer.html"


def load_quarantine() -> dict[str, str]:
    q = {}
    with open(QUARANTINE_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            q[r["event_id"]] = r["reason"]
    return q



def load_event_keys() -> dict[str, str]:
    """Return event_id → event_key from canonical/events.csv (if available)."""
    ek: dict[str, str] = {}
    if not EVENTS_CSV.exists():
        return ek
    with open(EVENTS_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("legacy_event_id", "").strip()
            key = r.get("event_key", "").strip()
            if eid and key:
                ek[eid] = key
    return ek


def load_events(quarantine: dict, event_keys: dict) -> list[dict]:
    events = []
    with open(STAGE2_CSV, newline="", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            eid = r["event_id"]
            events.append({
                "id":  eid,
                "ek":  event_keys.get(eid, eid),
                "yr":  r["year"],
                "nm":  r["event_name"],
                "dt":  r.get("date", ""),
                "loc": r.get("location", ""),
                "rr":  r.get("results_raw", ""),
                "qt":  quarantine.get(eid, ""),
            })
    events.sort(key=lambda e: (e["yr"], e["nm"]))
    return events


def load_pf_index() -> dict[str, list[dict]]:
    idx: dict[str, list] = {}
    with open(PF_CSV, newline="", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            eid = r["event_id"]
            pu_raw = r.get("person_unresolved", "")
            pu = 1 if pu_raw in ("1", "True") else 0
            entry = {
                "div": r.get("division_canon", ""),
                "cat": r.get("division_category", ""),
                "pl":  int(r["place"]) if r["place"].lstrip("-").isdigit() else 0,
                "ct":  r.get("competitor_type", "player"),
                "pc":  r.get("person_canon", ""),
                "tdn": r.get("team_display_name", ""),
                "tpk": r.get("team_person_key", ""),
                "pu":  pu,
                "cf":  r.get("coverage_flag", ""),
            }
            idx.setdefault(eid, []).append(entry)
    return idx


def build_html(events: list[dict], pf_index: dict, quarantine: dict) -> str:
    events_json = json.dumps(events, separators=(',', ':'), ensure_ascii=False)
    pf_json     = json.dumps(pf_index, separators=(',', ':'), ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Footbag Event Comparison Viewer</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{height:100%;overflow:hidden;font-family:'Segoe UI',Arial,sans-serif;font-size:13px;color:#222}}
#app{{display:grid;grid-template-rows:auto 1fr;height:100vh}}
#hdr{{background:#1F3864;color:#fff;padding:8px 18px;display:flex;align-items:center;gap:16px;flex-shrink:0}}
#hdr h1{{font-size:15px;font-weight:700;letter-spacing:.02em;white-space:nowrap}}
#hdr .hint{{font-size:11px;color:rgba(255,255,255,.55);white-space:nowrap}}
#hdr .count{{font-size:11px;color:rgba(255,255,255,.5);margin-left:auto;white-space:nowrap}}
#hdr .progress{{font-size:11px;color:rgba(255,255,255,.7);white-space:nowrap}}
#body{{display:grid;grid-template-columns:280px 1fr;overflow:hidden}}

/* ── Sidebar ── */
#sidebar{{display:flex;flex-direction:column;border-right:1px solid #ddd;background:#f8f9fa;overflow:hidden}}
#sidebar-top{{flex-shrink:0}}
#search{{width:100%;padding:8px 10px;border:none;border-bottom:1px solid #ddd;font-size:12px;outline:none;background:#fff}}
#search:focus{{border-bottom-color:#1F3864}}
#filter-bar{{display:flex;gap:0;border-bottom:1px solid #ddd;background:#f0f2f5}}
.fbtn{{flex:1;padding:5px 0;font-size:10px;font-weight:600;border:none;background:none;cursor:pointer;color:#666;letter-spacing:.04em;text-transform:uppercase;transition:background .1s}}
.fbtn:hover{{background:#e0e4ea}}
.fbtn.active{{background:#1F3864;color:#fff}}
#event-list{{flex:1;overflow-y:auto}}
.ev-item{{padding:6px 10px;cursor:pointer;border-bottom:1px solid #eee;display:flex;gap:6px;align-items:flex-start;user-select:none;position:relative}}
.ev-item:hover{{background:#e8f0fe}}
.ev-item.selected{{background:#1F3864;color:#fff}}
.ev-item.hidden{{display:none}}
.ev-left{{display:flex;flex-direction:column;gap:2px;flex:1;min-width:0}}
.ev-top{{display:flex;gap:5px;align-items:center}}
.ev-yr{{font-size:10px;background:#e0e0e0;border-radius:3px;padding:1px 5px;white-space:nowrap;flex-shrink:0;line-height:1.6}}
.ev-item.selected .ev-yr{{background:rgba(255,255,255,.2)}}
.ev-nm{{font-size:12px;line-height:1.3;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}}
.ev-ek{{font-family:'Consolas','Courier New',monospace;font-size:9px;color:#999;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:1px}}
.ev-item.selected .ev-ek{{color:rgba(255,255,255,.5)}}
.qt-dot{{width:6px;height:6px;border-radius:50%;background:#e53935;flex-shrink:0;margin-top:5px}}
.rv-badge{{width:16px;height:16px;border-radius:50%;flex-shrink:0;margin-top:3px;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700}}
.rv-badge.yes{{background:#43a047;color:#fff}}
.rv-badge.no{{background:#e53935;color:#fff}}
.rv-badge.none{{background:#e0e0e0;color:transparent}}
#no-results{{padding:14px 10px;font-size:12px;color:#999;font-style:italic;display:none}}

/* ── Main panel ── */
#main{{display:flex;flex-direction:column;overflow:hidden;background:#fff}}
#meta{{padding:8px 16px;border-bottom:1px solid #ddd;background:#fff;flex-shrink:0}}
#meta-row1{{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}}
#meta .mtitle{{font-weight:700;font-size:14px;line-height:1.3}}
#meta .meid{{font-family:'Consolas','Courier New',monospace;font-size:11px;color:#888;background:#f3f4f6;padding:1px 6px;border-radius:3px;white-space:nowrap}}
#meta-row2{{display:flex;align-items:center;gap:12px;margin-top:5px;flex-wrap:wrap}}
#meta .mmeta{{font-size:11px;color:#666;flex:1}}
.badge-qt{{display:inline-block;background:#e53935;color:#fff;font-size:10px;padding:1px 7px;border-radius:3px;margin-left:6px;font-weight:700;vertical-align:middle}}

/* ── Review buttons ── */
#review-btns{{display:flex;gap:6px;align-items:center;flex-shrink:0}}
.rv-btn{{padding:4px 14px;border-radius:4px;border:1.5px solid;font-size:12px;font-weight:700;cursor:pointer;transition:all .12s;background:#fff}}
.rv-btn.yes{{border-color:#43a047;color:#43a047}}
.rv-btn.yes:hover,.rv-btn.yes.active{{background:#43a047;color:#fff}}
.rv-btn.no{{border-color:#e53935;color:#e53935}}
.rv-btn.no:hover,.rv-btn.no.active{{background:#e53935;color:#fff}}
.rv-btn.clr{{border-color:#bbb;color:#888;font-weight:400;font-size:11px;padding:4px 8px}}
.rv-btn.clr:hover{{background:#f5f5f5}}

/* ── Panels ── */
#panels{{display:grid;grid-template-columns:1fr 1fr 1fr;flex:1;overflow:hidden}}
pre#legacy-text{{font-family:'Consolas','Courier New',monospace;font-size:11px;line-height:1.3;white-space:pre;color:#1a3a1a}}
.diff-missing{{background:rgba(255,120,120,0.28);display:block}}
.diff-extra{{background:rgba(120,200,255,0.28);display:block}}
.panel{{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid #e0e0e0}}
.panel:last-child{{border-right:none}}
.ptitle{{padding:5px 12px;font-size:10px;font-weight:700;text-transform:uppercase;color:#666;background:#f0f2f5;border-bottom:1px solid #ddd;flex-shrink:0;letter-spacing:.06em;display:flex;align-items:center;gap:8px}}
.ptitle .pcount{{font-weight:400;color:#999;text-transform:none}}
.pbody{{flex:1;overflow:auto;padding:10px 12px}}
pre#raw-text{{font-family:'Consolas','Courier New',monospace;font-size:11px;line-height:1.5;white-space:pre;color:#333}}
.no-data{{color:#aaa;font-style:italic;padding:14px 0;font-size:12px}}
.canon-div{{margin-bottom:16px}}
.dh{{display:flex;align-items:center;gap:7px;margin-bottom:5px;padding:4px 8px;background:#f5f5f5;border-radius:4px;flex-wrap:wrap}}
.dname{{font-weight:700;font-size:12px}}
.dcat{{font-size:10px;font-weight:700;padding:1px 7px;border-radius:10px;color:#fff}}
.dcat.net{{background:#1565C0}}.dcat.freestyle{{background:#6A1B9A}}.dcat.sideline{{background:#00695C}}.dcat.golf{{background:#2E7D32}}.dcat.unknown{{background:#888}}
.dcnt{{font-size:10px;color:#aaa}}
.cf{{font-size:10px;padding:1px 7px;border-radius:3px;font-weight:600}}
.cf.complete{{background:#E8F5E9;color:#2E7D32}}.cf.partial{{background:#FFF8E1;color:#c77700}}.cf.sparse{{background:#FFF3E0;color:#E65100}}
table.pt{{width:100%;border-collapse:collapse;font-size:12px}}
table.pt td{{padding:3px 6px;border-bottom:1px solid #f0f0f0;vertical-align:top}}
table.pt tr:hover td{{background:#f5f8ff}}
.pnum{{width:32px;text-align:right;color:#bbb;font-size:11px;padding-right:8px;white-space:nowrap}}
.pname{{color:#222}}
.np .pname{{color:#bbb;font-style:italic}}
.unres .pname{{color:#b45309;font-style:italic}}
</style>
</head>
<body>
<div id="app">
  <div id="hdr">
    <h1>Footbag Event Comparison Viewer</h1>
    <span class="hint">↑↓ navigate &nbsp;·&nbsp; Y / N to review &nbsp;·&nbsp; / to search</span>
    <span class="progress" id="hdr-progress"></span>
    <span class="count" id="hdr-count"></span>
  </div>
  <div id="body">
    <div id="sidebar">
      <div id="sidebar-top">
        <input id="search" type="text" placeholder="Filter by year, name, or event key…" autocomplete="off">
        <div id="filter-bar">
          <button class="fbtn active" data-filter="all">All</button>
          <button class="fbtn" data-filter="yes">✓ OK</button>
          <button class="fbtn" data-filter="no">✗ Flag</button>
          <button class="fbtn" data-filter="none">? Unreviewed</button>
        </div>
      </div>
      <div id="event-list"></div>
      <div id="no-results">No events match.</div>
    </div>
    <div id="main">
      <div id="meta">
        <div id="meta-row1">
          <span class="mtitle" id="meta-title">—</span>
          <span class="meid" id="meta-eid"></span>
        </div>
        <div id="meta-row2">
          <span class="mmeta" id="meta-sub"></span>
          <div id="review-btns">
            <button class="rv-btn yes" id="btn-yes" onclick="setReview('y')">✓ OK</button>
            <button class="rv-btn no"  id="btn-no"  onclick="setReview('n')">✗ Flag</button>
            <button class="rv-btn clr"              onclick="setReview(null)">clear</button>
          </div>
        </div>
      </div>
      <div id="panels">
        <div class="panel">
          <div class="ptitle">Raw Mirror</div>
          <div class="pbody"><pre id="raw-text"></pre></div>
        </div>
        <div class="panel">
          <div class="ptitle">Canonical → Legacy Format</div>
          <div class="pbody"><pre id="legacy-text"></pre></div>
        </div>
        <div class="panel">
          <div class="ptitle">Canonical Structured <span class="pcount" id="canon-count"></span></div>
          <div class="pbody" id="canon-body"></div>
        </div>
      </div>
    </div>
  </div>
</div>
<script>
const EVENTS   = {events_json};
const PF_INDEX = {pf_json};
const STORAGE_KEY = 'fbqc_v1_decisions';

const searchEl  = document.getElementById('search');
const listEl    = document.getElementById('event-list');
const noRes     = document.getElementById('no-results');
const metaTitle = document.getElementById('meta-title');
const metaEid   = document.getElementById('meta-eid');
const metaSub   = document.getElementById('meta-sub');
const rawText    = document.getElementById('raw-text');
const legacyText = document.getElementById('legacy-text');
const canonBody  = document.getElementById('canon-body');
const canonCnt   = document.getElementById('canon-count');
const hdrCount   = document.getElementById('hdr-count');
const hdrProg    = document.getElementById('hdr-progress');
const btnYes     = document.getElementById('btn-yes');
const btnNo      = document.getElementById('btn-no');

let currentEid   = null;
let activeFilter = 'all';
const eventMap   = new Map();

// ── Decisions (localStorage) ──────────────────────────────────────────────────
function loadDecisions() {{
  try {{ return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}'); }}
  catch {{ return {{}}; }}
}}
function saveDecisions(d) {{
  localStorage.setItem(STORAGE_KEY, JSON.stringify(d));
}}
let decisions = loadDecisions();

function getDecision(eid) {{ return decisions[eid] || null; }}

function setReview(val) {{
  if (!currentEid) return;
  if (val === null) {{
    delete decisions[currentEid];
  }} else {{
    decisions[currentEid] = val;
  }}
  saveDecisions(decisions);
  updateReviewButtons(val);
  updateSidebarBadge(currentEid, val);
  updateProgress();
  applyFilter();
}}

function updateReviewButtons(val) {{
  btnYes.classList.toggle('active', val === 'y');
  btnNo.classList.toggle('active',  val === 'n');
}}

function updateSidebarBadge(eid, val) {{
  const el = listEl.querySelector('.ev-item[data-eid="' + eid + '"] .rv-badge');
  if (!el) return;
  el.className = 'rv-badge ' + (val === 'y' ? 'yes' : val === 'n' ? 'no' : 'none');
  el.textContent = val === 'y' ? '✓' : val === 'n' ? '✗' : '';
}}

function updateProgress() {{
  const total    = EVENTS.length;
  const reviewed = Object.keys(decisions).length;
  const yes      = Object.values(decisions).filter(v => v === 'y').length;
  const no       = Object.values(decisions).filter(v => v === 'n').length;
  hdrProg.textContent = reviewed + '/' + total + ' reviewed · ' + yes + ' ok · ' + no + ' flagged';
}}

// ── Build sidebar ─────────────────────────────────────────────────────────────
EVENTS.forEach(ev => {{
  eventMap.set(ev.id, ev);
  const div = document.createElement('div');
  div.className = 'ev-item';
  div.dataset.eid    = ev.id;
  div.dataset.search = (ev.yr + ' ' + ev.nm + ' ' + ev.ek).toLowerCase();
  const dec = getDecision(ev.id);
  const badgeCls = dec === 'y' ? 'yes' : dec === 'n' ? 'no' : 'none';
  const badgeTxt = dec === 'y' ? '✓' : dec === 'n' ? '✗' : '';
  div.innerHTML =
    '<span class="rv-badge ' + badgeCls + '">' + badgeTxt + '</span>' +
    (ev.qt ? '<span class="qt-dot" title="Quarantined: ' + escHtml(ev.qt) + '"></span>' : '') +
    '<div class="ev-left">' +
      '<div class="ev-top">' +
        '<span class="ev-yr">' + escHtml(ev.yr) + '</span>' +
        '<span class="ev-nm">' + escHtml(ev.nm) + '</span>' +
      '</div>' +
      '<div class="ev-ek">' + escHtml(ev.ek) + '</div>' +
    '</div>';
  div.addEventListener('click', () => selectEvent(ev.id));
  listEl.appendChild(div);
}});

updateProgress();
hdrCount.textContent = EVENTS.length + ' events';

// ── Filter bar ────────────────────────────────────────────────────────────────
document.getElementById('filter-bar').addEventListener('click', e => {{
  const btn = e.target.closest('.fbtn');
  if (!btn) return;
  activeFilter = btn.dataset.filter;
  document.querySelectorAll('.fbtn').forEach(b => b.classList.toggle('active', b === btn));
  applyFilter();
}});

function applyFilter() {{
  const q = searchEl.value.toLowerCase().trim();
  let visible = 0;
  listEl.querySelectorAll('.ev-item').forEach(el => {{
    const eid = el.dataset.eid;
    const dec = getDecision(eid);
    const matchFilter =
      activeFilter === 'all'  ? true :
      activeFilter === 'yes'  ? dec === 'y' :
      activeFilter === 'no'   ? dec === 'n' :
      activeFilter === 'none' ? dec === null : true;
    const matchSearch = !q || el.dataset.search.includes(q);
    const show = matchFilter && matchSearch;
    el.classList.toggle('hidden', !show);
    if (show) visible++;
  }});
  noRes.style.display = visible === 0 ? 'block' : 'none';
  hdrCount.textContent = visible + ' / ' + EVENTS.length + ' events';
  // If current is now hidden, select first visible
  const curEl = currentEid && listEl.querySelector('.ev-item[data-eid="' + currentEid + '"]');
  if (!curEl || curEl.classList.contains('hidden')) {{
    const first = listEl.querySelector('.ev-item:not(.hidden)');
    if (first) selectEvent(first.dataset.eid);
  }}
}}

// ── Search ────────────────────────────────────────────────────────────────────
searchEl.addEventListener('input', applyFilter);

// ── Keyboard navigation ───────────────────────────────────────────────────────

document.addEventListener('keydown', e => {{
  if (document.activeElement === searchEl) {{
    if (e.key === 'Escape') {{ searchEl.blur(); return; }}
    if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
  }}
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {{
    e.preventDefault();
    const items = [...listEl.querySelectorAll('.ev-item:not(.hidden)')];
    const idx   = items.findIndex(el => el.dataset.eid === currentEid);
    const next  = e.key === 'ArrowDown' ? idx + 1 : idx - 1;
    if (next >= 0 && next < items.length) selectEvent(items[next].dataset.eid);
    return;
  }}
  if (e.key === '/' && document.activeElement !== searchEl) {{
    e.preventDefault(); searchEl.focus(); searchEl.select(); return;
  }}
  if (document.activeElement !== searchEl) {{
    if (e.key === 'y' || e.key === 'Y') {{ setReview('y'); return; }}
    if (e.key === 'n' || e.key === 'N') {{ setReview('n'); return; }}
  }}
}});

// ── Select event ──────────────────────────────────────────────────────────────
function selectEvent(eid) {{
  currentEid = eid;
  listEl.querySelectorAll('.ev-item.selected').forEach(el => el.classList.remove('selected'));
  const selEl = listEl.querySelector('.ev-item[data-eid="' + eid + '"]');
  if (selEl) {{ selEl.classList.add('selected'); selEl.scrollIntoView({{block: 'nearest'}}); }}

  const ev = eventMap.get(eid);
  if (!ev) return;

  metaTitle.innerHTML = escHtml(ev.nm) +
    (ev.qt ? ' <span class="badge-qt">QUARANTINED</span>' : '');
  metaEid.textContent = ev.ek;
  metaSub.textContent = [ev.yr, ev.dt, ev.loc].filter(Boolean).join('  ·  ');
  updateReviewButtons(getDecision(eid));

  const rows = PF_INDEX[eid] || [];
  rawText.textContent    = ev.rr || '(no mirror data available)';
  legacyText.textContent = renderLegacyResults(rows);
  canonBody.innerHTML    = buildCanonHtml(rows);
  canonCnt.textContent   = rows.length > 0 ? '(' + countDisplay(rows) + ')' : '';
}}

// ── Helpers ───────────────────────────────────────────────────────────────────
function countDisplay(rows) {{
  const divs = new Set(rows.map(r => r.div));
  let n = 0;
  const byDiv = groupByDiv(rows);
  for (const [, drows] of byDiv) n += dedupTeams(drows).length;
  return n + ' placements · ' + divs.size + ' div' + (divs.size !== 1 ? 's' : '');
}}

function groupByDiv(rows) {{
  const map = new Map();
  for (const r of rows) {{
    if (!map.has(r.div)) map.set(r.div, []);
    map.get(r.div).push(r);
  }}
  return map;
}}

function dedupTeams(rows) {{
  const seen = new Set(); const out = [];
  for (const r of rows) {{
    if (r.ct === 'team' && r.tpk) {{ if (seen.has(r.tpk)) continue; seen.add(r.tpk); }}
    out.push(r);
  }}
  return out;
}}

function renderLegacyResults(rows) {{
  if (!rows.length) return '(no canonical placements)';
  const byDiv = groupByDiv(rows);
  const lines = [];
  for (const [divName, drows] of byDiv) {{
    drows.sort((a, b) => a.pl !== b.pl ? a.pl - b.pl : getDisplayName(a).localeCompare(getDisplayName(b)));
    const display = dedupTeams(drows);
    lines.push(''); lines.push('--- ' + (divName || '(UNNAMED DIVISION)').toUpperCase() + ' ---'); lines.push('');
    const placeCounts = {{}};
    for (const r of display) placeCounts[r.pl] = (placeCounts[r.pl] || 0) + 1;
    for (const r of display) {{
      const isTie = placeCounts[r.pl] > 1;
      const plStr = r.pl > 0 ? r.pl + (isTie ? 'T' : '') + '. ' : '?. ';
      const marker = r.pc === '__NON_PERSON__' ? ' [non-person]' : r.pu ? ' [unresolved]' : '';
      lines.push(plStr + getDisplayName(r) + marker);
    }}
  }}
  while (lines.length && lines[0] === '') lines.shift();
  return lines.join('\\n');
}}

function getDisplayName(r) {{
  if (r.ct === 'team') return r.tdn || r.pc || '(team)';
  if (r.pc === '__NON_PERSON__') return '[non-person]';
  return r.pc || '?';
}}

function buildCanonHtml(rows) {{
  if (!rows.length) return '<p class="no-data">No canonical placements for this event.</p>';
  const byDiv = groupByDiv(rows);
  let html = '';
  for (const [divName, drows] of byDiv) {{
    drows.sort((a, b) => a.pl - b.pl);
    const display = dedupTeams(drows);
    const cat = drows[0].cat || ''; const cf = drows[0].cf || '';
    const catCls  = ['net','freestyle','sideline','golf'].includes(cat) ? cat : 'unknown';
    const cfHtml  = cf ? '<span class="cf ' + cf + '">' + escHtml(cf) + '</span>' : '';
    const catHtml = '<span class="dcat ' + catCls + '">' + (cat || '?') + '</span>';
    html += '<div class="canon-div"><div class="dh"><span class="dname">' + escHtml(divName || '(unnamed)') + '</span>';
    html += catHtml + cfHtml + '<span class="dcnt">' + display.length + ' placements</span></div>';
    html += '<table class="pt"><tbody>';
    for (const r of display) {{
      const isNp   = r.pc === '__NON_PERSON__';
      const rowCls = (isNp ? 'np' : '') + (r.pu ? ' unres' : '');
      const plDisp = r.pl > 0 ? r.pl : '—';
      let name = r.ct === 'team' ? escHtml(r.tdn || r.pc || '(team)')
               : isNp ? '<span style="color:#ccc">[non-person]</span>'
               : escHtml(r.pc || '?');
      html += '<tr class="' + rowCls.trim() + '"><td class="pnum">' + plDisp + '</td><td class="pname">' + name + '</td></tr>';
    }}
    html += '</tbody></table></div>';
  }}
  return html;
}}

function escHtml(s) {{
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}

function normLine(s) {{ return s.toLowerCase().replace(/\\s+/g,' ').trim(); }}

// Select first event on load
if (EVENTS.length) selectEvent(EVENTS[0].id);
</script>
</body>
</html>"""


def main() -> None:
    print("Loading quarantine…")
    quarantine = load_quarantine()
    print(f"  {len(quarantine)} quarantined events")

    print("Loading event keys…")
    event_keys = load_event_keys()
    print(f"  {len(event_keys)} event keys loaded")

    print("Loading stage2 events…")
    events = load_events(quarantine, event_keys)
    print(f"  {len(events)} events loaded")

    print("Loading Placements_Flat…")
    pf_index = load_pf_index()
    total_rows = sum(len(v) for v in pf_index.values())
    print(f"  {len(pf_index)} events with placements, {total_rows:,} total rows")

    print("Building HTML…")
    html = build_html(events, pf_index, quarantine)

    OUT_HTML.write_text(html, encoding="utf-8")
    size_kb = OUT_HTML.stat().st_size / 1024
    print(f"\nWritten: {OUT_HTML}")
    print(f"Size: {size_kb:.0f} KB ({size_kb/1024:.1f} MB)")
    print(f"\nOpen in browser:\n  file://{OUT_HTML}")


if __name__ == "__main__":
    main()
