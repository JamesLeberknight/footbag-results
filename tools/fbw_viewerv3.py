#!/usr/bin/env python3
import csv, json, sys, os
from pathlib import Path

# Increase field limit for large mirror text blobs
csv.field_size_limit(sys.maxsize)

# --- PATH SETTINGS ---
# Resolves the project root regardless of whether you run from / or /tools
ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML       = OUT / "fbw_archive_viewer.html"

def norm(text):
    """Standardizes text for matching by removing all non-alphanumeric characters."""
    return "".join(filter(str.isalnum, str(text))).lower()

def load_data():
    # 1. Load Scan Index into a dictionary for quick lookup
    scans = {}
    if not SCAN_INDEX_CSV.exists():
        print(f"!! Error: {SCAN_INDEX_CSV} not found.")
        return []

    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            fname = r.get("source_file", "")
            eid   = r.get("event_id", "")
            year  = r.get("year", "")
            name  = r.get("event_name", "")
            
            if not fname: continue
            
            # Map by ID
            if eid: scans[eid] = fname
            # Map by Normalized Year + Name (e.g., "1984worldfootbagchampionships")
            scans[norm(f"{year}{name}")] = fname

    # 2. Load Stage 2 and only keep those with a scan match
    matched = []
    if not STAGE2_CSV.exists():
        print(f"!! Error: {STAGE2_CSV} not found.")
        return []

    with open(STAGE2_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            eid  = r.get("event_id", "")
            year = r.get("year", "")
            name = r.get("event_name", "")
            
            # Try matching by ID first, then fuzzy normalized key
            img_file = scans.get(eid) or scans.get(norm(f"{year}{name}"))

            if img_file:
                matched.append({
                    "id": eid or norm(f"{year}{name}"),
                    "label": f"{year} - {name}",
                    "text": r.get("results_raw", "No results_raw content found."),
                    "jpg": img_file
                })

    print(f"Matched {len(matched)} events with FBW scans.")
    return sorted(matched, key=lambda x: x['label'], reverse=True)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>FBW Archive Reviewer</title>
    <style>
        body { margin:0; display:grid; grid-template-columns: 320px 1fr 1fr; height:100vh; font-family:sans-serif; background:#111; color:#eee; }
        #sidebar { background:#1e1e1e; border-right:1px solid #333; display:flex; flex-direction:column; overflow:hidden; }
        #search { padding:15px; border-bottom:1px solid #333; }
        #search input { width:100%; padding:8px; background:#333; color:white; border:1px solid #444; border-radius:4px; box-sizing:border-box; }
        #list { flex:1; overflow-y:auto; }
        .item { padding:12px; border-bottom:1px solid #222; cursor:pointer; font-size:12px; transition:0.1s; }
        .item:hover { background:#2a2d2e; }
        .item.active { background:#094771; font-weight:bold; }
        
        #text-pane { background:white; color:#222; overflow-y:auto; padding:20px; border-right:1px solid #ccc; }
        pre { white-space:pre-wrap; font-family:monospace; font-size:11px; line-height:1.4; background:#f8f8f8; padding:15px; border:1px solid #ddd; }
        
        #scan-pane { background:#2d2d2d; display:flex; flex-direction:column; overflow:hidden; }
        #toolbar { padding:10px; background:#1a1a1a; display:flex; gap:10px; align-items:center; border-bottom:1px solid #000; }
        #viewport { flex:1; overflow:auto; display:flex; justify-content:center; align-items:flex-start; padding:40px; }
        img { transition: transform 0.2s; box-shadow: 0 0 40px black; transform-origin: center center; }
        button { cursor:pointer; padding:6px 12px; background:#444; color:white; border:1px solid #555; }
    </style>
</head>
<body>
    <div id="sidebar">
        <div id="search"><input type="text" id="q" placeholder="Search events..." onkeyup="search()"></div>
        <div id="list"></div>
    </div>
    <div id="text-pane">
        <h3 id="title">Select an Entry</h3>
        <pre id="mirror"></pre>
    </div>
    <div id="scan-pane">
        <div id="toolbar">
            <button onclick="rotate(-90)">↺</button>
            <button onclick="rotate(90)">Rotate ↻</button>
            <span id="fname" style="font-size:10px; opacity:0.5; margin-left:auto;"></span>
        </div>
        <div id="viewport"><img id="pic" style="display:none;"></div>
    </div>
    <script>
        const DATA = %JSON_DATA%;
        let rotation = 0;

        function render(items) {
            const list = document.getElementById('list');
            list.innerHTML = items.map(ev => `<div class="item" id="item-${ev.id}" onclick="view('${ev.id}')">${ev.label}</div>`).join('');
        }

        function view(id) {
            const ev = DATA.find(e => e.id === id);
            document.querySelectorAll('.item').forEach(i => i.classList.remove('active'));
            document.getElementById('item-'+id)?.classList.add('active');
            document.getElementById('title').innerText = ev.label;
            document.getElementById('mirror').innerText = ev.text;
            document.getElementById('fname').innerText = ev.jpg;
            
            rotation = 0;
            const img = document.getElementById('pic');
            img.style.transform = 'rotate(0deg)';
            img.style.margin = '0';
            img.src = "scans/" + ev.jpg;
            img.style.display = "block";
        }

        function rotate(d) {
            rotation += d;
            const img = document.getElementById('pic');
            img.style.transform = `rotate(${rotation}deg)`;
            img.style.margin = (Math.abs(rotation)/90)%2 === 1 ? "150px 0" : "0";
        }

        function search() {
            const query = document.getElementById('q').value.toLowerCase();
            render(DATA.filter(d => d.label.toLowerCase().includes(query)));
        }

        render(DATA);
    </script>
</body>
</html>
"""

def main():
    events = load_data()
    if not events:
        print("!! Failed to match any events. Check column names and folder paths.")
        return

    html = HTML_TEMPLATE.replace("%JSON_DATA%", json.dumps(events))
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"--- SUCCESS ---")
    print(f"Generated: {OUT_HTML}")

if __name__ == "__main__":
    main()
