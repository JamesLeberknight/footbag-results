#!/usr/bin/env python3
import csv, json, sys, os
from pathlib import Path

# Increase field limit for large mirror text blobs
csv.field_size_limit(sys.maxsize)

# --- PATH SETTINGS ---
# Resolves the project root regardless of whether you run from / or /tools
ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
# Inputs
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PF_CSV         = OUT / "Placements_Flat.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
# Output
OUT_HTML       = OUT / "event_comparison_viewer.html"

def load_scan_index():
    """Maps event_id -> filename for Vols 2-14."""
    index = {}
    if not SCAN_INDEX_CSV.exists():
        print(f"!! Warning: {SCAN_INDEX_CSV} not found. Images will not load.")
        return index
    
    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("event_id")
            fname = r.get("source_file")
            if eid and fname:
                index[eid] = fname
                # Fallback fuzzy key for inconsistent naming
                fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
                index[fuzzy] = fname
    return index

def load_events(scan_index):
    events = []
    if not STAGE2_CSV.exists():
        print(f"!! Error: {STAGE2_CSV} missing. Run stage 2 script first.")
        return []

    with open(STAGE2_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            eid = r.get("event_id", "")
            year = r.get("year", "0000")
            name = r.get("event_name", "Unknown Event")
            
            # Fuzzy matching for images
            fuzzy = f"{year}_{name}".lower().replace(" ","")
            fname = scan_index.get(eid) or scan_index.get(fuzzy, "")
            
            events.append({
                "id": eid,
                "label": f"{year} - {name}",
                "scan_jpg": fname,
                "mirror": r.get("mirror_text", "No mirror text available.")
            })
    return sorted(events, key=lambda x: x['label'], reverse=True)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Footbag Archive: Vols 2-14</title>
    <style>
        body { margin: 0; font-family: 'Segoe UI', Tahoma, sans-serif; background: #1a1a1a; color: #eee; height: 100vh; overflow: hidden; }
        #app { display: grid; grid-template-columns: 320px 1fr 1fr; height: 100vh; }
        
        /* Sidebar & Search */
        #sidebar { background: #252526; border-right: 1px solid #333; display: flex; flex-direction: column; }
        #search-box { padding: 15px; background: #1e1e1e; border-bottom: 1px solid #333; }
        #search-input { w-id: 100%; width: 100%; padding: 8px; background: #333; border: 1px solid #444; color: white; border-radius: 4px; box-sizing: border-box; }
        #event-list { flex: 1; overflow-y: auto; }
        .event-item { padding: 10px 15px; cursor: pointer; border-bottom: 1px solid #333; font-size: 12px; transition: 0.2s; }
        .event-item:hover { background: #37373d; }
        .event-item.active { background: #094771; color: white; }

        /* Data Pane */
        #data-pane { background: #fff; color: #333; padding: 25px; overflow-y: auto; border-right: 1px solid #ccc; }
        #mirror-text { white-space: pre-wrap; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; background: #f9f9f9; padding: 15px; border: 1px solid #ddd; }

        /* Scan Pane */
        #scan-pane { display: flex; flex-direction: column; background: #2d2d2d; position: relative; }
        #toolbar { padding: 10px; background: #1e1e1e; display: flex; gap: 10px; border-bottom: 1px solid #000; align-items: center; }
        #viewport { flex: 1; overflow: auto; display: flex; justify-content: center; align-items: flex-start; padding: 60px; }
        #scan-img { transition: transform 0.2s; box-shadow: 0 0 40px rgba(0,0,0,0.8); transform-origin: center center; }
        button { cursor: pointer; padding: 5px 12px; background: #444; color: white; border: 1px solid #666; border-radius: 3px; }
        button:hover { background: #555; }
    </style>
</head>
<body>
    <div id="app">
        <div id="sidebar">
            <div id="search-box">
                <input type="text" id="search-input" placeholder="Search events or years..." onkeyup="filterEvents()">
            </div>
            <div id="event-list"></div>
        </div>
        <div id="data-pane">
            <h2 id="view-title">Select an event</h2>
            <div id="mirror-text"></div>
        </div>
        <div id="scan-pane">
            <div id="toolbar">
                <button onclick="rotate(-90)">↺ Rotate</button>
                <button onclick="rotate(90)">Rotate ↻</button>
                <span id="file-info" style="font-size: 11px; color: #888; margin-left: auto;"></span>
            </div>
            <div id="viewport">
                <img id="scan-img" style="display:none;">
            </div>
        </div>
    </div>

    <script>
        const EVENTS = %EVENTS_JSON%;
        let currentRotation = 0;

        function renderList(list) {
            const container = document.getElementById('event-list');
            container.innerHTML = '';
            list.forEach(ev => {
                const div = document.createElement('div');
                div.className = 'event-item';
                div.id = 'item-' + ev.id;
                div.innerText = ev.label;
                div.onclick = () => selectEvent(ev);
                container.appendChild(div);
            });
        }

        function filterEvents() {
            const q = document.getElementById('search-input').value.toLowerCase();
            const filtered = EVENTS.filter(e => e.label.toLowerCase().includes(q));
            renderList(filtered);
        }

        function selectEvent(ev) {
            // UI Updates
            document.querySelectorAll('.event-item').forEach(el => el.classList.remove('active'));
            document.getElementById('item-' + ev.id)?.classList.add('active');
            document.getElementById('view-title').innerText = ev.label;
            document.getElementById('mirror-text').innerText = ev.mirror;
            document.getElementById('file-info').innerText = ev.scan_jpg || "No scan mapped";

            // Image Logic
            currentRotation = 0;
            const img = document.getElementById('scan-img');
            img.style.transform = `rotate(0deg)`;
            img.style.margin = "0";
            
            if (ev.scan_jpg) {
                img.src = "scans/" + ev.scan_jpg;
                img.style.display = "block";
            } else {
                img.style.display = "none";
            }
        }

        function rotate(deg) {
            currentRotation += deg;
            const img = document.getElementById('scan-img');
            img.style.transform = `rotate(${currentRotation}deg)`;
            // Fix layout clipping when landscape is turned vertical
            const isVertical = (Math.abs(currentRotation) / 90) % 2 === 1;
            img.style.margin = isVertical ? "150px 0" : "0";
        }

        renderList(EVENTS);
    </script>
</body>
</html>
"""

def main():
    print("Indexing scans...")
    idx = load_scan_index()
    print("Loading event data...")
    evs = load_events(idx)
    
    if not evs:
        print("!! Failed to load events. Check if stage2_canonical_events.csv exists in /out.")
        return

    print(f"Building HTML for {len(evs)} events...")
    payload = json.dumps(evs)
    html_content = HTML_TEMPLATE.replace("%EVENTS_JSON%", payload)
    
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"--- SUCCESS ---")
    print(f"Generated: {OUT_HTML}")
    print(f"Make sure your JPEGs are in: {OUT}/scans/")

if __name__ == "__main__":
    main()
