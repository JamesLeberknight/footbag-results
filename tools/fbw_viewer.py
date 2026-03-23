import csv, json, sys, os
from pathlib import Path

# Increase field limit for large text blobs
csv.field_size_limit(sys.maxsize)

# --- DIRECTORY SETUP ---
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
OUT = ROOT / "out"
INPUTS = ROOT / "inputs"

# Files
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
SCAN_INDEX_CSV = INPUTS / "magazine_scan_index.csv"
OUT_HTML       = OUT / "fbw_only_viewer.html"

def load_data():
    if not SCAN_INDEX_CSV.exists():
        print(f"ERROR: Could not find {SCAN_INDEX_CSV}")
        return []
    
    # 1. Load all scans from the index
    scan_data = {}
    with open(SCAN_INDEX_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            eid = r.get('event_id')
            fname = r.get('source_file')
            if eid and fname:
                # Store by ID and a "Fuzzy" key (year_name)
                fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
                info = {'jpg': fname, 'year': r.get('year',''), 'name': r.get('event_name','')}
                scan_data[eid] = info
                scan_data[fuzzy] = info

    print(f"Indexed {len(scan_data)} scan entries (including fuzzy keys).")

    # 2. Match with mirror text from stage2
    final_events = []
    if not STAGE2_CSV.exists():
        print(f"ERROR: Could not find {STAGE2_CSV}")
        return []

    with open(STAGE2_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            eid = r.get('event_id')
            year = r.get('year', '')
            name = r.get('event_name', '')
            fuzzy = f"{year}_{name}".lower().replace(" ","")
            
            # Match strictly by ID or Fallback to Fuzzy
            match = scan_data.get(eid) or scan_data.get(fuzzy)
            
            if match:
                final_events.append({
                    'id': eid,
                    'label': f"{year} - {name}",
                    'mirror': r.get('mirror_text', 'No text found in Stage 2 CSV'),
                    'jpg': match['jpg']
                })

    # Sort newest to oldest
    final_events.sort(key=lambda x: x['label'], reverse=True)
    print(f"Successfully matched {len(final_events)} events with scans.")
    return final_events

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>FBW Archive (Scans Only)</title>
    <style>
        body { margin:0; display:grid; grid-template-columns: 350px 1fr 1fr; height:100vh; font-family:sans-serif; background:#111; color:#eee; }
        #sidebar { background:#1e1e1e; border-right:1px solid #333; display:flex; flex-direction:column; }
        #search-box { padding:15px; border-bottom:1px solid #333; }
        #search-box input { width:100%; padding:8px; background:#333; color:white; border:1px solid #444; border-radius:4px; box-sizing:border-box; }
        #list { flex:1; overflow-y:auto; }
        .item { padding:12px; border-bottom:1px solid #222; cursor:pointer; font-size:12px; }
        .item:hover { background:#2a2d2e; }
        .item.active { background:#094771; color:white; }
        
        #text-pane { background:white; color:#222; overflow-y:auto; padding:20px; border-right:1px solid #ccc; }
        pre { white-space:pre-wrap; font-family:monospace; font-size:11px; line-height:1.4; background:#f4f4f4; padding:15px; border:1px solid #ddd; }
        
        #scan-pane { background:#2d2d2d; display:flex; flex-direction:column; overflow:hidden; }
        #toolbar { padding:10px; background:#1a1a1a; display:flex; gap:10px; align-items:center; }
        #viewport { flex:1; overflow:auto; display:flex; justify-content:center; align-items:flex-start; padding:40px; }
        img { transition: transform 0.2s; box-shadow: 0 0 30px black; transform-origin: center center; }
        button { cursor:pointer; padding:6px 12px; background:#444; color:white; border:1px solid #555; }
    </style>
</head>
<body>
    <div id="sidebar">
        <div id="search-box"><input type="text" id="q" placeholder="Search event names..." onkeyup="doSearch()"></div>
        <div id="list"></div>
    </div>
    <div id="text-pane">
        <h2 id="title">Select an Archive Entry</h2>
        <pre id="mirror"></pre>
    </div>
    <div id="scan-pane">
        <div id="toolbar">
            <button onclick="rot(-90)">↺</button>
            <button onclick="rot(90)">Rotate ↻</button>
            <span id="finfo" style="font-size:11px; opacity:0.5; margin-left:auto;"></span>
        </div>
        <div id="viewport"><img id="pic" style="display:none;"></div>
    </div>

    <script>
        const DATA = %JSON_DATA%;
        let rotation = 0;

        function render(items) {
            const list = document.getElementById('list');
            list.innerHTML = items.map(ev => `
                <div class="item" id="item-${ev.id}" onclick="view('${ev.id}')">${ev.label}</div>
            `).join('');
        }

        function view(id) {
            const ev = DATA.find(e => e.id === id);
            document.querySelectorAll('.item').forEach(i => i.classList.remove('active'));
            document.getElementById('item-'+id).classList.add('active');
            
            document.getElementById('title').innerText = ev.label;
            document.getElementById('mirror').innerText = ev.mirror;
            document.getElementById('finfo').innerText = ev.jpg;

            rotation = 0;
            const img = document.getElementById('pic');
            img.style.transform = 'rotate(0deg)';
            img.style.margin = '0';
            img.src = "scans/" + ev.jpg;
            img.style.display = "block";
        }

        function rot(d) {
            rotation += d;
            const img = document.getElementById('pic');
            img.style.transform = `rotate(${rotation}deg)`;
            img.style.margin = (Math.abs(rotation)/90)%2 === 1 ? "150px 0" : "0";
        }

        function doSearch() {
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
        print("CRITICAL: No events found to display.")
        return
        
    html = HTML_TEMPLATE.replace("%JSON_DATA%", json.dumps(events))
    with open(OUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"--- DONE ---")
    print(f"Viewer created: {OUT_HTML}")

if __name__ == "__main__":
    main()
