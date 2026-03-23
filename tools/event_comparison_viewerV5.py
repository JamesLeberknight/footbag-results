#!/usr/bin/env python3
import csv, json, sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT           = Path(__file__).resolve().parent.parent
OUT            = ROOT / "out"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
PF_CSV         = OUT / "Placements_Flat.csv"
QUARANTINE_CSV = ROOT / "inputs" / "review_quarantine_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML       = OUT / "event_comparison_viewer.html"

def load_quarantine():
    q = {}
    if QUARANTINE_CSV.exists():
        with open(QUARANTINE_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f): q[r["event_id"]] = r["reason"]
    return q

def load_scan_index():
    """Returns event_id -> jpg AND a fuzzy name -> jpg fallback."""
    index = {}
    if not SCAN_INDEX_CSV.exists(): return index
    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = r.get("source_file", "")
            eid = r.get("event_id", "")
            if eid: index[eid] = fname
            # Fuzzy fallback for Vols 2-7
            fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
            index[fuzzy] = fname
    return index

def load_events(quarantine, scan_index):
    events = []
    if not STAGE2_CSV.exists(): return []
    with open(STAGE2_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r["event_id"]
            fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
            
            # Use Direct ID first, then Fuzzy Fallback
            fname = scan_index.get(eid) or scan_index.get(fuzzy, "")
            
            events.append({
                "id": eid,
                "year": r["year"],
                "name": r["event_name"],
                "mirror": r.get("mirror_text", ""),
                "scan_jpg": fname,
                "q": quarantine.get(eid, "")
            })
    return sorted(events, key=lambda x: (x['year'], x['name']), reverse=True)

def load_pf_index():
    idx = {}
    if not PF_CSV.exists(): return idx
    with open(PF_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            idx.setdefault(r["event_id"], []).append(r)
    return idx

# --- Updated HTML Template with Rotation Logic ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Footbag Archive Viewer</title>
    <style>
        body { margin:0; font-family: sans-serif; height:100vh; display:grid; grid-template-rows: auto 1fr; overflow:hidden; }
        #hdr { background:#1F3864; color:white; padding:10px; display:flex; gap:20px; align-items:center; }
        #body { display:grid; grid-template-columns: 300px 1fr 1fr; overflow:hidden; background:#eee; }
        #list { overflow-y:scroll; background:white; border-right:1px solid #ccc; }
        #data { overflow-y:scroll; padding:20px; background:white; border-right:1px solid #ccc; }
        #scan-pane { background:#333; display:flex; flex-direction:column; position:relative; }
        #toolbar { background:#222; padding:8px; display:flex; gap:10px; color:white; border-bottom:1px solid #000; }
        #viewport { flex:1; overflow:auto; display:flex; justify-content:center; padding:40px; }
        img { transition: transform 0.2s; box-shadow: 0 0 20px black; transform-origin: center center; }
        .ev-item { padding:8px; cursor:pointer; border-bottom:1px solid #eee; font-size:12px; }
        .ev-item:hover { background:#f0f7ff; }
        .ev-item.active { background:#d1e8ff; font-weight:bold; }
        pre { white-space: pre-wrap; font-size:11px; font-family:monospace; background:#f9f9f9; padding:10px; border:1px solid #ddd; }
        table { width:100%; border-collapse:collapse; font-size:12px; margin-top:10px;}
        td, th { border:1px solid #ddd; padding:4px; }
    </style>
</head>
<body>
    <div id="hdr">
        <strong>Footbag Event Review</strong>
        <input type="text" id="search" placeholder="Search..." onkeyup="search()">
    </div>
    <div id="body">
        <div id="list" id="ev-list"></div>
        <div id="data">
            <h3 id="title">Select Event</h3>
            <div id="placements"></div>
            <hr>
            <h4>Mirror Text</h4>
            <pre id="mirror"></pre>
        </div>
        <div id="scan-pane">
            <div id="toolbar">
                <button onclick="rotate(-90)">↺</button>
                <button onclick="rotate(90)">↻ Rotate</button>
                <span id="fname" style="font-size:10px; margin-left:auto; opacity:0.6;"></span>
            </div>
            <div id="viewport">
                <img id="scan-img" style="display:none;">
            </div>
        </div>
    </div>
    <script>
        const EVENTS = %EVENTS_JSON%;
        const PF = %PF_JSON%;
        let rotation = 0;

        function renderList(items) {
            const container = document.getElementById('list');
            container.innerHTML = items.map(ev => `
                <div class="ev-item" id="ev-${ev.id}" onclick="selectEvent('${ev.id}')">
                    ${ev.year} ${ev.name}
                </div>
            `).join('');
        }

        function selectEvent(id) {
            const ev = EVENTS.find(e => e.id === id);
            document.querySelectorAll('.ev-item').forEach(el => el.classList.remove('active'));
            document.getElementById('ev-'+id)?.classList.add('active');
            
            document.getElementById('title').innerText = ev.year + " " + ev.name;
            document.getElementById('mirror').innerText = ev.mirror;
            document.getElementById('fname').innerText = ev.scan_jpg || "No Scan";

            // Render Placements
            const rows = PF[id] || [];
            let html = '<table><tr><th>Pl</th><th>Name</th><th>Discipline</th></tr>';
            rows.forEach(r => {
                html += `<tr><td>${r.pl}</td><td>${r.pc}</td><td>${r.dn}</td></tr>`;
            });
            document.getElementById('placements').innerHTML = html + '</table>';

            // Image Handling
            rotation = 0;
            const img = document.getElementById('scan-img');
            img.style.transform = 'rotate(0deg)';
            img.style.margin = '0';
            if (ev.scan_jpg) {
                img.src = "scans/" + ev.scan_jpg;
                img.style.display = "block";
            } else {
                img.style.display = "none";
            }
        }

        function rotate(deg) {
            rotation += deg;
            const img = document.getElementById('scan-img');
            img.style.transform = `rotate(${rotation}deg)`;
            img.style.margin = (Math.abs(rotation) / 90) % 2 === 1 ? "150px 0" : "0";
        }

        function search() {
            const q = document.getElementById('search').value.toLowerCase();
            renderList(EVENTS.filter(e => (e.year + e.name).toLowerCase().includes(q)));
        }

        renderList(EVENTS);
        if(EVENTS.length) selectEvent(EVENTS[0].id);
    </script>
</body>
</html>
"""

def main():
    q = load_quarantine()
    s_idx = load_scan_index()
    evs = load_events(q, s_idx)
    pf = load_pf_index()
    
    html = HTML_TEMPLATE.replace("%EVENTS_JSON%", json.dumps(evs))
    html = html.replace("%PF_JSON%", json.dumps(pf))
    
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Viewer created at {OUT_HTML}")

if __name__ == "__main__":
    main()
