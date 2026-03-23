import csv, json, sys, os
from pathlib import Path

# Increase field limit for large mirror text blobs
csv.field_size_limit(sys.maxsize)

# SET PATHS - Robust enough to run from / or /tools
ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
PF_CSV = OUT / "Placements_Flat.csv"
STAGE2_CSV = OUT / "stage2_canonical_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML = OUT / "event_comparison_viewer.html"

def load_scan_index():
    """Maps event_id -> filename with a Year_Name fallback for Vols 2-14."""
    index = {}
    if not SCAN_INDEX_CSV.exists():
        print(f"Warning: {SCAN_INDEX_CSV} not found.")
        return index
    
    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("event_id")
            fname = r.get("source_file")
            if eid and fname:
                index[eid] = fname
                # Fallback fuzzy key: "1986_sunshinestateopen"
                fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
                index[fuzzy] = fname
    return index

def load_events(scan_index):
    events = []
    if not STAGE2_CSV.exists():
        print(f"Error: {STAGE2_CSV} missing. Run stage 2 script first.")
        return []

    with open(STAGE2_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r["event_id"]
            # Try direct ID match, then fuzzy fallback
            fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
            fname = scan_index.get(eid) or scan_index.get(fuzzy, "")
            
            events.append({
                "id": eid,
                "name": f"{r['year']} {r['event_name']}",
                "scan_jpg": fname,
                "rotation": 0,
                "mirror": r.get("mirror_text", "")
            })
    return events

# --- HTML TEMPLATE WITH ROTATION TOOL ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Footbag Archive Viewer (Vols 2-14)</title>
    <style>
        #app { display: grid; grid-template-columns: 300px 1fr 1fr; height: 100vh; overflow: hidden; font-family: sans-serif; }
        #list { overflow-y: auto; background: #f0f0f0; border-right: 1px solid #ccc; }
        #data { overflow-y: auto; padding: 20px; border-right: 1px solid #ccc; }
        #scan-pane { background: #333; position: relative; display: flex; flex-direction: column; }
        #toolbar { background: #222; padding: 10px; display: flex; gap: 10px; color: white; }
        #viewport { flex: 1; overflow: auto; display: flex; justify-content: center; align-items: flex-start; padding: 50px; }
        img { transition: transform 0.2s; box-shadow: 0 0 20px black; transform-origin: center center; }
        .event-item { padding: 10px; cursor: pointer; border-bottom: 1px solid #ddd; font-size: 13px; }
        .event-item:hover { background: #e0e0e0; }
    </style>
</head>
<body>
    <div id="app">
        <div id="list">
            <div style="padding:10px; font-weight:bold; background:#1F3864; color:white;">Events (Vols 2-14)</div>
            <div id="event-list"></div>
        </div>
        <div id="data">
            <h2 id="title">Select an Event</h2>
            <pre id="mirror" style="white-space: pre-wrap; font-size: 11px; color: #444;"></pre>
        </div>
        <div id="scan-pane">
            <div id="toolbar">
                <button onclick="rotate(-90)">↺ Rotate</button>
                <button onclick="rotate(90)">Rotate ↻</button>
                <span id="filename" style="font-size:11px; margin-left:auto;"></span>
            </div>
            <div id="viewport">
                <img id="scan-img" src="" style="display:none;">
            </div>
        </div>
    </div>

    <script>
        const EVENTS = %EVENTS_JSON%;
        let currentRot = 0;

        const listEl = document.getElementById('event-list');
        EVENTS.forEach(ev => {
            const div = document.createElement('div');
            div.className = 'event-item';
            div.innerText = ev.name;
            div.onclick = () => selectEvent(ev);
            listEl.appendChild(div);
        });

        function selectEvent(ev) {
            currentRot = 0;
            document.getElementById('title').innerText = ev.name;
            document.getElementById('mirror').innerText = ev.mirror;
            document.getElementById('filename').innerText = ev.scan_jpg || "No Scan Mapped";
            
            const img = document.getElementById('scan-img');
            img.style.transform = `rotate(0deg)`;
            img.style.margin = "0";
            if (ev.scan_jpg) {
                // IMPORTANT: This assumes JPEGs are in an 'out/scans/' folder
                img.src = "scans/" + ev.scan_jpg;
                img.style.display = "block";
            } else {
                img.style.display = "none";
            }
        }

        function rotate(deg) {
            currentRot += deg;
            const img = document.getElementById('scan-img');
            img.style.transform = `rotate(${currentRot}deg)`;
            img.style.margin = (Math.abs(currentRot) / 90) % 2 === 1 ? "150px 0" : "0";
        }
    </script>
</body>
</html>
"""

def main():
    idx = load_scan_index()
    evs = load_events(idx)
    
    if not evs:
        print("No events loaded. Check your CSV paths.")
        return

    json_data = json.dumps(evs)
    html = HTML_TEMPLATE.replace("%EVENTS_JSON%", json_data)
    
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Success! Created {OUT_HTML}")

if __name__ == "__main__":
    main()
