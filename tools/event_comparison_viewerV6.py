#!/usr/bin/env python3
import csv, json, sys, os
from pathlib import Path

# Increase limit for large text blobs
csv.field_size_limit(sys.maxsize)

# Path Setup
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "out"
# Change this to whichever CSV has your main event list + mirror_text
DATA_CSV = OUT / "stage2_canonical_events.csv" 
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML = OUT / "simple_viewer.html"

def load_data():
    # 1. Load Scan Index (Fuzzy)
    scans = {}
    if SCAN_INDEX_CSV.exists():
        with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                fname = r.get("source_file", "")
                if r.get("event_id"): scans[r["event_id"]] = fname
                # Create fuzzy key (e.g. "1984_worldfootbagchampionships")
                fuzzy = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
                scans[fuzzy] = fname

    # 2. Load Events
    events = []
    if not DATA_CSV.exists():
        print(f"Error: {DATA_CSV} not found.")
        return []

    with open(DATA_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("event_id", "")
            year = r.get("year", "")
            name = r.get("event_name", "")
            fuzzy = f"{year}_{name}".lower().replace(" ","")
            
            events.append({
                "label": f"{year} {name}",
                "mirror": r.get("mirror_text", "No text found"),
                "jpg": scans.get(eid) or scans.get(fuzzy, "")
            })
    return sorted(events, key=lambda x: x['label'], reverse=True)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Simple Footbag Archive</title>
    <style>
        body { margin:0; display:grid; grid-template-columns: 300px 1fr 1fr; height:100vh; font-family:sans-serif; background:#1e1e1e; color:#ccc; }
        #list { overflow-y:auto; border-right:1px solid #444; background:#252526; }
        #text-pane { overflow-y:auto; padding:20px; background:#fff; color:#333; border-right:1px solid #ccc; }
        #img-pane { overflow:auto; display:flex; flex-direction:column; background:#2d2d2d; }
        .item { padding:10px; border-bottom:1px solid #333; cursor:pointer; font-size:12px; }
        .item:hover { background:#37373d; }
        pre { white-space:pre-wrap; font-family:monospace; font-size:11px; }
        #toolbar { padding:10px; background:#111; display:flex; gap:10px; }
        img { transition: transform 0.2s; max-width:100%; margin-top:20px; }
    </style>
</head>
<body>
    <div id="list"></div>
    <div id="text-pane"><h3 id="title">Select Event</h3><pre id="mirror"></pre></div>
    <div id="img-pane">
        <div id="toolbar"><button onclick="rotate(-90)">↺</button><button onclick="rotate(90)">Rotate ↻</button></div>
        <div style="padding:20px; text-align:center;"><img id="pic"></div>
    </div>
    <script>
        const DATA = %JSON%;
        let rot = 0;
        const list = document.getElementById('list');
        DATA.forEach((ev, i) => {
            const d = document.createElement('div');
            d.className = 'item';
            d.innerText = ev.label;
            d.onclick = () => {
                rot = 0;
                document.getElementById('title').innerText = ev.label;
                document.getElementById('mirror').innerText = ev.mirror;
                const img = document.getElementById('pic');
                img.style.transform = 'rotate(0deg)';
                img.src = ev.jpg ? "scans/" + ev.jpg : "";
                img.style.display = ev.jpg ? "inline-block" : "none";
            };
            list.appendChild(d);
        });
        function rotate(d) { rot += d; document.getElementById('pic').style.transform = `rotate(${rot}deg)`; }
    </script>
</body>
</html>
"""

events = load_data()
with open(OUT_HTML, "w") as f:
    f.write(HTML.replace("%JSON%", json.dumps(events)))
print(f"Done! Created {OUT_HTML}")
