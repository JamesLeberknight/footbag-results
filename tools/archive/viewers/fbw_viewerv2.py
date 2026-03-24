#!/usr/bin/env python3
import csv, json, sys, os
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "out"
STAGE2_CSV = OUT / "stage2_canonical_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML = OUT / "fbw_archive_viewer.html"

def normalize(text):
    """Removes all non-alphanumeric chars for bulletproof matching."""
    return "".join(filter(str.isalnum, str(text))).lower()

def load_data():
    # 1. Index the Scans
    scans = {}
    if not SCAN_INDEX_CSV.exists():
        print(f"Error: Missing {SCAN_INDEX_CSV}")
        return []

    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = r.get("source_file", "")
            eid = r.get("event_id", "")
            year = r.get("year", "")
            name = r.get("event_name", "")
            
            # Store multiple keys to ensure a match
            if eid: scans[eid] = fname
            scans[normalize(f"{year}{name}")] = fname
            scans[normalize(name)] = fname # Last resort

    # 2. Match with Events
    matched_events = []
    if not STAGE2_CSV.exists():
        print(f"Error: Missing {STAGE2_CSV}")
        return []

    with open(STAGE2_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("event_id", "")
            year = r.get("year", "")
            name = r.get("event_name", "")
            
            # Try 1: Exact ID
            # Try 2: Year + Name normalize
            # Try 3: Name only normalize
            norm_key = normalize(f"{year}{name}")
            img_file = scans.get(eid) or scans.get(norm_key) or scans.get(normalize(name))

            if img_file:
                matched_events.append({
                    "label": f"{year} - {name}",
                    "mirror": r.get("mirror_text", "No text found"),
                    "jpg": img_file
                })

    print(f"Matched {len(matched_events)} events with scans.")
    return sorted(matched_events, key=lambda x: x['label'], reverse=True)

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>FBW Archive Viewer</title>
    <style>
        body { margin:0; display:grid; grid-template-columns: 320px 1fr 1fr; height:100vh; font-family:sans-serif; background:#1e1e1e; color:#eee; }
        #list { overflow-y:auto; border-right:1px solid #333; background:#252526; }
        #text { overflow-y:auto; padding:20px; background:#fff; color:#222; border-right:1px solid #ccc; }
        #scan { overflow:hidden; display:flex; flex-direction:column; background:#2d2d2d; position:relative; }
        .item { padding:10px; border-bottom:1px solid #333; cursor:pointer; font-size:12px; }
        .item:hover { background:#37373d; }
        .item.active { background:#094771; }
        pre { white-space:pre-wrap; font-family:monospace; font-size:11px; background:#f4f4f4; padding:15px; border:1px solid #ddd; }
        #viewport { flex:1; overflow:auto; display:flex; justify-content:center; padding:40px; }
        img { transition: transform 0.2s; box-shadow: 0 0 20px black; }
        #tools { padding:10px; background:#111; display:flex; gap:10px; }
        button { cursor:pointer; background:#444; color:white; border:1px solid #666; padding:5px 10px; }
    </style>
</head>
<body>
    <div id="list"></div>
    <div id="text"><h3 id="title">Select Event</h3><pre id="mirror"></pre></div>
    <div id="scan">
        <div id="tools">
            <button onclick="rot(-90)">↺</button>
            <button onclick="rot(90)">Rotate ↻</button>
            <span id="fn" style="font-size:10px; margin-left:auto; opacity:0.5;"></span>
        </div>
        <div id="viewport"><img id="pic" style="display:none;"></div>
    </div>
    <script>
        const DATA = %JSON%;
        let rotation = 0;
        const listDiv = document.getElementById('list');

        DATA.forEach((ev, i) => {
            const div = document.createElement('div');
            div.className = 'item';
            div.innerText = ev.label;
            div.onclick = () => {
                rotation = 0;
                document.querySelectorAll('.item').forEach(el => el.classList.remove('active'));
                div.classList.add('active');
                document.getElementById('title').innerText = ev.label;
                document.getElementById('mirror').innerText = ev.mirror;
                document.getElementById('fn').innerText = ev.jpg;
                const img = document.getElementById('pic');
                img.src = "scans/" + ev.jpg;
                img.style.display = "block";
                img.style.transform = "rotate(0deg)";
            };
            listDiv.appendChild(div);
        });

        function rot(d) {
            rotation += d;
            const img = document.getElementById('pic');
            img.style.transform = `rotate(${rotation}deg)`;
            img.style.margin = (Math.abs(rotation)/90)%2 === 1 ? "150px 0" : "0";
        }
    </script>
</body>
</html>
"""

events = load_data()
if events:
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(HTML.replace("%JSON%", json.dumps(events)))
    print(f"Success: Open {OUT_HTML}")
else:
    print("Zero matches. Please check that 'event_id' exists in both CSVs.")
